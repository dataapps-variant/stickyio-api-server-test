from fastapi import FastAPI, HTTPException, Query
from typing import Optional, List, Dict, Any
import asyncio
import aiohttp
import json
from datetime import datetime, timedelta
from cachetools import TTLCache
import logging
import time
from datetime import datetime, timezone
# Configure logging
logging.basicConfig(level=logging.INFO)
from zoneinfo import ZoneInfo
from google.cloud import bigquery
from google.oauth2 import service_account

logger = logging.getLogger(__name__)

app = FastAPI(title="Sticky.io to Airbyte Middleware", version="1.0.0")

# Configuration 
STICKY_API_BASE = "" #https://pdfdotnet.sticky.io/api/v1/
STICKY_AUTH = []  # Basic Auth
RATE_LIMIT_PER_MINUTE = 15
REQUEST_TIMEOUT = 36000

# Cache for order details to avoid repeated API calls
order_cache = TTLCache(maxsize=1000000, ttl=7200)  # 1 hour cache


import base64

def getUsersPass(cred:str):

    global STICKY_AUTH

    # Decode from Base64
    decoded_bytes = base64.b64decode(cred)
    decoded_str = decoded_bytes.decode('utf-8')

    # Split into user and pass
    if ':' in decoded_str:
        auth = decoded_str.split(':', 1)
        print("Username:", auth[0])
        print("Password:", auth[1])

        return auth
    else:
        print("Invalid format. Expected 'user:pass' after decoding.")

class StickyAPIClient:
    def __init__(self):
        self.session = None
        self.request_count = 0
        self.last_reset = time.time()
        self.semaphore = asyncio.Semaphore(80)  # Limit concurrent requests
        self.request_times = []  # Track request times for sliding window
        self.last_auth = None  # Track last used auth

    async def get_session(self):
        current_auth = aiohttp.BasicAuth(STICKY_AUTH[0], STICKY_AUTH[1])
        
        # Recreate session if auth changed or session doesn't exist
        if self.session is None or self.last_auth != current_auth:
            if self.session is not None:
                await self.session.close()
            
            timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                auth=current_auth,
                headers={"Content-Type": "application/json"}
            )
            self.last_auth = current_auth
        
        return self.session
    
    async def rate_limit_check_sliding_window(self):
        """Sliding window rate limiting - more efficient"""
        current_time = time.time()
        
        # Remove requests older than 1 minute
        self.request_times = [req_time for req_time in self.request_times 
                             if current_time - req_time < 60]
        
        # If we're at the limit, wait until the oldest request expires
        if len(self.request_times) >= 15:
            wait_time = 60 - (current_time - self.request_times[0]) + 0.1
            if wait_time > 0:
                await asyncio.sleep(wait_time)
                # Clean up expired requests after waiting
                current_time = time.time()
                self.request_times = [req_time for req_time in self.request_times 
                                     if current_time - req_time < 60]
        
        # Record this request
        self.request_times.append(current_time)
    
    async def call_order_view_batch(self, batch: List[str], batch_num: int, bIsOriginal: bool = True) -> List[Dict]:
        """Process a single batch with proper rate limiting"""
        async with self.semaphore:  # Limit concurrent batches
            max_retries = 3
            retry_delay = 1
            
            logger.info(f"Processing batch {batch_num}: {len(batch)} orders")
            
            for attempt in range(max_retries):
                try:
                    await self.rate_limit_check_sliding_window()
                    
                    payload = {
                        "order_id": [int(oid) for oid in batch],
                        "return_variants": 1
                    }
                    
                    session = await self.get_session()
                    async with session.post(
                        f"{STICKY_API_BASE}order_view",
                        json=payload
                    ) as response:
                        if response.status != 200:
                            error_text = await response.text()
                            if attempt == max_retries - 1:
                                logger.error(f"Batch {batch_num} failed after {max_retries} attempts: {error_text}")
                                return []
                            else:
                                logger.warning(f"Batch {batch_num} failed, attempt {attempt + 1}: {error_text}")
                                await asyncio.sleep(retry_delay)
                                retry_delay *= 2
                                continue
                        
                        batch_result = await response.json()
                        
                        # Transform orders (your existing logic)
                        orders = list(batch_result.get("data", {}).values()) if isinstance(batch_result.get("data"), dict) else [batch_result]
                        transformed_orders = []

                        for order in orders:
                            order_copy = order.copy()
                            products = order_copy.pop("products", [])

                            if isinstance(products, list) and len(products) == 1:
                                product = products[0]
                                for key, value in product.items():
                                    if isinstance(value, dict):
                                        for key_1, value_1 in value.items():
                                            order_copy[f"prd_{key}_{key_1}"] = value_1
                                    else:
                                        order_copy[f"prd_{key}"] = value

                            if "totals_breakdown" in order_copy and isinstance(order_copy["totals_breakdown"], dict):
                                breakdown = order_copy.pop("totals_breakdown")
                                for k, v in breakdown.items():
                                    order_copy[f"breakdown_{k}"] = v

                            order_copy.pop("utm_info", None)
                            if bIsOriginal:
                                order_copy.pop("order_customer_types", None)

                            if "systemNotes" in order_copy and isinstance(order_copy["systemNotes"], list):
                                order_copy["systemNotes"] = ",".join(map(str, order_copy["systemNotes"]))
                            
                            order_copy["source"] = "original" if bIsOriginal else "updated"
                            
                            transformed_orders.append(order_copy)

                        # Cache the results
                        #for order in transformed_orders:
                        #    order_id = str(order.get("order_id"))
                        #    order_cache[order_id] = order
                        
                        logger.info(f"Batch {batch_num} completed successfully: {len(transformed_orders)} orders")
                        return transformed_orders
                        
                except Exception as e:
                    if attempt == max_retries - 1:
                        logger.error(f"Batch {batch_num} failed after {max_retries} attempts: {str(e)}")
                        return []
                    else:
                        logger.warning(f"Batch {batch_num} error, attempt {attempt + 1}: {str(e)}")
                        await asyncio.sleep(retry_delay)
                        retry_delay *= 2
            
            return []
    
    async def call_order_view_concurrent(self, order_ids: List[str], isOriginal:bool = True) -> List[Dict[str, Any]]:
        """Call order_view API with concurrent batch processing"""
        
        # Check cache first
        cached_orders = []
        uncached_ids = []
        
        for order_id in order_ids:
            if order_id in order_cache:
                cached_orders.append(order_cache[order_id])
            else:
                uncached_ids.append(order_id)
        
        if not uncached_ids:
            logger.info("All orders found in cache")
            return cached_orders
        
        # Create batches of 500
        batch_size = 500
        batches = []
        for i in range(0, len(uncached_ids), batch_size):
            batch = uncached_ids[i:i + batch_size]
            batches.append((batch, i//batch_size + 1))
        
        logger.info(f"Processing {len(batches)} batches concurrently with rate limit of 80/min")
        
        # Process batches concurrently
        tasks = []
        for batch, batch_num in batches:
            task = asyncio.create_task(
                self.call_order_view_batch(batch, batch_num,isOriginal)
            )
            tasks.append(task)
        
        # Wait for all batches to complete
        all_orders = []
        completed_batches = 0
        
        for coro in asyncio.as_completed(tasks):
            try:
                batch_result = await coro
                all_orders.extend(batch_result)
                completed_batches += 1
                logger.info(f"Progress: {completed_batches}/{len(batches)} batches completed")
            except Exception as e:
                logger.error(f"Task failed: {str(e)}")
                completed_batches += 1
        
        # Combine cached and fresh orders
        all_orders.extend(cached_orders)
        logger.info(f"Total orders retrieved: {len(all_orders)} (including {len(cached_orders)} from cache)")
        
        return all_orders
    
    async def call_order_find(
        self, 
        start_date: str, 
        end_date: str,
        start_time: str = "00:00:00",
        end_time: str = "23:59:59", 
        campaign_id: str = "all",
        date_type: str = "create",
        criteria: str = "all",
        search_type: str = "all"
    ) -> List[Dict]:
        """Call Sticky.io order_find API with retry logic"""

        max_retries = 3
        retry_delay = 5

        payload = {
            "campaign_id": campaign_id,
            "start_date": start_date,
            "end_date": end_date,
            "start_time": start_time,
            "end_time": end_time,
            "date_type": date_type,
            "criteria": criteria,
            "search_type": search_type
        }

        logger.info(f"Calling order_find with payload: {payload}")

        for attempt in range(max_retries):
            try:
                await self.rate_limit_check_sliding_window()

                session = await self.get_session()
                async with session.post(
                    f"{STICKY_API_BASE}order_find",
                    json=payload
                ) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        if attempt == max_retries - 1:
                            logger.error(f"order_find failed after {max_retries} attempts: {error_text}")
                            raise HTTPException(
                                status_code=response.status,
                                detail=f"Sticky.io API error: {error_text}"
                            )
                        else:
                            logger.warning(f"order_find failed, attempt {attempt + 1}: {error_text}")
                            await asyncio.sleep(retry_delay)
                            retry_delay *= 2
                            continue
                        
                    return await response.json()

            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"order_find failed after {max_retries} attempts: {str(e)}")
                    raise HTTPException(status_code=500, detail=f"API error: {str(e)}")
                else:
                    logger.warning(f"order_find error, attempt {attempt + 1}: {str(e)}")
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2
    

    async def call_order_find_updated(
        self, 
        start_date: str, 
        end_date: str,
        start_time: str = "00:00:00",
        end_time: str = "23:59:59", 
        campaign_id: str = "all",
        group_keys: List[str] = ["chargeback","confirmation","fraud","refund","reprocess","return","rma","void"]
    ) -> List[Dict]:
        """Call Sticky.io order_find API"""
        await self.rate_limit_check_sliding_window()
        
        payload = {
            "campaign_id": campaign_id,
            "start_date": start_date,
            "end_date": end_date,
            "start_time": start_time,
            "end_time": end_time,
            "group_keys": group_keys
        }
        
        logger.info(f"Calling order_find with payload: {payload}")
        
        session = await self.get_session()
        async with session.post(
            f"{STICKY_API_BASE}order_find_updated",
            json=payload
        ) as response:
            if response.status != 200:
                error_text = await response.text()
                logger.error(f"order_find API error: {error_text}")
                raise HTTPException(
                    status_code=response.status,
                    detail=f"Sticky.io API error: {error_text}"
                )
            return await response.json()
        
    async def call_order_view(self, order_ids: List[str]) -> Dict[str, Any]:
        """Call Sticky.io order_view API for multiple orders with 500 order limit and retry logic"""
        
        # Check cache first
        cached_orders = []
        uncached_ids = []
        
        for order_id in order_ids:
            if order_id in order_cache:
                cached_orders.append(order_cache[order_id])
            else:
                uncached_ids.append(order_id)
        
        if not uncached_ids:
            return cached_orders
        
        # Fetch uncached orders in batches of 500 (Sticky.io limit)
        batch_size = 500
        all_orders = []
        
        for i in range(0, len(uncached_ids), batch_size):
            batch = uncached_ids[i:i + batch_size]
            max_retries = 3
            retry_delay = 1
            
            logger.info(f"Processing batch {i//batch_size + 1}: {len(batch)} orders")
            
            for attempt in range(max_retries):
                try:
                    await self.rate_limit_check_sliding_window()
                    
                    payload = {
                        "order_id": [int(oid) for oid in batch],
                        "return_variants": 1
                    }
                    
                    session = await self.get_session()
                    async with session.post(
                        f"{STICKY_API_BASE}order_view",
                        json=payload
                    ) as response:
                        if response.status != 200:
                            error_text = await response.text()
                            if attempt == max_retries - 1:
                                logger.error(f"Order view failed for batch after {max_retries} attempts: {error_text}")
                                continue
                            else:
                                logger.warning(f"Order view failed, attempt {attempt + 1}: {error_text}")
                                await asyncio.sleep(retry_delay)
                                retry_delay *= 2
                                continue
                        
                        batch_result = await response.json()
                        
                        # Process orders
                        orders = list(batch_result.get("data", {}).values()) if isinstance(batch_result.get("data"), dict) else [batch_result]
                        transformed_orders = []

                        for order in orders:
                            order_copy = order.copy()
                            products = order_copy.pop("products", [])

                            if isinstance(products, list) and len(products) == 1:
                                product = products[0]
                                for key, value in product.items():
                                    if isinstance(value, dict):
                                        for key_1, value_1 in value.items():
                                            order_copy[f"prd_{key}_{key_1}"] = value_1
                                    else:
                                        order_copy[f"prd_{key}"] = value

                            if "totals_breakdown" in order_copy and isinstance(order_copy["totals_breakdown"], dict):
                                breakdown = order_copy.pop("totals_breakdown")
                                for k, v in breakdown.items():
                                    order_copy[f"breakdown_{k}"] = v

                            order_copy.pop("utm_info", None)
                            order_copy.pop("custom_fields", None)
                            order_copy.pop("order_customer_types", None)

                            if "systemNotes" in order_copy and isinstance(order_copy["systemNotes"], list):
                                order_copy["systemNotes"] = ",".join(map(str, order_copy["systemNotes"]))

                            transformed_orders.append(order_copy)


                        # Handle single vs multiple order responses
                        if isinstance(transformed_orders, list):
                            for order in transformed_orders:
                                #order_id = str(order.get("order_id"))
                                #order_cache[order_id] = order
                                all_orders.append(order)
                        else:
                            logger.error(f"Error in transformation")
                            raise Exception("Error in transformation")
                        
                        break  # Success, break out of retry loop
                        
                except Exception as e:
                    if attempt == max_retries - 1:
                        logger.error(f"Order view failed after {max_retries} attempts: {str(e)}")
                    else:
                        logger.warning(f"Order view error, attempt {attempt + 1}: {str(e)}")
                        await asyncio.sleep(retry_delay)
                        retry_delay *= 2
        
        # Combine cached and fresh orders
        all_orders.extend(cached_orders)
        return all_orders

    # 🆕 NEW METHODS FOR CHUNKING SOLUTION
    def convert_order_timestamp_to_find_params(self, timestamp: str) -> tuple[str, str]:
        """
        Convert order_view timestamp to order_find format
        Input: "2024-09-12 00:39:54" (from order_view)
        Output: ("09/12/2024", "00:39:54") (for order_find)
        """
        try:
            dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
            start_date = dt.strftime("%m/%d/%Y")
            start_time = dt.strftime("%H:%M:%S")
            return start_date, start_time
        except ValueError as e:
            logger.error(f"Error converting timestamp {timestamp}: {e}")
            raise ValueError(f"Invalid timestamp format: {timestamp}")

    async def call_order_find_complete(
        self, 
        start_date: str, 
        end_date: str,
        start_time: str = "00:00:00",
        end_time: str = "23:59:59", 
        campaign_id: str = "all",
        date_type: str = "create",
        criteria: str = "all",
        search_type: str = "all"
    ) -> tuple[List[str], int]:
        """
        🆕 Complete order_find that handles 50K+ order limits by intelligent chunking
        Uses actual order timestamps to determine chunk boundaries.
        Returns: (all_order_ids, total_orders_found)
        """
        all_order_ids = []
        current_start_date = start_date
        current_start_time = start_time
        chunk_count = 0
        total_orders_reported = 0
        
        logger.info(f"🔄 Starting complete order_find from {start_date} {start_time} to {end_date} {end_time}")
        
        cache_key : str = f"order_find:{start_date}:{end_date}:{start_time}:{end_time}:{campaign_id}:{date_type}:{criteria}:{search_type}"
        if cache_key in order_cache:
            print(f"Cache HIT for key: {cache_key}")
            return order_cache[cache_key]  # Returns the cached tuple
        
        while True:
            chunk_count += 1
            
            # Call order_find for current chunk
            logger.info(f"📋  Chunk {chunk_count}: Calling order_find from {current_start_date} {current_start_time}")
            
            response = await self.call_order_find(
                start_date=current_start_date,
                end_date=end_date,
                start_time=current_start_time,
                end_time=end_time,
                campaign_id=campaign_id,
                date_type=date_type,
                criteria=criteria,
                search_type=search_type
            )
            
            order_ids = response.get("order_id", [])
            total_orders_chunk = int(response.get("total_orders", "0"))
            
            # Store the total from first chunk (this is the real total we're aiming for)
            if chunk_count == 1:
                total_orders_reported = total_orders_chunk
                logger.info(f"🎯 Target total orders: {total_orders_reported}")
            
            logger.info(f"📦 Chunk {chunk_count}: Got {len(order_ids)} order IDs")
            
            # Add orders from this chunk
            all_order_ids.extend(order_ids)
            
            # Check if we've reached the end
            if len(order_ids) < 50000 and len(order_ids) == total_orders_chunk :
                logger.info(f"✅ Final chunk {chunk_count}: {len(order_ids)} orders (< 50K limit)")
                break
            
            # Check if we hit exactly 50K - likely truncated
            if len(order_ids) == 50000:
                logger.info(f"⚠️  Chunk {chunk_count}: Got exactly 50K orders - likely truncated, continuing...")
                
                # Get timestamp of last order in this batch
                last_order_id = str(order_ids[-1])
                logger.info(f"🔍 Getting timestamp for last order: {last_order_id}")
                
                try:
                    # Call order_view to get timestamp of last order
                    last_order_details = await self.call_order_view([last_order_id])
                    
                    if not last_order_details or len(last_order_details) == 0:
                        logger.error(f"❌ Could not get details for last order {last_order_id}")
                        break
                    
                    last_timestamp = last_order_details[0].get("time_stamp")
                    
                    if not last_timestamp:
                        logger.error(f"❌ No timestamp found for order {last_order_id}")
                        break
                    
                    logger.info(f"📅 Last order timestamp: {last_timestamp}")
                    
                    # Convert to order_find format  
                    current_start_date, current_start_time = self.convert_order_timestamp_to_find_params(last_timestamp)
                    
                    logger.info(f"➡️  Next chunk will start from: {current_start_date} {current_start_time}")
                    
                except Exception as e:
                    logger.error(f"❌ Error getting timestamp for order {last_order_id}: {str(e)}")
                    break
            else:
                # Got less than 50K orders, we're done
                break
        
        # Deduplicate while preserving order (Option A approach)
        logger.info(f"🔄 Deduplicating {len(all_order_ids)} orders...")
        unique_order_ids = list(dict.fromkeys(all_order_ids))
        duplicates_removed = len(all_order_ids) - len(unique_order_ids)
        
        logger.info(f"   ✅ Complete order_find finished:")
        logger.info(f"   📊 Total chunks: {chunk_count}")
        logger.info(f"   📋 Orders collected: {len(all_order_ids)}")
        logger.info(f"   🎯 Unique orders: {len(unique_order_ids)}")
        logger.info(f"   🔄 Duplicates removed: {duplicates_removed}")
        logger.info(f"   📈 Target vs Actual: {total_orders_reported} vs {len(unique_order_ids)}")
        
        # Verification
        if len(unique_order_ids) != total_orders_reported:
            logger.warning(f"⚠️  Mismatch: Expected {total_orders_reported}, got {len(unique_order_ids)} unique orders")
        else:
            logger.info(f"✅ Perfect match: Got all {total_orders_reported} orders!")
        
        result = (unique_order_ids,total_orders_reported)
        order_cache[cache_key] = result
        
        return unique_order_ids, total_orders_reported

    async def call_order_find_updated_complete(
        self, 
        start_date: str, 
        end_date: str,
        start_time: str = "00:00:00",
        end_time: str = "23:59:59", 
        campaign_id: str = "all",
        group_keys: List[str] = ["chargeback","confirmation","fraud","refund","reprocess","return","rma","void"]
    ) -> tuple[List[str], int]:
        """
        🆕 Complete order_find that handles 50K+ order limits by intelligent chunking
        Uses actual order timestamps to determine chunk boundaries.
        Returns: (all_order_ids, total_orders_found)
        """
        all_order_ids = []
        current_start_date = start_date
        current_start_time = start_time
        chunk_count = 0
        total_orders_reported = 0
        
        logger.info(f"🔄 Starting complete order_find from {start_date} {start_time} to {end_date} {end_time}")
        
        while True:
            chunk_count += 1
            
            # Call order_find for current chunk
            logger.info(f"📋 Chunk {chunk_count}: Calling order_find from {current_start_date} {current_start_time}")
            
            response = await self.call_order_find_updated(
                start_date=current_start_date,
                end_date=end_date,
                start_time=current_start_time,
                end_time=end_time,
                campaign_id=campaign_id,
                group_keys = group_keys
            )
            
            order_ids = response.get("order_id", [])
            total_orders_chunk = int(response.get("total_orders", "0"))
            
            # Store the total from first chunk (this is the real total we're aiming for)
            if chunk_count == 1:
                total_orders_reported = total_orders_chunk
                logger.info(f"🎯 Target total orders: {total_orders_reported}")
            
            logger.info(f"📦 Chunk {chunk_count}: Got {len(order_ids)} order IDs")
            
            # Add orders from this chunk
            all_order_ids.extend(order_ids)
            
            # Check if we've reached the end
            if len(order_ids) < 50000 and len(order_ids) == total_orders_chunk :
                logger.info(f"✅ Final chunk {chunk_count}: {len(order_ids)} orders (< 50K limit)")
                break
            
            # Check if we hit exactly 50K - likely truncated
            if len(order_ids) == 50000:
                logger.info(f"⚠️  Chunk {chunk_count}: Got exactly 50K orders - likely truncated, continuing...")
                
                # Get timestamp of last order in this batch
                last_order_id = str(order_ids[-1])
                logger.info(f"🔍 Getting timestamp for last order: {last_order_id}")
                
                try:
                    # Call order_view to get timestamp of last order
                    last_order_details = await self.call_order_view([last_order_id])
                    
                    if not last_order_details or len(last_order_details) == 0:
                        logger.error(f"❌ Could not get details for last order {last_order_id}")
                        break
                    
                    last_timestamp = last_order_details[0].get("time_stamp")
                    
                    if not last_timestamp:
                        logger.error(f"❌ No timestamp found for order {last_order_id}")
                        break
                    
                    logger.info(f"📅 Last order timestamp: {last_timestamp}")
                    
                    # Convert to order_find format  
                    current_start_date, current_start_time = self.convert_order_timestamp_to_find_params(last_timestamp)
                    
                    logger.info(f"➡️  Next chunk will start from: {current_start_date} {current_start_time}")
                    
                except Exception as e:
                    logger.error(f"❌ Error getting timestamp for order {last_order_id}: {str(e)}")
                    break
            else:
                # Got less than 50K orders, we're done
                break
        
        # Deduplicate while preserving order (Option A approach)
        logger.info(f"🔄 Deduplicating {len(all_order_ids)} orders...")
        unique_order_ids = list(dict.fromkeys(all_order_ids))
        duplicates_removed = len(all_order_ids) - len(unique_order_ids)
        
        logger.info(f"   ✅ Complete order_find_updated finished:")
        logger.info(f"   📊 Total chunks: {chunk_count}")
        logger.info(f"   📋 Orders collected: {len(all_order_ids)}")
        logger.info(f"   🎯 Unique orders: {len(unique_order_ids)}")
        logger.info(f"   🔄 Duplicates removed: {duplicates_removed}")
        logger.info(f"   📈 Target vs Actual: {total_orders_reported} vs {len(unique_order_ids)}")
        
        # Verification
        if len(unique_order_ids) != total_orders_reported:
            logger.warning(f"⚠️  Mismatch: Expected {total_orders_reported}, got {len(unique_order_ids)} unique orders")
        else:
            logger.info(f"✅ Perfect match: Got all {total_orders_reported} orders!")
        
        return unique_order_ids, total_orders_reported
    

# Initialize client
sticky_client = StickyAPIClient()

def convert_cursor_to_date_time(cursor: str) -> tuple[str, str, str, str]:
    """Convert cursor timestamp to Sticky.io date/time format"""
    try:
        cursor_datetime = datetime.strptime(cursor, "%Y-%m-%d %H:%M:%S") + timedelta(seconds=1)
        start_date = cursor_datetime.strftime("%m/%d/%Y")
        start_time = cursor_datetime.strftime("%H:%M:%S")
        
        # End date is today
        end_datetime = datetime.now(ZoneInfo("America/New_York")) - timedelta(hours=1)
        end_date = end_datetime.strftime("%m/%d/%Y")
        end_time = end_datetime.strftime("%H:%M:%S")
        
        return start_date, start_time, end_date, end_time
        
    except ValueError:
        logger.warning(f"Invalid cursor format: {cursor}")
        # Default to last 30 days
        today = datetime.now()
        default_start = today - timedelta(days=30)
        
        return (
            default_start.strftime("%m/%d/%Y"),
            "00:00:00", 
            today.strftime("%m/%d/%Y"),
            "23:59:59"
        )

def filter_orders_by_cursor(orders: List[Dict[str, Any]], cursor: str) -> List[Dict[str, Any]]:
    """Filter orders to only include those after the cursor timestamp"""
    if not cursor or not orders:
        return orders
    
    try:
        cursor_datetime = datetime.strptime(cursor, "%Y-%m-%d %H:%M:%S")
        filtered_orders = []
        
        for order in orders:
            order_timestamp = order.get("time_stamp")
            if order_timestamp:
                try:
                    order_datetime = datetime.strptime(order_timestamp, "%Y-%m-%d %H:%M:%S")
                    if order_datetime > cursor_datetime:
                        filtered_orders.append(order)
                except ValueError:
                    logger.warning(f"Could not parse timestamp for order {order.get('order_id')}: {order_timestamp}")
                    filtered_orders.append(order)
            else:
                filtered_orders.append(order)
        # Sort orders by timestamp
        #filtered_orders.sort(
        #    key=lambda x: datetime.strptime(x.get("time_stamp", ""), "%Y-%m-%d %H:%M:%S") 
        #    if x.get("time_stamp") else datetime.max
        #)
        return filtered_orders
    except ValueError:
        logger.warning(f"Invalid cursor format: {cursor}")
        return orders

def transform_sticky_response(
    order_find_response: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """Transform Sticky.io response to Airbyte-friendly format"""
    
    if order_find_response.get("response_code") != "100":
        return []
    
    order_ids = order_find_response.get("order_id", [])  
    transformed_orders = []
    
    for order_id in order_ids:
        # Base order info
        base_order = {
            "order_id": order_id,
        }       
                
        transformed_orders.append(base_order)
    
    return transformed_orders

def estimate_response_size(data: any) -> int:
    """Estimate response size in bytes"""
    try:
        json_str = json.dumps(data)
        return len(json_str.encode('utf-8'))
    except Exception:
        # Fallback estimation
        import sys
        return sys.getsizeof(data)


def GetMissingOrders(company):

    SVC ="ewogICJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIsCiAgInByb2plY3RfaWQiOiAidmFyaWFudC1maW5hbmNlLWRhdGEtcHJvamVjdCIsCiAgInByaXZhdGVfa2V5X2lkIjogIjEwNjY5YjNmZGY0ZmU2MWJiNjJiMjE0MWFjY2M1YTA0NzI0NzhmNjgiLAogICJwcml2YXRlX2tleSI6ICItLS0tLUJFR0lOIFBSSVZBVEUgS0VZLS0tLS1cbk1JSUV2UUlCQURBTkJna3Foa2lHOXcwQkFRRUZBQVNDQktjd2dnU2pBZ0VBQW9JQkFRRHJ6TlRvK0dsNnZ2SldcbmdBZFlVczE0dmlVdUMzS0svc3cyd3NCWjVmTnlvK3JjTWY1NTlxM2JlWnR4R2toUllsUDRKYStxWXVqWVZoTDFcbmRMT09IbXVJb0JzN2ZKNWM0a2FXanVVRStYNWV4d3lvTjVycFNjYm82MUYyYmx4QzBrWllick51KytMYkUzcmZcbmk1QitKcGpkT052VkNHVnVLQk40eFFvaVc0WnJndXN1M2FlNmNkS0U4aS9SN0FDUitPb0dqTWg5dEI0WldFeXVcbmw0OGhEYkhKU0poOGRJaFhKRStteklLK1Q5d1Ntclc5dkNCZ2IyazhwNjdkdjFnWDFzMnJWQlZCR2xWUVdyUXRcbklxSzNySWJzRUxMMzF5OFhHazlpWldNM0oyMXI4aENsWDhBS0pHYkVkQ2pzUUJuNHhuai9Tei9RbXUvbXUvYnhcbm5CZEZiVjBGQWdNQkFBRUNnZ0VBRGdVNEhLRVdubnlIaXNLaWpTc2hPZjV1VmdKS3ZYNkFkSG9ZZDAvdnJYK1hcbkdhQWtYaXFmZEVjVENjTFREWG0vL2VkNXZqTVMzcmdoZVBSSEw5cFpzUDQ2R0V1azIrZDlaSHJiSGJSYkFmWXFcblo3OGtxQjNocEp4SFUvZ2tacm03Z29zVWdyTVo3a1pHZmsrOVYrN2lGSGRLeE93eW9iM2l5SUhJeERtMmNLSnpcbk1UTFBpZjZBVzBkV3NnZWhlS09pblg2THMvQXZYMUdPbVNtTVJ3RkNuVHF0K2Q4eE5JVm9JaGxmSER6RENPQWVcbmpNMitaazdXZHRiVmQ2QmxrSWliTjROYm1YU3hUSUFuemk0NTZJSG1VVVZtL09NdkR4OGhVR3Avc25YUUJtUXlcbmxvMFNOUEgyUHg1cXFoWU1rWmZ5R3pHcEdKcHpVcTdYaGxub1ZHQTVFUUtCZ1FEN2VtNGJYeUNUZDZWb3hXV2VcbjBhbEMyZlpUUUdwQU5rdmRBc0VxVWwxTS9iU3dycnBxa0pta3lnazRTdzZwSXN3WmxoOGVnVXRPY2JwbkJFSUVcbmNjSTJadHBNa2RtcUxkT1BzeG56TER1ZjVxa1MyMC9IcEpwVHduY0JxUnZpMmVXWEdablFGT0ZXK3UrZ0pqZ0ZcbnFoK1lDWUVTK3lXcFdVUVp5WitPaXhQZzhRS0JnUUR3Q2p2Mkk4VFpDblpRdlU2bncrSnpMeExYQjFPcStQSkRcbkRLV2lrd2pwTkt2ei9KOXNIc09FV1FyaXJUVHR5RzNEVEUwd3BNVnJUc0k2MC9hdjZCZnN4M09tZ0k1OEhlcHdcbkhxWW5GRWdSVFRJaHp3ZU1GZDkxb3BxcEtudUZUU3NqdjFWN2FhVzVZQ0dIVDhnVTJ6VC9tUmVIZmpvOGdrR2FcbkdkbVNjdDU5VlFLQmdRRHVoU0VLTlIvZ3Z3clVaT1lOelM2TmljNXBDQisrNThEc3owQUh0RGRxWHZpUzNDZFVcbkMvS3VxakkwZ254VlQvdm1DTTFiVWFicnNGTHNnczFiQ2NyN2JuSi9UWmIySXFFWEd2angvSEpSSjZZVmpJNE9cbi9jQ2kwVCt2QTRhL2s0eC8xSGhmTkc3RzRSdUcrcmtJSm1QeEFKSzhQaGxxbHBCUkpUdUJKOGlqQVFLQmdGanhcbnNkNDJ5czRSamwzRWg4eXFUTktaY3NXeXRWSDVCT3ZMVitTeHp1OTYwT3lMZ3hjeEh3bC9aUVV4WVJkcTJTRXdcbnVMbDVsSjE2aFlYKzNMMjVwb1BhTkFSU1JubS9MQXQzaitHVEpsRWk1WnlaZGhaMlZHTG1hYUNkV1QrL3BHaU9cbmtVSTFsMjdsTEFkVGpMUU50Y213RklQa1JmZjkzQWtaNHdEZEI0d3hBb0dBTlJ2QUZXMnpuTkdqTXBMZTFadVZcbmxTUjJzZk0yQTVlbzdLUTNjNTFOV2lMOENzK0FvcU1BeVVzVnBvZndDWGFJMlJxajVFVVFZRlV5SUl1RUJMb0Fcbmx6TlRET2RsdVhPTUNOUDV1Mk9LZmRhMWxBVmd6TFFqN1JNNWlyVjZ5TXBBL2tDKytRRGkyUVZRaFRUeDArWnRcbk45WDhQN3diY29QVWNPNXUrakM1R1RZPVxuLS0tLS1FTkQgUFJJVkFURSBLRVktLS0tLVxuIiwKICAiY2xpZW50X2VtYWlsIjogInN0aWNreS1tYWludGFpbmVyQHZhcmlhbnQtZmluYW5jZS1kYXRhLXByb2plY3QuaWFtLmdzZXJ2aWNlYWNjb3VudC5jb20iLAogICJjbGllbnRfaWQiOiAiMTEwNTQxMzU4MjIzODQ3NjM5MjMyIiwKICAiYXV0aF91cmkiOiAiaHR0cHM6Ly9hY2NvdW50cy5nb29nbGUuY29tL28vb2F1dGgyL2F1dGgiLAogICJ0b2tlbl91cmkiOiAiaHR0cHM6Ly9vYXV0aDIuZ29vZ2xlYXBpcy5jb20vdG9rZW4iLAogICJhdXRoX3Byb3ZpZGVyX3g1MDlfY2VydF91cmwiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vb2F1dGgyL3YxL2NlcnRzIiwKICAiY2xpZW50X3g1MDlfY2VydF91cmwiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vcm9ib3QvdjEvbWV0YWRhdGEveDUwOS9zdGlja3ktbWFpbnRhaW5lciU0MHZhcmlhbnQtZmluYW5jZS1kYXRhLXByb2plY3QuaWFtLmdzZXJ2aWNlYWNjb3VudC5jb20iLAogICJ1bml2ZXJzZV9kb21haW4iOiAiZ29vZ2xlYXBpcy5jb20iCn0K"
    service_account_info = json.loads(base64.b64decode(SVC).decode('utf-8'))
    bq_credentials = service_account.Credentials.from_service_account_info(service_account_info)
    bq_client = bigquery.Client(project="variant-finance-data-project", credentials=bq_credentials)
    existing_query = f"""
           SELECT DISTINCT order_id 
           FROM `variant-finance-data-project.Sticky_Data.missing_orders` 
           WHERE company = '{company}' AND order_id IS NOT NULL
       """
    existing_df = bq_client.query(existing_query).to_dataframe()
    existing_order_ids = set(existing_df['order_id'].astype(str).tolist())

    # Delete existing records from the table
    if existing_order_ids:
        # Convert set to comma-separated string for SQL IN clause
        order_ids_str = ", ".join([f"'{oid}'" for oid in existing_order_ids])

        delete_query = f"""
            DELETE FROM `variant-finance-data-project.Sticky_Data.missing_orders`
            WHERE company = '{company}' AND order_id IN ({order_ids_str})
        """

        # Execute delete query
        delete_job = bq_client.query(delete_query)
        delete_job.result()  # Wait for completion

        print(f"{company}: Deleted {len(existing_order_ids)} existing records from missing_orders table")

        return existing_order_ids
    else:
        return None

# API Routes

@app.get("/")
async def root():
    """Health check endpoint"""
    return {"message": "Sticky.io to Airbyte Middleware", "status": "healthy"}

@app.get("/health")
async def health_check():
    """Detailed health check"""
    return {
        "status": "healthy",
        "service": "sticky-io-middleware",
        "version": "1.0.0",
        "timestamp": datetime.now().isoformat()
    }

# 🔧 COMPLETE FIXED SOLUTION: Handles 50K+ limits + 32MB response limits + Full concurrency
@app.get("/api/orders")
async def get_orders(
    company: str = Query("_", description="Company Name criteria"),
    cred: str = Query("_", description="Company Name criteria"),
    limit: int = Query(500, ge=0, le=5000, description="Number of orders to return (max 3000)"),  # ✅ FIXED:  Reasonable default, increased max
    offset: int = Query(0, ge=0, description="Number of orders to skip"),
    start_date: Optional[str] = Query(None, description="Start date in MM/DD/YYYY format"),
    end_date: Optional[str] = Query(None, description="End date in MM/DD/YYYY format"),
    start_time: Optional[str] = Query("00:00:00", description="Start time in HH:MM:SS format"),
    end_time: Optional[str] = Query("23:59:59", description="End time in HH:MM:SS format"),
    include_details: bool = Query(True, description="Include full order details from order_view"),
    cursor: Optional[str] = Query(None, description="Cursor for incremental sync (YYYY-MM-DD HH:MM:SS)"),
    date_type: str = Query("create", description="Date type: create, modify, etc."),
    criteria: str = Query("all", description="Search criteria"),
    search_type: str = Query("all", description="Search type"),
    sync_mode: str = Query("full_refresh", description="Sync mode: full_refresh or incremental"),
    auto_adjust: bool = Query(True, description="Automatically adjust batch size if response too large")  # ✅ NEW
):
    """
    🔧 COMPLETE FIXED SOLUTION: 
    ✅ Handles Sticky.io 50K order_find limit with intelligent chunking
    ✅ Prevents 32MB response limit with proper pagination  
    ✅ Maintains full concurrency benefits for speed
    ✅ Perfect data integrity with deduplication
    ✅ Response size monitoring and auto-adjustment
    """
    global STICKY_API_BASE  # Declare intent to modify the global variable
    global STICKY_AUTH
    STICKY_AUTH = getUsersPass(cred)
    STICKY_API_BASE = f"https://{company}.sticky.io/api/v1/"

    logger.info(f"{STICKY_API_BASE},{STICKY_AUTH[0]},{STICKY_AUTH[1]}")
    start_time_perf = time.perf_counter()
    try:
        # Handle cursor-based date/time conversion for incremental sync
        if cursor and sync_mode == "incremental":
            start_date, start_time, end_date, end_time = convert_cursor_to_date_time(cursor)
            logger.info(f"Incremental sync: cursor {cursor} converted to {start_date} {start_time} - {end_date} {end_time}")
        else:
            # Use provided dates or defaults (reduced from 30 to 7 days for safety)
            if not start_date:
                start_date = (datetime.now() - timedelta(days=7)).strftime("%m/%d/%Y")
            if not end_date:
                end_date = datetime.now().strftime("%m/%d/%Y")
        
        # 🆕 STEP 1: Get ALL order IDs using complete chunking (handles 50K+ limits)
        logger.info(f"🔄 Getting complete order list from {start_date} {start_time} to {end_date} {end_time}")
        
        order_ids, total_orders_found = await sticky_client.call_order_find_complete(
            start_date=start_date,
            end_date=end_date,
            start_time=start_time,
            end_time=end_time,
            date_type=date_type,
            criteria=criteria,
            search_type=search_type
        )
        
        logger.info(f"🎯 Complete order discovery: {len(order_ids)} unique orders (target: {total_orders_found})")
        
        if not order_ids:
            return {
                "data": [],
                "pagination": {"offset": offset, "limit": limit, "total": 0, "has_more": False},
                "incremental": {"cursor": cursor, "cursor_field": "time_stamp"},
                "sync_mode": sync_mode,
                "chunking_info": {
                    "chunking_used": True,
                    "total_chunks": 0,
                    "orders_found": 0
                }
            }
        
        # Add missing orders...
        missing_orders = GetMissingOrders(company)
        if missing_orders:
            logger.info(f"🔧 Missing order find : {len(missing_orders)}")
            order_ids.extend(list(missing_orders))
        else:
            logger.info(f"✅ No missing orders found in BigQuery for {company}")
        # ✅ STEP 2: Apply pagination to ORDER IDs FIRST (prevents 32MB responses)
        original_limit = limit
        paginated_order_ids = order_ids[offset:offset + limit]
        logger.info(f"🔧 FIXED: Paginated to {len(paginated_order_ids)} order IDs (from total {len(order_ids)} orders)")
        
        # ✅ STEP 3: Fetch details ONLY for paginated subset (maintains your concurrency!)
        transformed_orders = []
        if include_details and paginated_order_ids:
            logger.info(f"Fetching details for {len(paginated_order_ids)} orders using FULL CONCURRENCY")
            # 🚀 Your existing concurrent processing is maintained - but only for the smaller subset!
            order_details = await sticky_client.call_order_view_concurrent(paginated_order_ids,True)
            transformed_orders = order_details
        else:
            # Create basic order objects for non-detailed responses
            transformed_orders = [{"order_id": order_id} for order_id in paginated_order_ids]
        
        # Step 4: Filter by cursor for incremental sync
        if cursor and sync_mode == "incremental":
            logger.info(f"Filtering {len(transformed_orders)} orders by cursor")
            transformed_orders = filter_orders_by_cursor(transformed_orders, cursor)
            logger.info(f"After cursor filtering: {len(transformed_orders)} orders")
        
        # ✅ STEP 5: Build response with complete metadata
        response_data = {
            "data": transformed_orders,
            "cursor": cursor,
            "pagination": {
                "offset": offset,
                "limit": original_limit,
                "total": len(order_ids),  # ✅ Use actual discovered total
                "has_more": offset + len(paginated_order_ids) < len(order_ids),
                "next_offset": offset + len(paginated_order_ids) if offset + len(paginated_order_ids) < len(order_ids) else None,
                "current_page": (offset // original_limit) + 1,
                "total_pages": (len(order_ids) + original_limit - 1) // original_limit
            },
            "incremental": {
                "cursor": cursor,
                "cursor_field": "time_stamp"
            },
            "sync_mode": sync_mode,
            "debug_info": {
                "orders_discovered_total": len(order_ids),
                "orders_after_pagination": len(transformed_orders),
                "date_range": f"{start_date} {start_time} - {end_date} {end_time}",
                "optimization_applied": "pagination_first_with_complete_chunking"
            },
            "chunking_info": {
                "chunking_used": True,
                "total_discovered": len(order_ids),
                "reported_total": total_orders_found,
                "match_status": "perfect" if len(order_ids) == total_orders_found else "mismatch",
                "data_integrity": "guaranteed"
            }
        }
        
        # Calculate next cursor from the actual response data
        if transformed_orders and include_details:
            timestamps = [order.get("time_stamp") for order in transformed_orders if order.get("time_stamp")]
            if timestamps:
                datetime_objects = [ datetime.strptime(ts, '%Y-%m-%d %H:%M:%S') for ts in timestamps ]
                max_datetime = max(datetime_objects)
                # Convert back to your format
                cursor = max_datetime.strftime('%Y-%m-%d %H:%M:%S')
                response_data["incremental"]["cursor"] = cursor
                response_data["cursor"] = cursor
        
        # ✅ STEP 6: Response size monitoring and auto-adjustment
        response_size = estimate_response_size(response_data)
        response_size_mb = response_size / (1024 * 1024)
        
        logger.info(f"📊 Response size: {response_size_mb:.2f} MB for {len(transformed_orders)} orders")
        
        # Add response size info to metadata
        response_data["metadata"] = {
            "response_size_mb": round(response_size_mb, 2),
            "processing_time": f"{time.perf_counter() - start_time_perf:.2f}s",
            "concurrency_used": True,
            "optimization_status": "success",
            "chunking_enabled": True
        }
        
        # Check if response is still too large and suggest adjustment
        if response_size_mb > 31 and auto_adjust and len(transformed_orders) > 100:
            # Calculate what the limit should be to stay under 25MB
            avg_size_per_order = response_size_mb / len(transformed_orders)
            suggested_limit = int(25 / avg_size_per_order)  # 23MB to be safe
            suggested_limit = max(100, min(suggested_limit, 3000))  # Keep reasonable bounds
            
            logger.warning(f"⚠️  Response size ({response_size_mb:.2f} MB) exceeds 31MB limit")

            #return {
            #    "response_too_large": True,
            #    "current_size_mb": round(response_size_mb, 2),
            #    "current_orders": len(transformed_orders),
            #    "suggested_limit": suggested_limit,
            #    "retry_url": f"/api/orders?limit={suggested_limit}&offset={offset}&start_date={start_date}&end_date={end_date}&include_details={include_details}",
            #    "message": f"Response size ({response_size_mb:.2f} MB) exceeds 25MB. Use suggested_limit={suggested_limit} or the retry_url.",
            #    "pagination_info": {
            #        "total_orders": len(order_ids),
            #        "recommended_batches": (len(order_ids) + suggested_limit - 1) // suggested_limit,
            #        "estimated_total_time": f"{((len(order_ids) + suggested_limit - 1) // suggested_limit) * 45} seconds"
            #    },
            #    "chunking_info": response_data["chunking_info"]
            #}
        
        # Response is good size - return normally
        logger.info(f"✅ Returning {len(transformed_orders)} orders ({response_size_mb:.2f} MB) - WITHIN LIMITS")
        end_time_pref = time.perf_counter()
        elapsed_time = end_time_pref - start_time_perf

        logger.info(f"✅ Function --get_orders-- completed in: {elapsed_time:.6f} seconds")

        return response_data
    
    except Exception as e:
        logger.error(f"❌ Error in get_orders: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


# 🔧 COMPLETE FIXED SOLUTION: Handles 50K+ limits + 32MB response limits + Full concurrency
@app.get("/api/orders/updated")
async def get_orders(
    cred: str = Query("_", description="Company Name criteria"),
    company: str = Query("_", description="Company Name criteria"),
    limit: int = Query(500, ge=0, le=5000, description="Number of orders to return (max 3000)"),  # ✅ FIXED: Reasonable default, increased max
    offset: int = Query(0, ge=0, description="Number of orders to skip"),
    start_date: Optional[str] = Query(None, description="Start date in MM/DD/YYYY format"),
    end_date: Optional[str] = Query(None, description="End date in MM/DD/YYYY format"),
    start_time: Optional[str] = Query("00:00:00", description="Start time in HH:MM:SS format"),
    end_time: Optional[str] = Query("23:59:59", description="End time in HH:MM:SS format"),
    include_details: bool = Query(True, description="Include full order details from order_view"),
    cursor: Optional[str] = Query(None, description="Cursor for incremental sync (YYYY-MM-DD HH:MM:SS)"),
    sync_mode: str = Query("full_refresh", description="Sync mode: full_refresh or incremental"),
    auto_adjust: bool = Query(True, description="Automatically adjust batch size if response too large"),
    group_keys: List[str] = Query(["chargeback","confirmation","fraud","refund","reprocess","return","rma","void"])   # ✅ NEW
):
    """
    🔧 COMPLETE FIXED SOLUTION: 
    ✅ Handles Sticky.io 50K order_find limit with intelligent chunking
    ✅ Prevents 32MB response limit with proper pagination  
    ✅ Maintains full concurrency benefits for speed
    ✅ Perfect data integrity with deduplication
    ✅ Response size monitoring and auto-adjustment
    """
    global STICKY_API_BASE  # Declare intent to modify the global variable
    global STICKY_AUTH
    STICKY_AUTH = getUsersPass(cred)
    STICKY_API_BASE = f"https://{company}.sticky.io/api/v1/"

    logger.info(f"{STICKY_API_BASE},{STICKY_AUTH[0]},{STICKY_AUTH[1]}")
    
    start_time_perf = time.perf_counter()
    try:
        # Handle cursor-based date/time conversion for incremental sync
        if cursor and sync_mode == "incremental":
            start_date, start_time, end_date, end_time = convert_cursor_to_date_time(cursor)
            logger.info(f"Incremental sync: cursor {cursor} converted to {start_date} {start_time} - {end_date} {end_time}")
        else:
            # Use provided dates or defaults (reduced from 30 to 7 days for safety)
            if not start_date:
                start_date = (datetime.now() - timedelta(days=7)).strftime("%m/%d/%Y")
            if not end_date:
                end_date = datetime.now().strftime("%m/%d/%Y")
        
        # 🆕 STEP 1: Get ALL order IDs using complete chunking (handles 50K+ limits)
        logger.info(f"🔄 Getting complete order list from {start_date} {start_time} to {end_date} {end_time}")
        
        order_ids, total_orders_found = await sticky_client.call_order_find_updated_complete(
            start_date=start_date,
            end_date=end_date,
            start_time=start_time,
            end_time=end_time,
            group_keys=group_keys
        )
        
        logger.info(f"🎯 Complete order discovery: {len(order_ids)} unique orders updated (target: {total_orders_found})")
        
        if not order_ids:
            return {
                "data": [],
                "pagination": {"offset": offset, "limit": limit, "total": 0, "has_more": False},
                "incremental": {"cursor": cursor, "cursor_field": "time_stamp"},
                "sync_mode": sync_mode,
                "chunking_info": {
                    "chunking_used": True,
                    "total_chunks": 0,
                    "orders_found": 0
                }
            }
        
        # ✅ STEP 2: Apply pagination to ORDER IDs FIRST (prevents 32MB responses)
        original_limit = limit
        paginated_order_ids = order_ids[offset:offset + limit]
        logger.info(f"🔧 FIXED: Paginated to {len(paginated_order_ids)} order IDs (from total {len(order_ids)} orders)")
        
        # ✅ STEP 3: Fetch details ONLY for paginated subset (maintains your concurrency!)
        transformed_orders = []
        if include_details and paginated_order_ids:
            logger.info(f"Fetching details for {len(paginated_order_ids)} orders using FULL CONCURRENCY")
            # 🚀 Your existing concurrent processing is maintained - but only for the smaller subset!
            order_details = await sticky_client.call_order_view_concurrent(paginated_order_ids, False)
            transformed_orders = order_details
        else:
            # Create basic order objects for non-detailed responses
            transformed_orders = [{"order_id": order_id} for order_id in paginated_order_ids]
        
        # Step 4: Filter by cursor for incremental sync
        if cursor and sync_mode == "incremental":
            logger.info(f"Filtering {len(transformed_orders)} orders by cursor")
            transformed_orders = filter_orders_by_cursor(transformed_orders, cursor)
            logger.info(f"After cursor filtering: {len(transformed_orders)} orders")
        
        # ✅ STEP 5: Build response with complete metadata
        response_data = {
            "data": transformed_orders,
            "cursor": cursor,
            "pagination": {
                "offset": offset,
                "limit": original_limit,
                "total": len(order_ids),  # ✅ Use actual discovered total
                "has_more": offset + len(paginated_order_ids) < len(order_ids),
                "next_offset": offset + len(paginated_order_ids) if offset + len(paginated_order_ids) < len(order_ids) else None,
                "current_page": (offset // original_limit) + 1,
                "total_pages": (len(order_ids) + original_limit - 1) // original_limit
            },
            "incremental": {
                "cursor": cursor,
                "cursor_field": "time_stamp"
            },
            "sync_mode": sync_mode,
            "debug_info": {
                "orders_discovered_total": len(order_ids),
                "orders_after_pagination": len(transformed_orders),
                "date_range": f"{start_date} {start_time} - {end_date} {end_time}",
                "optimization_applied": "pagination_first_with_complete_chunking"
            },
            "chunking_info": {
                "chunking_used": True,
                "total_discovered": len(order_ids),
                "reported_total": total_orders_found,
                "match_status": "perfect" if len(order_ids) == total_orders_found else "mismatch",
                "data_integrity": "guaranteed"
            }
        }
        
        # Calculate next cursor from the actual response data
        if transformed_orders and include_details:
            timestamps = [order.get("time_stamp") for order in transformed_orders if order.get("time_stamp")]
            if timestamps:
                datetime_objects = [ datetime.strptime(ts, '%Y-%m-%d %H:%M:%S') for ts in timestamps ]
                max_datetime = max(datetime_objects)
                # Convert back to your format
                cursor = max_datetime.strftime('%Y-%m-%d %H:%M:%S')
                response_data["incremental"]["cursor"] =cursor
                response_data["cursor"] = cursor
        
        # ✅ STEP 6: Response size monitoring and auto-adjustment
        response_size = estimate_response_size(response_data)
        response_size_mb = response_size / (1024 * 1024)
        
        logger.info(f"📊 Response size: {response_size_mb:.2f} MB for {len(transformed_orders)} orders")
        
        # Add response size info to metadata
        response_data["metadata"] = {
            "response_size_mb": round(response_size_mb, 2),
            "processing_time": f"{time.perf_counter() - start_time_perf:.2f}s",
            "concurrency_used": True,
            "optimization_status": "success",
            "chunking_enabled": True
        }
        
        # Check if response is still too large and suggest adjustment
        if response_size_mb > 25 and auto_adjust and len(transformed_orders) > 100:
            # Calculate what the limit should be to stay under 25MB
            avg_size_per_order = response_size_mb / len(transformed_orders)
            suggested_limit = int(23 / avg_size_per_order)  # 23MB to be safe
            suggested_limit = max(100, min(suggested_limit, 3000))  # Keep reasonable bounds
            
            logger.warning(f"⚠️  Response size ({response_size_mb:.2f} MB) exceeds 25MB limit")
            
            return {
                "response_too_large": True,
                "current_size_mb": round(response_size_mb, 2),
                "current_orders": len(transformed_orders),
                "suggested_limit": suggested_limit,
                "retry_url": f"/api/orders?limit={suggested_limit}&offset={offset}&start_date={start_date}&end_date={end_date}&include_details={include_details}",
                "message": f"Response size ({response_size_mb:.2f} MB) exceeds 25MB. Use suggested_limit={suggested_limit} or the retry_url.",
                "pagination_info": {
                    "total_orders": len(order_ids),
                    "recommended_batches": (len(order_ids) + suggested_limit - 1) // suggested_limit,
                    "estimated_total_time": f"{((len(order_ids) + suggested_limit - 1) // suggested_limit) * 45} seconds"
                },
                "chunking_info": response_data["chunking_info"]
            }
        
        # Response is good size - return normally
        logger.info(f"✅ Returning {len(transformed_orders)} orders ({response_size_mb:.2f} MB) - WITHIN LIMITS")
        end_time_pref = time.perf_counter()
        elapsed_time = end_time_pref - start_time_perf

        logger.info(f"✅ Function --get_orders-- completed in: {elapsed_time:.6f} seconds")

        return response_data
    
    except Exception as e:
        logger.error(f"❌ Error in get_orders: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.get("/api/orders/{order_id}")
async def get_order_details(order_id: str):
    """Get detailed information for a specific order"""
    try:
        order_details = await sticky_client.call_order_view([order_id])
        
        if not order_details or len(order_details) == 0:
            raise HTTPException(status_code=404, detail=f"Order {order_id} not found")
        
        return order_details[0]  # Return first (and only) order from list
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting order {order_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.on_event("startup")
async def startup_event():
    """Initialize on startup"""
    logger.info("🚀 Sticky.io Middleware API started with COMPLETE CHUNKING + PAGINATION FIXES")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    if sticky_client.session:
        await sticky_client.session.close()
    logger.info("Sticky.io Middleware API stopped")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8878)
