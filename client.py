import os
import math
import aiohttp
import asyncio
import logging
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class Config:
    BASE_URL = "http://localhost:6000/stream"
    BATCH_SIZE = 16  
    CORES = os.cpu_count()
    MAX_CONCURRENT_REQUESTS = min(CORES or 1 * 4, 64)
    EARTH_RADIUS_KM = 6371.0  # Earth's radius in kilometers


class GPSUtils:
    @staticmethod
    def haversine_distance(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
        """Calculate the Haversine distance between two GPS points."""
        lat1, lng1, lat2, lng2 = map(math.radians, [lat1, lng1, lat2, lng2])
        dlat = lat2 - lat1
        dlng = lng2 - lng1
        a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlng / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return Config.EARTH_RADIUS_KM * c

    @staticmethod
    def process_gps(previous_point: Optional[Dict[str, float]], current_gps: Dict[str, Any]) -> Tuple[float, Dict[str, float]]:
        """
        Extract GPS data and calculate the distance from the previous point.
        Always returns a valid dictionary for the next point.
        """
        try:
            lat = float(current_gps.get("lat", 0))
            lng = float(current_gps.get("lng", 0))

            if previous_point and lat == previous_point["lat"] and lng == previous_point["lng"]:
                return 0.0, previous_point

            if previous_point:
                distance = GPSUtils.haversine_distance(previous_point["lat"], previous_point["lng"], lat, lng)
                return distance, {"lat": lat, "lng": lng}
            
            return 0.0, {"lat": lat, "lng": lng}
        
        except (ValueError, TypeError, KeyError) as e:
            logging.warning(f"Invalid GPS data encountered: {current_gps}, Error: {e}")
            return 0.0, previous_point

class DataUtils:
    @staticmethod
    def sort_by_timestamp(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Sort data by the GPS read_timestamp field."""
        try:
            return sorted(data, key=lambda x: x["gps"]["read_timestamp"])
        except (KeyError, TypeError):
            return []


class DataFetcher:
    def __init__(self, url: str, batch_size: int, max_concurrent_requests: int):
        self.url = url
        self.batch_size = batch_size
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        self.stop_event = asyncio.Event()

    async def fetch_sample(self, session: aiohttp.ClientSession, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Fetch a single sample from the server."""
        async with self.semaphore:
            if self.stop_event.is_set():
                return None

            try:
                async with session.get(self.url, params=params) as response:
                    if self.stop_event.is_set():  # Check if a 404 has already been encountered
                        return None
                    elif response.status == 200:
                        return await response.json()
                    elif response.status == 404:
                        self.stop_event.set()
                        logging.info("Received 404. Stopping further requests.")
                    else:
                        logging.warning(f"Unexpected status {response.status} for params {params}")
            except aiohttp.ClientError as e:
                logging.error(f"Client error while fetching sample with params {params}: {e}")
            except asyncio.TimeoutError:
                logging.error(f"Request timed out for params {params}")
            except Exception as e:
                logging.error(f"Unexpected error while fetching sample with params {params}: {e}")
            return None

    async def fetch_batch(self, session: aiohttp.ClientSession, batch_index: int) -> List[Dict[str, Any]]:
        """Fetch a batch of data using parallel sample fetches."""
        logging.info(f"Starting to fetch batch {batch_index}...")
        try:
            tasks = [
                self.fetch_sample(session, {"batch_index": batch_index, "sample_index": i})
                for i in range(self.batch_size)
            ]
            results = await asyncio.gather(*tasks)

            #Handle exceptions in results
            valid_results = []
            for result in results:
                if isinstance(result, Exception):
                    logging.error(f"Task failed with exception: {result}")
                elif result is not None:
                    valid_results.append(result)
            
            return valid_results
        
        except Exception as e:
            logging.error(f"Unexpected error while fetching batch {batch_index}: {e}")
            return []

    async def async_data_generator(self, session: aiohttp.ClientSession) -> AsyncGenerator[List[Dict[str, Any]], None]:
        """Generate batches of data asynchronously."""
        batch_index = 1
        while not self.stop_event.is_set():
            try:
                batch = await self.fetch_batch(session, batch_index)
                if not batch:
                    break
                yield batch
                batch_index += 1
            except Exception as e:
                logging.error(f"Error in async_data_generator for batch {batch_index}: {e}")
                break

# --- MAIN PROCESSING ---
class DataProcessor:
    def __init__(self):
        self.total_distance = 0.0
        self.total_points = 0
        self.previous_point = None
        self.processed_ids = set()

    async def process_batch(self, batch: List[Dict[str, Any]]):
        """Process a single batch of data."""
        sorted_batch = DataUtils.sort_by_timestamp(batch)
        for sample in sorted_batch:
            gps_data = sample.get("gps")
            frame_data = sample.get("frame")

            if gps_data and frame_data:
                unique_id = (gps_data.get("read_timestamp"), frame_data.get("frame_id"))
                if unique_id in self.processed_ids:
                    continue

                self.processed_ids.add(unique_id)
                distance, self.previous_point = GPSUtils.process_gps(self.previous_point, gps_data)
                self.total_distance += distance
                logging.info(f"Current Distance: {self.total_distance:.6f} km")

    async def process_data(self, url: str, batch_size: int, max_concurrent_requests: int):
        """Fetch and process data from the server."""
        fetcher = DataFetcher(url, batch_size, max_concurrent_requests)

        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=max_concurrent_requests)) as session:
            async for batch in fetcher.async_data_generator(session):
                await self.process_batch(batch)

        self.total_points = len(self.processed_ids)
        logging.info(f"Final Total Distance: {self.total_distance:.6f} km")
        logging.info(f"Total Points Processed: {self.total_points}")


async def main():
    processor = DataProcessor()
    await processor.process_data(Config.BASE_URL, Config.BATCH_SIZE, Config.MAX_CONCURRENT_REQUESTS)


if __name__ == "__main__":
    asyncio.run(main())
