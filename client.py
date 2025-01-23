import requests
import logging
import json
import math
import time


def calculate_distance(lat1, lng1, lat2, lng2):
    """Calculate the Haversine distance between two GPS points."""
    R = 6371.0  # Earth's radius in kilometers, more precise with a float
    # Convert degrees to radians
    lat1, lng1, lat2, lng2 = map(math.radians, [lat1, lng1, lat2, lng2])
    # Differences in latitude and longitude
    dlat = lat2 - lat1
    dlng = lng2 - lng1
    # Haversine formula
    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlng / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = R * c
    return distance 


def parse_data(line):
    """Parse a single line of data into a JSON object."""
    try:
        parsed_data = json.loads(line)
        logging.debug(f"Parsed data: {parsed_data}")
        return parsed_data
    except json.JSONDecodeError as e:
        logging.warning(f"Failed to decode JSON: {e}. Line: {line}")
        return None


def process_gps_data(previous_point, current_gps):
    """Extract GPS data and calculate the distance from the previous point."""
    try:
        lat = float(current_gps.get("lat", 0))
        lng = float(current_gps.get("lng", 0))

        # Skip processing if the coordinates are identical
        if previous_point and lat == previous_point["lat"] and lng == previous_point["lng"]:
            logging.info(f"Skipping identical GPS point: {current_gps}")
            return 0, previous_point

        if previous_point is not None:
            distance = calculate_distance(
                previous_point["lat"], previous_point["lng"], lat, lng
            )
            logging.info(f"Latest distance calculated: {distance:.6f} km")
            return distance, {"lat": lat, "lng": lng}

        # For the first data point
        return 0, {"lat": lat, "lng": lng}
    except (TypeError, ValueError) as e:
        logging.warning(f"Invalid GPS data encountered: {current_gps}, Error: {e}")
        return 0, previous_point
    

def fetch_and_process_data(url):
    """Fetch data from the server and calculate the total distance incrementally."""
    logging.debug("Starting to fetch and process data from the server...")
    retry_count = 0
    max_retries = 3
    total_distance = 0.0
    total_points = 0
    previous_point = None

    # Create a persistent session
    with requests.Session() as session:
        while True:
            try:
                response = session.get(url, stream=True)
                logging.debug(f"Received response with status code: {response.status_code}")
                if response.status_code == 404:
                    logging.info("No more data available from the server.")
                    break

                response.raise_for_status()

                json_buffer = ""
                for line in response.iter_lines(decode_unicode=True):
                    if line:
                        json_buffer += line.strip()
                        if json_buffer.startswith("{") and json_buffer.endswith("}"):
                            data = parse_data(json_buffer)
                            json_buffer = ""  # Reset buffer for the next object

                            if data is None:
                                continue

                            gps_data = data.get("gps")
                            if not gps_data:
                                logging.warning(f"Missing GPS data in: {data}")
                                continue

                            distance, previous_point = process_gps_data(previous_point, gps_data)
                            total_distance += distance
                            total_points += 1

            except requests.ConnectionError as e:
                retry_count += 1
                logging.error(f"Connection error: {e}. Retrying {retry_count}/{max_retries}...")
                if retry_count >= max_retries:
                    logging.error("Max retries reached. Terminating.")
                    break
                time.sleep(2)
            except requests.RequestException as e:
                logging.error(f"Request error: {e}. Terminating.")
                break

    logging.info(f"Final total distance: {total_distance:.6f} km")
    logging.info(f"Total data points processed: {total_points}")
    return total_distance, total_points


def main():
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
    url = "http://localhost:6000/stream"
    total_distance, total_points = fetch_and_process_data(url)
    logging.info(f"Summary: Total Distance = {total_distance:.2f} km, Total Points = {total_points}")

if __name__ == "__main__":
    main()