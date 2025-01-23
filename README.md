# GPS Data Processor 

This script calculates the total distance traveled based on a stream of GPS data received from a server.

## Architecture

* **Data Source:** Expects a stream of JSON objects, each containing GPS data (latitude and longitude).
* **Distance Calculation:** Currently uses the Haversine formula. Can be improved by using `geopy` for more accurate and potentially faster distance calculations.
    * **Error Handling:** Implements basic error handling for:
    * **Connection Errors:** Retries a limited number of times before failing.
    * **Invalid Data:** Skips invalid or missing GPS data points.
* **Data Processing:** Processes data incrementally, calculating distance and maintaining state between requests.

## Dependencies

* **Python:** Requires Python 3.9.
* **Requests:** Install using `pip install requests`

## Installation & Execution

1. **Save the Script:** Save the provided code as a Python file (e.g., `client.py`).

2. **Modify URL (Optional):** If the server providing the GPS data stream is located at a different URL, modify the `url` variable in the `main` function accordingly.

3. **Install Dependencies:** 

- Open your terminal or command prompt and run the following command to install the necessary libraries: using `pip install requests` in the terminal.

4. **Run the Script:** 

- Open Terminal/Command Prompt: Open your terminal or command prompt and navigate to the directory where you saved the client.py file. You can use the cd command to change directories.

- Execute the Script: Run the following command in your terminal or command prompt: `python client.py`


## Output

The script will output the following information to the console:

* **Total Distance:** The total distance traveled calculated from the GPS data.
* **Total Data Points:** The number of data points processed.


## Notes:

The script assumes that the server provides a stream of JSON data containing GPS information (latitude and longitude).

## Future Improvement Suggestions:
- Distance Calculation: By using geopy.distance.distance().
- Data Validation:  Implement stricter input validation to ensure that the received GPS data is within valid ranges.
- Error Handling: More Granular Error Handling.
- Caching: To store and reuse frequently calculated distances.