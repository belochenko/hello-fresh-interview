# Recipe ETL Process for HelloFresh as a part of interview process

This program processes a dataset of recipes, specifically targeting recipes that contain chilies as an ingredient. The processed data is then saved to CSV files.

## Prerequisites

- Python 3.x
- Required Python libraries: `requests`

## How to Run

## Features

1. **Download Capability**: Downloads the recipe dataset from a provided URL.
2. **Streaming**: Efficiently handles potentially large datasets by streaming the download and processing the data line-by-line.
3. **Data Processing**: Filters recipes based on the presence of chilies (or its variants) in the ingredients. Calculates the difficulty level of each recipe based on prep and cook times.
4. **Data Aggregation**: Computes the average total time for each difficulty level.
5. **CSV Output**: Saves the processed recipes to `Chilies.csv` and the aggregated data to `Results.csv`. Both files use `|` as a separator.

## Setup and Execution

1. Ensure you have the required libraries installed, mainly `requests` for data downloading.
2. Run the main script: `python recipes-etl/main.py`.
3. The processed data will be saved in the `data` directory as `Chilies.csv` and `Results.csv`.

## Testing

The program includes unit tests to ensure its functionality. To run the tests:

```bash
pytest recipes-etl/src/tests/test_etl_pipeline.py
```