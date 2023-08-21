import requests
import json
import re
import csv

from typing import Dict, Union, Any, Generator, List, Optional


def stream_download(url: str, local_filename: str) -> None:
    """
    Download a file in chunks from a given URL and save it locally.

    Why streaming?
    1. Memory Efficiency: By streaming the content, we don't need to load the entire file into memory,
       which is especially beneficial for large files.
    2. Speed: We can start processing (or saving) parts of the file while the rest is still being downloaded.
    3. Stability: In case of network interruptions or issues, streaming can allow for retries of specific
       chunks rather than restarting the download of the entire file.
    """
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        with open(local_filename, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)


def recipe_generator(filename: str) -> Generator[Dict[str, Any], None, None]:
    """
    Generator that yields individual recipes from a non-standard JSON file.
    Processes the file line-by-line to optimize memory usage.
    """
    with open(filename, 'r') as file:
        buffer = ""
        for line in file:
            line = line.strip()
            if line == "":  # skip empty lines
                continue
            buffer += line
            if line.endswith("}"):
                try:
                    recipe = json.loads(buffer)
                    yield recipe
                    buffer = ""
                except json.decoder.JSONDecodeError:
                    print(f"Failed to decode: {buffer}")
                    buffer = ""


def process_recipe(recipe: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Process individual recipe to determine difficulty based on prepTime and cookTime.
    Filters recipes based on the presence of "chilies" or its variants.
    """

    # This regex pattern ensures:
    # - Starts and ends at word boundaries (\b).
    # - Matches "chili", "chilie", "chilies", "chilis", "chile", or "chiles".
    # - Ensures it doesn't match substrings of larger words, like "chilled".
    pattern = re.compile(r'\b(chil(?:i|ie)s?|chiles?)\b', re.IGNORECASE)
    if pattern.search(recipe.get('ingredients', '')):
        # Extracting time from the 'PTxxM' format
        prep_time_match = re.search(r'(\d+)', recipe.get('prepTime', 'PT0M'))
        cook_time_match = re.search(r'(\d+)', recipe.get('cookTime', 'PT0M'))

        # Safely extract prep_time and cook_time
        prep_time = int(prep_time_match.group(1)) if prep_time_match else 0
        cook_time = int(cook_time_match.group(1)) if cook_time_match else 0

        total_time = prep_time + cook_time

        # Determine difficulty based on total_time
        if total_time > 60:
            difficulty = "Hard"
        elif 30 <= total_time <= 60:
            difficulty = "Medium"
        elif total_time < 30:
            difficulty = "Easy"
        else:
            difficulty = "Unknown"

        recipe['difficulty'] = difficulty
        recipe['total_time'] = total_time
        return recipe
    return None


def save_to_csv(data: List[Dict[str, Union[str, int]]], filename: str) -> None:
    """
    Save a list of dictionaries to a CSV file with "|" as a separator.
    """
    with open(filename, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys(), delimiter='|')
        writer.writeheader()
        for recipe in data:
            writer.writerow(recipe)


def aggregate_difficulty(data: List[Dict[str, Union[str, int]]]) -> List[Dict[str, Union[str, int]]]:
    """
    Aggregate the average total_time for recipes at each difficulty level.
    """
    aggregated_data = []
    difficulties = ["Easy", "Medium", "Hard"]
    for difficulty in difficulties:
        total_times = [recipe['total_time'] for recipe in data if recipe['difficulty'] == difficulty]
        average_time = sum(total_times) / len(total_times) if total_times else 0
        aggregated_data.append({"difficulty": difficulty, "AverageTotalTime": average_time})
    return aggregated_data
