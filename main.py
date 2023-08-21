import json
from pathlib import Path

from src.etl_pipeline import stream_download, process_recipe, recipe_generator, save_to_csv, \
    aggregate_difficulty

script_dir = Path(__file__).parent


def ensure_data_folder() -> Path:
    """
    Ensures that the data folder exists.
    If it doesn't, the folder is created.
    """
    data_dir = script_dir / "data"  # This will be the absolute path to the data directory
    if not data_dir.exists():
        data_dir.mkdir()

    return data_dir


if __name__ == "__main__":
    data_folder = ensure_data_folder()

    url = "https://bnlf-tests.s3.eu-central-1.amazonaws.com/recipes.json"
    filename = f"{data_folder}/bi_recipes.json"
    stream_download(url, filename)

    chilies_recipes = [process_recipe(recipe) for recipe in recipe_generator(filename) if process_recipe(recipe)]
    chilies_recipes = list({json.dumps(recipe): recipe for recipe in chilies_recipes}.values())  # Removing duplicates
    save_to_csv(chilies_recipes, f"{data_folder}/Chilies.csv")

    aggregated_data = aggregate_difficulty(chilies_recipes)
    save_to_csv(aggregated_data, f"{data_folder}/Results.csv")
