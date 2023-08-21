import unittest
from unittest.mock import patch, mock_open, MagicMock
from src.etl_pipeline import stream_download, recipe_generator, process_recipe, save_to_csv, aggregate_difficulty


class TestRecipeProcessing(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    @patch('builtins.open', new_callable=mock_open)
    @patch('requests.get')
    def test_stream_download_simplified(self, mock_get, mock_open_instance):
        # Mock the streaming response from requests.get
        mock_response = MagicMock()
        mock_response.iter_content.return_value = iter([b'chunk1', b'chunk2'])  # Use iter to simulate streaming
        mock_get.return_value = mock_response

        stream_download('http://example.com', 'testfile.csv')

        # Ensure the requests.get method was called with the correct URL and streaming enabled
        mock_get.assert_called_once_with('http://example.com', stream=True)
        mock_open_instance.assert_called_once_with('testfile.csv', 'wb')

    def test_recipe_generator(self):
        data = '{"key1": "value1"}\n{"key2": "value2"}'
        m = mock_open(read_data=data)

        with patch('builtins.open', m):
            recipes = list(recipe_generator('testfile.csv'))

        self.assertEqual(recipes, [{"key1": "value1"}, {"key2": "value2"}])

    def test_process_recipe(self):
        recipe = {
            "ingredients": "chili, salt, pepper",
            "prepTime": "PT10M",
            "cookTime": "PT20M"
        }
        processed = process_recipe(recipe)
        self.assertEqual(processed["difficulty"], "Medium")
        self.assertEqual(processed["total_time"], 30)

    @patch('csv.DictWriter')
    def test_save_to_csv(self, mock_writer):
        data = [{"key": "value"}]
        save_to_csv(data, "testfile.csv")
        # Check if the header and rows were written
        mock_writer().writeheader.assert_called_once()
        mock_writer().writerow.assert_called_once_with(data[0])

    def test_aggregate_difficulty(self):
        data = [
            {"difficulty": "Easy", "total_time": 10},
            {"difficulty": "Easy", "total_time": 20},
            {"difficulty": "Medium", "total_time": 45},
            {"difficulty": "Hard", "total_time": 80}
        ]
        aggregated = aggregate_difficulty(data)
        self.assertEqual(aggregated[0]["AverageTotalTime"], 15)  # Easy
        self.assertEqual(aggregated[1]["AverageTotalTime"], 45)  # Medium
        self.assertEqual(aggregated[2]["AverageTotalTime"], 80)  # Hard


if __name__ == '__main__':
    unittest.main()
