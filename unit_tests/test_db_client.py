import pytest
from unittest.mock import MagicMock
from storage.db_client import MongoDBClient


class TestMongoDBClient:
    @pytest.fixture
    def mock_mongodb_client(self):
        client = MongoDBClient()
        client.client = MagicMock()
        client.db = MagicMock()
        return client

    def test_update_stats_global_success(self, mock_mongodb_client):
        mock_mongodb_client.update_stats_global(100, 10, 20)
        mock_mongodb_client.db["stats_global"].update_one.assert_called_once()

    def test_update_stats_global_no_db(self, mock_mongodb_client):
        mock_mongodb_client.db = None
        mock_mongodb_client.update_stats_global(100, 10, 20)
        mock_mongodb_client.db["stats_global"].update_one.assert_not_called()

    def test_update_cancellations_success(self, mock_mongodb_client):
        cancellations_dict = {"A": 5, "B": 3}
        mock_mongodb_client.update_cancellations(cancellations_dict)
        assert mock_mongodb_client.db["cancellations"].update_one.call_count == 2

    def test_update_cancellations_no_db(self, mock_mongodb_client):
        mock_mongodb_client.db = None
        cancellations_dict = {"A": 5, "B": 3}
        mock_mongodb_client.update_cancellations(cancellations_dict)
        mock_mongodb_client.db["cancellations"].update_one.assert_not_called()

    def test_update_day_of_week_success(self, mock_mongodb_client):
        day_counts = {1: 10, 2: 5}
        mock_mongodb_client.update_day_of_week(day_counts)
        assert mock_mongodb_client.db["day_of_week"].update_one.call_count == 2

    def test_update_day_of_week_no_db(self, mock_mongodb_client):
        mock_mongodb_client.db = None
        day_counts = {1: 10, 2: 5}
        mock_mongodb_client.update_day_of_week(day_counts)
        mock_mongodb_client.db["day_of_week"].update_one.assert_not_called()

    def test_update_routes_success(self, mock_mongodb_client):
        routes_list = [{"origin": "ATL", "dest": "LAX", "flights": 100}]
        mock_mongodb_client.update_routes(routes_list)
        mock_mongodb_client.db["routes"].update_one.assert_called_once()

    def test_update_routes_no_db(self, mock_mongodb_client):
        mock_mongodb_client.db = None
        routes_list = [{"origin": "ATL", "dest": "LAX", "flights": 100}]
        mock_mongodb_client.update_routes(routes_list)
        mock_mongodb_client.db["routes"].update_one.assert_not_called()

    def test_update_carriers_success(self, mock_mongodb_client):
        carriers_stats = [{"carrier": "AA", "total": 1000, "cancelled": 50, "delayed": 200}]
        mock_mongodb_client.update_carriers(carriers_stats)
        mock_mongodb_client.db["carriers"].update_one.assert_called_once()

    def test_update_carriers_no_db(self, mock_mongodb_client):
        mock_mongodb_client.db = None
        carriers_stats = [{"carrier": "AA", "total": 1000, "cancelled": 50, "delayed": 200}]
        mock_mongodb_client.update_carriers(carriers_stats)
        mock_mongodb_client.db["carriers"].update_one.assert_not_called()

    def test_update_airports_monthly_success(self, mock_mongodb_client):
        airports_list = [{"year": 2023, "month": 1, "airport": "ATL", "arrivals": 5000}]
        mock_mongodb_client.update_airports_monthly(airports_list)
        mock_mongodb_client.db["airports_monthly"].update_one.assert_called_once()

    def test_update_airports_monthly_no_db(self, mock_mongodb_client):
        mock_mongodb_client.db = None
        airports_list = [{"year": 2023, "month": 1, "airport": "ATL", "arrivals": 5000}]
        mock_mongodb_client.update_airports_monthly(airports_list)
        mock_mongodb_client.db["airports_monthly"].update_one.assert_not_called()

    def test_close_success(self, mock_mongodb_client):
        mock_mongodb_client.close()
        mock_mongodb_client.client.close.assert_called_once()

    def test_close_no_client(self, mock_mongodb_client):
        mock_mongodb_client.client = None
        mock_mongodb_client.close()
        if mock_mongodb_client.client:
            mock_mongodb_client.client.close.assert_not_called()

    def test_mongodb_client_connection_failure(self, monkeypatch):
        # Mock MongoClient to raise an exception during initialization
        def mock_mongo_client(*args, **kwargs):
            raise Exception("Failed to connect to MongoDB")

        monkeypatch.setattr("storage.db_client.MongoClient", mock_mongo_client)

        # Create MongoDBClient instance, which should now fail to connect
        client = MongoDBClient()

        # Assert that client and db are None
        assert client.client is None
        assert client.db is None


# To run this test file, navigate to the directory containing it and run: `pytest test_db_client.py`
# Or simply run `pytest` in the root directory to discover and run all test files.

