import pytest
from unittest.mock import MagicMock
import dask.dataframe as dd
import pandas as pd
from processing.metrics_routes import compute_metrics_routes


def test_compute_metrics_routes_success():
    # Mock Dask DataFrame
    data = {'Origin': ['ATL', 'LAX', 'ATL', 'JFK'], 'Dest': ['LAX', 'SFO', 'JFK', 'LAX']}
    df = pd.DataFrame(data)
    ddf = dd.from_pandas(df, npartitions=2)

    # Call the function
    results = compute_metrics_routes(ddf)

    # Assertions
    assert len(results) == 4
    assert {'origin': 'ATL', 'dest': 'LAX', 'flights': 1} in results
    assert {'origin': 'LAX', 'dest': 'SFO', 'flights': 1} in results
    assert {'origin': 'ATL', 'dest': 'JFK', 'flights': 1} in results
    assert {'origin': 'JFK', 'dest': 'LAX', 'flights': 1} in results



def test_compute_metrics_routes_missing_columns():
    # Mock Dask DataFrame with missing columns
    data = {'Missing': [1, 2, 3]}
    df = pd.DataFrame(data)
    ddf = dd.from_pandas(df, npartitions=2)

    # Call the function
    results = compute_metrics_routes(ddf)

    # Assertions
    assert results == []


def test_compute_metrics_routes_exception():
    # Mock Dask DataFrame that raises an exception
    ddf = MagicMock()
    ddf.groupby.side_effect = Exception("Test Exception")

    results = compute_metrics_routes(ddf)

    # Assertions
    assert results == []

"""
This file contains pytest unit tests for the compute_metrics_routes function in processing/metrics_routes.py.
It tests the function's ability to correctly compute route metrics from a Dask DataFrame.
"""
