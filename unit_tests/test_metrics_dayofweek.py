import pytest
from unittest.mock import MagicMock
import dask.dataframe as dd
import pandas as pd
from processing.metrics_dayofweek import compute_metrics_dayofweek


def test_compute_metrics_dayofweek_success():
    # Mock Dask DataFrame
    data = {'DayOfWeek': [1, 2, 3, 1, 2, 4, 5, 6, 7]}
    df = pd.DataFrame(data)
    ddf = dd.from_pandas(df, npartitions=2)

    # Call the function
    results = compute_metrics_dayofweek(ddf)

    # Assertions
    expected_results = {1: 2, 2: 2, 3: 1, 4: 1, 5: 1, 6: 1, 7: 1}
    assert results == expected_results


def test_compute_metrics_dayofweek_missing_column():
    # Mock Dask DataFrame with missing 'DayOfWeek' column
    data = {'MissingColumn': [1, 2, 3]}
    df = pd.DataFrame(data)
    ddf = dd.from_pandas(df, npartitions=2)

    # Call the function
    results = compute_metrics_dayofweek(ddf)

    # Assertions
    expected_results = {i: 0 for i in range(1, 8)}
    assert results == expected_results


def test_compute_metrics_dayofweek_exception():
    ddf = MagicMock()
    ddf.__getitem__.side_effect = Exception("Test Exception")
    results = compute_metrics_dayofweek(ddf)
    assert results == {i: 0 for i in range(1, 8)}

"""
This file contains pytest unit tests for the compute_metrics_dayofweek function in processing/metrics_dayofweek.py.
It tests the function's ability to correctly compute day of week metrics from a Dask DataFrame.
"""
