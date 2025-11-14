import pytest
from unittest.mock import MagicMock
import dask.dataframe as dd
import pandas as pd
from processing.metrics_globales import compute_metrics_global


def test_compute_metrics_global_success():
    # Mock Dask DataFrame
    data = {'Cancelled': [0, 1, 0, 0, 1], 'ArrDelay': [0, 10, -5, 5, 0]}
    df = pd.DataFrame(data)
    ddf = dd.from_pandas(df, npartitions=2)

    # Call the function
    results = compute_metrics_global(ddf)

    # Assertions
    assert results['total_flights'] == 5
    assert results['cancelled'] == 2
    assert results['delayed'] == 2
    assert isinstance(results['total_flights'], int)
    assert isinstance(results['cancelled'], int)
    assert isinstance(results['delayed'], int)


def test_compute_metrics_global_exception():
    # Mock Dask DataFrame that raises an exception
    ddf = MagicMock()
    ddf.shape = MagicMock(side_effect=Exception("Test Exception"))

    # Call the function
    results = compute_metrics_global(ddf)
    assert results['total_flights'] == 0


"""
This file contains pytest unit tests for the compute_metrics_global function in processing/metrics_globales.py.
It tests the function's ability to correctly compute global metrics from a Dask DataFrame.
"""

