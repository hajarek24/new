import pytest
from unittest.mock import MagicMock
import dask.dataframe as dd
import pandas as pd
from processing.metrics_airports import compute_metrics_airports


def test_compute_metrics_airports_success():
    # Mock Dask DataFrame
    data = {'Year': [2025, 2025, 2024],
            'Month': [1, 2, 1],
            'Dest': ['ATL', 'ATL', 'LAX']}
    df = pd.DataFrame(data)
    ddf = dd.from_pandas(df, npartitions=2)

    # Call the function
    results = compute_metrics_airports(ddf)

    expected_results = [
        {'year': 2025, 'month': 1, 'airport': 'ATL', 'arrivals': 1},
        {'year': 2025, 'month': 2, 'airport': 'ATL', 'arrivals': 1},
        {'year': 2024, 'month': 1, 'airport': 'LAX', 'arrivals': 1}
    ]

    # Assertions
    assert len(results) == 3
    assert sorted(results, key=lambda d: (d['year'], d['month'], d['airport'])) == sorted(expected_results, key=lambda d: (d['year'], d['month'], d['airport']))


def test_compute_metrics_airports_missing_columns():
    # Mock Dask DataFrame with missing columns
    data = {'MissingColumn': [1, 2, 3]}
    df = pd.DataFrame(data)
    ddf = dd.from_pandas(df, npartitions=2)
    results = compute_metrics_airports(ddf)
    assert results == []


def test_compute_metrics_airports_exception():
    ddf = MagicMock()
    ddf.groupby.side_effect = Exception("Test Exception")
    # Call the function
    results = compute_metrics_airports(ddf)

    # Assertions
    assert results == []
