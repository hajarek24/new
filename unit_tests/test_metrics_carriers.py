import pytest
from unittest.mock import MagicMock
import dask.dataframe as dd
import pandas as pd
from processing.metrics_carriers import compute_metrics_carriers


def test_compute_metrics_carriers_success():
    # Mock Dask DataFrame
    data = {'UniqueCarrier': ['AA', 'AA', 'BB', 'CC'],
            'Cancelled': [0, 1, 0, 0],
            'ArrDelay': [10, 0, -5, 20]}
    df = pd.DataFrame(data)
    ddf = dd.from_pandas(df, npartitions=2)

    # Call the function
    results = compute_metrics_carriers(ddf)

    # Assertions
    assert len(results) == 3
    expected_results = [
        {'carrier': 'AA', 'total': 2, 'cancelled': 1, 'delayed': 1},
        {'carrier': 'BB', 'total': 1, 'cancelled': 0, 'delayed': 0},
        {'carrier': 'CC', 'total': 1, 'cancelled': 0, 'delayed': 1}
    ]
    assert sorted(results, key=lambda d: d['carrier']) == sorted(expected_results, key=lambda d: d['carrier'])


def test_compute_metrics_carriers_missing_columns():
    # Mock Dask DataFrame with missing columns
    data = {'MissingColumn': [1, 2, 3]}
    df = pd.DataFrame(data)
    ddf = dd.from_pandas(df, npartitions=2)
    results = compute_metrics_carriers(ddf)
    assert results == []


def test_compute_metrics_carriers_no_delays_or_cancellations():
    data = {'UniqueCarrier': ['AA', 'BB'], 'Cancelled': [0, 0], 'ArrDelay': [0, 0]}
    df = pd.DataFrame(data)
    ddf = dd.from_pandas(df, npartitions=2)
    results = compute_metrics_carriers(ddf)
    assert len(results) == 2
    assert results[0]['cancelled'] == 0
    assert results[0]['delayed'] == 0


def test_compute_metrics_carriers_exception():
    ddf = MagicMock()
    ddf.merge.side_effect = Exception("Test Exception")
    results = compute_metrics_carriers(ddf)
    # Assertions
    assert results == []
