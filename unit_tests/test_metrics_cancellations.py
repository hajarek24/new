import pytest
from unittest.mock import MagicMock
import dask.dataframe as dd
import pandas as pd
from processing.metrics_cancellations import compute_metrics_cancellations


def test_compute_metrics_cancellations_success():
    # Mock Dask DataFrame
    data = {'Cancelled': [1, 1, 0, 1], 'CancellationCode': ['A', 'B', 'C', 'A']}
    df = pd.DataFrame(data)
    ddf = dd.from_pandas(df, npartitions=2)

    # Call the function
    results = compute_metrics_cancellations(ddf)

    # Assertions
    assert results['A'] == 2
    assert results['B'] == 1
    assert results['C'] == 0
    assert results['D'] == 0


def test_compute_metrics_cancellations_no_cancellations():
    # Mock Dask DataFrame with no cancelled flights
    data = {'Cancelled': [0, 0, 0], 'CancellationCode': ['A', 'B', 'C']}
    df = pd.DataFrame(data)
    ddf = dd.from_pandas(df, npartitions=2)
    results = compute_metrics_cancellations(ddf)
    assert results['A'] == 0


def test_compute_metrics_cancellations_exception():
    ddf = MagicMock()
    ddf.__getitem__.side_effect = Exception("Test Exception")
    results = compute_metrics_cancellations(ddf)
    # Assertions
    assert results['A'] == 0
