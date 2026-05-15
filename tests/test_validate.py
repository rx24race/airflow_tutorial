import pytest


def test_validate_records_returns_valid_input(dag_module):
    records = [{"id": "bitcoin", "symbol": "btc", "current_price": 100_000}]

    assert dag_module.validate_records(records) == records


def test_validate_records_rejects_empty_input(dag_module):
    with pytest.raises(ValueError, match="No data extracted from API"):
        dag_module.validate_records([])


def test_validate_records_rejects_missing_required_keys(dag_module):
    with pytest.raises(ValueError, match="Missing required keys"):
        dag_module.validate_records([{"id": "bitcoin", "symbol": "btc"}])
