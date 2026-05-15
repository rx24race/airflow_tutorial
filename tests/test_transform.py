import json


def make_record(**overrides):
    record = {
        "id": "bitcoin",
        "symbol": "btc",
        "name": "Bitcoin",
        "current_price": 100_000.0,
        "market_cap": 150_000_000_000,
        "last_updated": "2026-05-15T01:02:03.000Z",
        "price_change_percentage_24h": 6.0,
    }
    record.update(overrides)
    return record


def test_transform_records_normalizes_and_enriches_data(dag_module):
    result = dag_module.transform_records([make_record()])

    assert len(result) == 1
    assert result[0]["symbol"] == "BTC"
    assert result[0]["last_updated"] == "2026-05-15T01:02:03Z"
    assert result[0]["is_high_value"] is True
    assert result[0]["trend"] == "Strong Bullish"
    assert "load_timestamp" in result[0]
    json.dumps(result)


def test_transform_records_classifies_trends(dag_module):
    changes = [6, 1, 0, -1, -6]
    result = dag_module.transform_records(
        [make_record(id=f"coin-{idx}", price_change_percentage_24h=change) for idx, change in enumerate(changes)]
    )

    assert [record["trend"] for record in result] == [
        "Strong Bullish",
        "Bullish",
        "Neutral",
        "Bearish",
        "Strong Bearish",
    ]


def test_transform_records_converts_missing_values_to_none(dag_module):
    result = dag_module.transform_records([make_record(price_change_percentage_24h=float("nan"))])

    assert result[0]["price_change_percentage_24h"] is None
