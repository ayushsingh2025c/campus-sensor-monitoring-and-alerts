def rule_payload(**overrides):
    return {
        "name": "High temperature",
        "sensorId": "sensor-rule-test",
        "type": "temperature",
        "operator": ">=",
        "threshold": 30,
        "enabled": True,
        **overrides,
    }


def test_create_rule_with_enabled_status_and_operator(client, cleanup_rows):
    response = client.post("/api/v1/rules", json=rule_payload(enabled=False))

    assert response.status_code == 201
    rule = response.json()
    assert rule["operator"] == ">="
    assert rule["enabled"] is False
    assert rule["sensorId"] == "sensor-rule-test"
    cleanup_rows(rule=rule["id"])


def test_list_rules_filters_by_enabled_and_metric(client, cleanup_rows):
    created = client.post("/api/v1/rules", json=rule_payload(enabled=False))
    rule_id = created.json()["id"]
    cleanup_rows(rule=rule_id)

    response = client.get(
        "/api/v1/rules",
        params={"enabled": "false", "sensorId": "sensor-rule-test", "type": "temperature"},
    )

    assert response.status_code == 200
    assert any(rule["id"] == rule_id for rule in response.json())
    assert all(not rule["enabled"] for rule in response.json())


def test_rule_rejects_invalid_operator_and_window_definition(client):
    invalid_operator = client.post("/api/v1/rules", json=rule_payload(operator="~="))
    missing_aggregation = client.post(
        "/api/v1/rules",
        json=rule_payload(windowSeconds=60),
    )
    aggregation_without_window = client.post(
        "/api/v1/rules",
        json=rule_payload(aggregation="average"),
    )

    assert invalid_operator.status_code == 422
    assert missing_aggregation.status_code == 422
    assert aggregation_without_window.status_code == 422


def test_rule_list_pagination_is_bounded(client):
    assert client.get("/api/v1/rules", params={"limit": 201}).status_code == 422
    assert client.get("/api/v1/rules", params={"offset": -1}).status_code == 422
