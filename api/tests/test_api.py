import pytest
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch

from api.main import app, compute_risk_score, AccountFeatures

client = TestClient(app)


SAMPLE_FEAT = AccountFeatures(
    account_id         = "ACC000001",
    txn_count          = 120,
    total_amount       = 15_000.0,
    avg_amount         = 125.0,
    max_txn_count_24h  = 30,
    spike_count        = 5,
    distinct_merchants = 12,
    distinct_cities    = 3,
    latest_date        = "2024-06-15",
)


def make_mock_engine(feat_row=None, dev_row=None, device_rows=None):
    """Return a mock engine that returns given rows."""
    engine_mock = MagicMock()
    conn_mock   = MagicMock()
    engine_mock.connect.return_value.__enter__ = lambda s: conn_mock
    engine_mock.connect.return_value.__exit__  = MagicMock(return_value=False)

    def execute_side_effect(stmt, params=None):
        sql = str(stmt)
        result = MagicMock()
        if "account_features" in sql and feat_row is not None:
            result.fetchone.return_value = feat_row
        elif "device_features" in sql and dev_row is not None:
            result.fetchone.return_value = dev_row
        elif "transactions" in sql and device_rows is not None:
            if "GROUP BY account_id" in sql:
                result.fetchall.return_value = device_rows
            else:
                result.fetchone.return_value = MagicMock(shared=1)
        else:
            result.fetchone.return_value  = None
            result.fetchall.return_value  = []
        return result

    conn_mock.execute.side_effect = execute_side_effect
    return engine_mock



class TestComputeRiskScore:
    def test_low_risk_clean_account(self):
        feat = AccountFeatures(
            account_id="ACC000001", txn_count=10, total_amount=500.0,
            avg_amount=50.0, max_txn_count_24h=2, spike_count=0,
            distinct_merchants=3, distinct_cities=1, latest_date="2024-01-01",
        )
        score, label, factors = compute_risk_score(feat, device_shared=1)
        assert label == "LOW"
        assert score < 0.3
        assert factors == []

    def test_high_risk_many_spikes(self):
        feat = AccountFeatures(
            account_id="ACC000002", txn_count=100, total_amount=50_000.0,
            avg_amount=500.0, max_txn_count_24h=80, spike_count=50,
            distinct_merchants=20, distinct_cities=8, latest_date="2024-01-01",
        )
        score, label, factors = compute_risk_score(feat, device_shared=5)
        assert label == "HIGH"
        assert score >= 0.6
        assert len(factors) >= 2

    def test_medium_risk_device_sharing(self):
        feat = AccountFeatures(
            account_id="ACC000003", txn_count=20, total_amount=1000.0,
            avg_amount=50.0, max_txn_count_24h=5, spike_count=1,
            distinct_merchants=3, distinct_cities=1, latest_date="2024-01-01",
        )
        score, label, factors = compute_risk_score(feat, device_shared=3)
        # sharing 3 accounts → some medium risk
        assert score > 0.0
        assert any("shared" in f.lower() for f in factors)

    def test_score_bounded(self):
        feat = AccountFeatures(
            account_id="ACC999999", txn_count=1, total_amount=1_000_000.0,
            avg_amount=1_000_000.0, max_txn_count_24h=999, spike_count=999,
            distinct_merchants=100, distinct_cities=99, latest_date="2024-01-01",
        )
        score, _, _ = compute_risk_score(feat, device_shared=100)
        assert 0.0 <= score <= 1.0



class TestHealthEndpoint:
    def test_health_returns_200(self):
        import api.main as main_module
        mock_engine = MagicMock()
        conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = lambda s: conn
        mock_engine.connect.return_value.__exit__  = MagicMock(return_value=False)
        with patch.object(main_module, "engine", mock_engine):
            resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert "version" in data


class TestRiskEndpoint:
    def _make_feat_row(self):
        row = MagicMock()
        row.account_id         = "ACC000001"
        row.date               = "2024-06-15"
        row.txn_count          = 120
        row.total_amount       = 15_000.0
        row.avg_amount         = 125.0
        row.max_txn_count_24h  = 30
        row.spike_count        = 5
        row.distinct_merchants = 12
        row.distinct_cities    = 3
        return row

    def test_valid_account_returns_200(self):
        import api.main as m
        row = self._make_feat_row()
        dev = MagicMock(); dev.shared = 1
        with patch.object(m, "engine", make_mock_engine(feat_row=row, dev_row=dev)):
            resp = client.get("/risk/ACC000001")
        assert resp.status_code == 200
        data = resp.json()
        assert data["account_id"] == "ACC000001"
        assert "risk_score" in data
        assert data["risk_label"] in ("LOW", "MEDIUM", "HIGH")
        assert 0.0 <= data["risk_score"] <= 1.0

    def test_invalid_account_id_format(self):
        resp = client.get("/risk/BADID")
        assert resp.status_code == 422

    def test_account_not_found(self):
        import api.main as m
        with patch.object(m, "engine", make_mock_engine(feat_row=None)):
            resp = client.get("/risk/ACC999999")
        assert resp.status_code == 404

    def test_response_has_latency(self):
        import api.main as m
        row = self._make_feat_row()
        dev = MagicMock(); dev.shared = 2
        with patch.object(m, "engine", make_mock_engine(feat_row=row, dev_row=dev)):
            resp = client.get("/risk/ACC000001")
        assert resp.status_code == 200
        assert resp.json()["latency_ms"] >= 0

    def test_date_filter_param(self):
        import api.main as m
        row = self._make_feat_row()
        dev = MagicMock(); dev.shared = 1
        with patch.object(m, "engine", make_mock_engine(feat_row=row, dev_row=dev)):
            resp = client.get("/risk/ACC000001?date=2024-06-15")
        assert resp.status_code == 200


class TestDeviceEndpoint:
    def _make_dev_row(self):
        row = MagicMock()
        row.distinct_accounts  = 3
        row.multi_account_flag = 1
        return row

    def _make_linked_rows(self):
        r1 = MagicMock()
        r1.account_id   = "ACC000001"
        r1.device_id    = "DEV000001"
        r1.txn_count    = 50
        r1.total_amount = 5000.0
        r2 = MagicMock()
        r2.account_id   = "ACC000002"
        r2.device_id    = "DEV000001"
        r2.txn_count    = 30
        r2.total_amount = 3000.0
        return [r1, r2]

    def test_valid_device_returns_200(self):
        import api.main as m
        dev = self._make_dev_row()
        linked = self._make_linked_rows()
        with patch.object(m, "engine", make_mock_engine(dev_row=dev, device_rows=linked)):
            resp = client.get("/device/DEV000001")
        assert resp.status_code == 200
        data = resp.json()
        assert data["device_id"] == "DEV000001"
        assert isinstance(data["linked_accounts"], list)
        assert data["multi_account_flag"] is True

    def test_invalid_device_id_format(self):
        resp = client.get("/device/BADDEVICE")
        assert resp.status_code == 422

    def test_device_not_found(self):
        import api.main as m
        with patch.object(m, "engine", make_mock_engine(dev_row=None)):
            resp = client.get("/device/DEV999999")
        assert resp.status_code == 404

    def test_limit_param(self):
        import api.main as m
        dev = self._make_dev_row()
        linked = self._make_linked_rows()
        with patch.object(m, "engine", make_mock_engine(dev_row=dev, device_rows=linked)):
            resp = client.get("/device/DEV000001?limit=10")
        assert resp.status_code == 200

    def test_limit_out_of_range(self):
        resp = client.get("/device/DEV000001?limit=999")
        assert resp.status_code == 422
