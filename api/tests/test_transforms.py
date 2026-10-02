import pytest
import pandas as pd

from spark.jobs.pandas_transform import clean, account_features, device_features
from data_quality.checks import run_checks, DataQualityError


def make_df(n=10, inject_bad=False) -> pd.DataFrame:
    import uuid
    from datetime import datetime, timedelta
    rows = []
    for i in range(n):
        ts = datetime(2024, 1, 1) + timedelta(hours=i * 2)
        rows.append({
            "transaction_id": str(uuid.uuid4()),
            "account_id":     f"ACC{str(i % 3).zfill(6)}",
            "device_id":      f"DEV{str(i % 2).zfill(6)}",
            "amount":         str(round(50 + i * 10.5, 2)),
            "merchant":       f"MER{str(i % 5).zfill(5)}",
            "timestamp":      ts.strftime("%Y-%m-%d %H:%M:%S"),
            "city":           "New York",
        })
    if inject_bad:
        rows.append({**rows[0], "transaction_id": "null-acc", "account_id": ""})
        rows.append({**rows[0], "transaction_id": "neg-amt",  "amount": "-10.0"})
        rows.append({**rows[0], "transaction_id": "bad-type", "amount": "NaN_str"})
        rows.append(dict(rows[0]))  # duplicate transaction_id
    return pd.DataFrame(rows)


class TestClean:
    def test_returns_two_frames(self):
        c, q = clean(make_df(20))
        assert isinstance(c, pd.DataFrame)
        assert isinstance(q, pd.DataFrame)

    def test_clean_rows_pass_through(self):
        c, q = clean(make_df(20))
        assert len(c) == 20
        assert len(q) == 0

    def test_null_account_quarantined(self):
        _, q = clean(make_df(5, inject_bad=True))
        assert any(q["transaction_id"] == "null-acc")

    def test_negative_amount_quarantined(self):
        _, q = clean(make_df(5, inject_bad=True))
        assert any(q["transaction_id"] == "neg-amt")

    def test_bad_type_quarantined(self):
        _, q = clean(make_df(5, inject_bad=True))
        assert any(q["transaction_id"] == "bad-type")

    def test_duplicate_quarantined(self):
        _, q = clean(make_df(5, inject_bad=True))
        assert len(q[q["quarantine_reason"] == "duplicate"]) >= 1

    def test_amount_is_numeric_after_clean(self):
        c, _ = clean(make_df(10))
        assert pd.api.types.is_float_dtype(c["amount"])

    def test_timestamp_parsed(self):
        c, _ = clean(make_df(10))
        assert pd.api.types.is_datetime64_any_dtype(c["timestamp"])


class TestAccountFeatures:
    def _clean(self):
        c, _ = clean(make_df(30))
        return c

    def test_output_has_required_columns(self):
        feat = account_features(self._clean())
        for col in ("account_id", "date", "txn_count", "total_amount",
                    "avg_amount", "max_txn_count_24h", "spike_count"):
            assert col in feat.columns

    def test_txn_count_positive(self):
        assert (account_features(self._clean())["txn_count"] > 0).all()

    def test_avg_amount_between_min_max(self):
        df = self._clean()
        feat = account_features(df)
        merged = feat.merge(
            df.groupby("account_id")["amount"].agg(["min", "max"]),
            on="account_id",
        )
        assert (merged["avg_amount"] >= merged["min"]).all()
        assert (merged["avg_amount"] <= merged["max"]).all()

    def test_spike_count_non_negative(self):
        assert (account_features(self._clean())["spike_count"] >= 0).all()


class TestDeviceFeatures:
    def _clean(self):
        c, _ = clean(make_df(30))
        return c

    def test_output_has_required_columns(self):
        feat = device_features(self._clean())
        for col in ("device_id", "date", "distinct_accounts", "txn_count", "multi_account_flag"):
            assert col in feat.columns

    def test_multi_account_flag(self):
        feat = device_features(self._clean())
        multi = feat[feat["distinct_accounts"] > 1]
        assert (multi["multi_account_flag"] == 1).all()

    def test_single_account_devices_no_flag(self):
        feat = device_features(self._clean())
        single = feat[feat["distinct_accounts"] == 1]
        assert (single["multi_account_flag"] == 0).all()


class TestDataQualityChecks:
    def test_clean_data_passes(self, tmp_path):
        report = run_checks(make_df(100), str(tmp_path))
        assert report.passed
        assert report.null_rows == 0
        assert report.duplicate_rows == 0
        assert report.negative_amount == 0

    def test_high_null_rate_fails(self, tmp_path):
        import uuid
        df = make_df(100)
        # add 5% nulls — above the 1% threshold
        for _ in range(5):
            df.loc[len(df)] = {
                "transaction_id": str(uuid.uuid4()),
                "account_id":     "",
                "device_id":      "DEV000001",
                "amount":         "50",
                "merchant":       "MER00001",
                "timestamp":      "2024-01-01 00:00:00",
                "city":           "New York",
            }
        with pytest.raises(DataQualityError, match="Null rate"):
            run_checks(df, str(tmp_path))

    def test_quarantine_file_written(self, tmp_path):
        try:
            run_checks(make_df(20, inject_bad=True), str(tmp_path))
        except DataQualityError:
            pass
        assert len(list(tmp_path.glob("quarantine_*.csv"))) > 0

    def test_row_count_drop_detected(self, tmp_path):
        with pytest.raises(DataQualityError, match="Row count dropped"):
            run_checks(make_df(50), str(tmp_path), previous_row_count=1000)
