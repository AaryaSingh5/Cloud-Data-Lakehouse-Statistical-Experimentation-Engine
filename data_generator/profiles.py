"""Fault profiles: per fault type, the chance a batch gets it and (row-level) the fraction of rows hit."""

ROW_FAULTS = ["duplicate_exact", "duplicate_key_conflict", "null_values", "out_of_range", "corrupt_rows"]
SCHEMA_FAULTS = ["column_rename", "column_dropped", "extra_column", "type_change", "timestamp_format_change"]
BATCH_FAULTS = ["empty_file", "late_arriving"]
ALL_FAULTS = ROW_FAULTS + SCHEMA_FAULTS + BATCH_FAULTS


def _profile(row_p, row_rate, schema_p, empty_p, late_p):
    prof = {f: {"batch_prob": row_p, "row_rate": row_rate} for f in ROW_FAULTS}
    prof.update({f: {"batch_prob": schema_p, "row_rate": None} for f in SCHEMA_FAULTS})
    prof["empty_file"] = {"batch_prob": empty_p, "row_rate": None}
    prof["late_arriving"] = {"batch_prob": late_p, "row_rate": None}
    return prof


PROFILES = {
    "none": {},
    "low": _profile(0.05, 0.01, 0.01, 0.01, 0.03),
    "medium": _profile(0.12, 0.03, 0.025, 0.02, 0.06),
    "high": _profile(0.25, 0.08, 0.05, 0.04, 0.12),
}
