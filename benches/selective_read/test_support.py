"""Synthetic observations shared by the Python oracle integration checks."""

def observation(value, reuse=False, revision=2):
    count = (2 if revision == 4 else 10) if reuse else 1
    return {"open_query_ns": None if reuse else value, "initialization_ns": 5 if reuse else None,
            "initialization_plus_query1_ns": value + 5 if reuse else None,
            "initialization_plus_all_queries_ns": count * value + 5 if reuse else None,
            "session_elapsed_ns": count * value + (5 if reuse else 0), "cleanup_ns": 1,
            "external_metrics": {key: 1 for key in ("process_user_cpu_ns", "process_system_cpu_ns", "process_cpu_ns", "peak_rss_bytes", "process_elapsed_ns")},
            "queries": [{"query_index": index, "output_rows": 3, "output_batches": 1,
                         "completion_ns": value, "first_batch_ns": 1, "first_batch_unavailable_reason": None} for index in range(count)]}

