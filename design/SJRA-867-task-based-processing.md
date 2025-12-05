# SJRA-867: Task-based Processing

https://d3b.atlassian.net/browse/SJRA-867

---
## 1. Overview and decision summary


### 1.1 Overview

This design document outlines the changes in implementation from case-based processing to task-based processing.
It also explores potential changes to the Occurrence table schema by removing Exomiser-related columns.

### 1.2 Decision Summary

- The ETL will be adapted to use `task_id` instead of `case_id` for data imports.
- Exomiser columns will be retained in the Occurrence table to maintain query performance.

---
## 2. Problem Statement

How to adapt the ETL to the data model V2 ?

---
## 3. Context and Background

With the changes to the data model V2, the case-based approach is no longer appropriate. 

The case-based approach forced a single task per case, which is no longer valid as multiple tasks can exist per case.

Currently, the Case model defines how data is imported into StarRocks by the ETL. It determines the priority and which files should be grouped together during the import process.

All of this can be represented by the Task model, by creating data model representations of Tasks in the ETL codebase.

---
## 4. Proposed Design

- Replace the concept of importing a Case to importing a Task.
- Replace references to `case_id` from the ETL, use `task_id` instead.

## 4.1 Benchmarks

The following benchmarks were implemented in order to measure the performance of queries with and without Exomiser columns when
fetching Occurrence data for a specific `locus_id`. 

This scenario attempts to simulate a real-world use-case of the "Expand Occurrence" UI component, in which we fetch Occurrence data for exposing it in the UI for a specific Occurrence (`locus_id`).

This test was implemented using a test sample data of 2 cases, 1 trio and 1 solo. (~1M Occurrence rows, ~2.7K Exomiser rows).

Each test simulated 5 concurrent processes executing as fast a possible `SELECT ...` statements on occurrence using randomized `locus_id` from the original dataset.


### 4.1.1 Exomiser columns in Occurrence

```
Average QPS per process: 107.73
Estimated aggregate QPS: 538.63

Detailed per-process statistics:

+--------------+-----------------+--------+------------+------------+------------+------------+
|   Process ID |   Total Queries |    QPS |   P50 (ms) |   P90 (ms) |   P95 (ms) |   P99 (ms) |
+==============+=================+========+============+============+============+============+
|            1 |            6448 | 107.4  |       5.98 |      12.39 |      37    |      53.84 |
+--------------+-----------------+--------+------------+------------+------------+------------+
|            2 |            6472 | 107.8  |       5.98 |      12.01 |      37.28 |      53.61 |
+--------------+-----------------+--------+------------+------------+------------+------------+
|            3 |            6474 | 107.83 |       5.97 |      12.52 |      37.1  |      53.61 |
+--------------+-----------------+--------+------------+------------+------------+------------+
|            4 |            6475 | 107.85 |       5.96 |      12.23 |      37.14 |      54.09 |
+--------------+-----------------+--------+------------+------------+------------+------------+
|            5 |            6468 | 107.74 |       5.97 |      12.48 |      37.17 |      53.32 |
+--------------+-----------------+--------+------------+------------+------------+------------+
```

### 4.1.2 Exomiser columns not in Occurrence

```
Average QPS per process: 66.59
Estimated aggregate QPS: 332.97

Detailed per-process statistics:

+--------------+-----------------+-------+------------+------------+------------+------------+
|   Process ID |   Total Queries |   QPS |   P50 (ms) |   P90 (ms) |   P95 (ms) |   P99 (ms) |
+==============+=================+=======+============+============+============+============+
|            1 |            3995 | 66.55 |       9.45 |      40.2  |      43.81 |      65.96 |
+--------------+-----------------+-------+------------+------------+------------+------------+
|            2 |            3995 | 66.55 |       9.45 |      39.91 |      43.99 |      69.15 |
+--------------+-----------------+-------+------------+------------+------------+------------+
|            3 |            3999 | 66.62 |       9.45 |      40.22 |      43.95 |      66.65 |
+--------------+-----------------+-------+------------+------------+------------+------------+
|            4 |            3996 | 66.57 |       9.43 |      39.76 |      43.93 |      67    |
+--------------+-----------------+-------+------------+------------+------------+------------+
|            5 |            4002 | 66.67 |       9.42 |      39.86 |      44.09 |      65.71 |
+--------------+-----------------+-------+------------+------------+------------+------------+
```

`EXPLAIN ANALYZE` output for a single query with joins:

```
Explain String                                                                                                                                                      |
--------------------------------------------------------------------------------------------------------------------------------------------------------------------+
 [0mSummary [0m                                                                                                                                                     |
     [0mQueryId: 226a0c1f-cb9b-11f0-9f55-4a9c046607db [0m                                                                                                           |
     [0mVersion: 3.4.2-c15ba7c [0m                                                                                                                                  |
     [0mState: Finished [0m                                                                                                                                         |
     [0mTotalTime: 1s434ms [0m                                                                                                                                      |
         [0mExecutionTime: 1s317ms [Scan: 104.921ms (7.97%), Network: 90.399ms (6.86%), ResultDeliverTime: 643.636ms (48.87%), ScheduleTime: 374.432ms (28.43%)] [0m|
         [0mCollectProfileTime: 5ms [0m                                                                                                                             |
         [0mFrontendProfileMergeTime: 22.892ms [0m                                                                                                                  |
     [0mQueryPeakMemoryUsage: ?, QueryAllocatedMemoryUsage: 2.088 GB [0m                                                                                            |
     [0mTop Most Time-consuming Nodes: [0m                                                                                                                          |
         [1m [31m1. EXCHANGE (id=5) : 512.645ms (37.80%) [0m                                                                                                        |
         [1m [38;2;250;128;114m2. RESULT_SINK: 397.961ms (29.35%) [0m                                                                                               |
         [1m [38;2;250;128;114m3. HASH_JOIN (id=2) [COLOCATE, LEFT OUTER JOIN]: 250.263ms (18.45%) [0m                                                              |
         [0m4. OLAP_SCAN (id=0) : 109.058ms (8.04%) [0m                                                                                                             |
         [0m5. DECODE (id=4) : 80.217ms (5.92%) [0m                                                                                                                 |
         [0m6. PROJECT (id=3) : 4.856ms (0.36%) [0m                                                                                                                 |
         [0m7. OLAP_SCAN (id=1) : 1.074ms (0.08%) [0m                                                                                                               |
     [0mTop Most Memory-consuming Nodes: [0m                                                                                                                        |
     [0mNonDefaultVariables: [0m                                                                                                                                    |
         [0mcharacter_set_results: utf8 -> NULL [0m                                                                                                                 |
         [0menable_adaptive_sink_dop: false -> true [0m                                                                                                             |
         [0menable_async_profile: true -> false [0m                                                                                                                 |
         [0menable_profile: false -> true [0m                                                                                                                       |
         [0msql_mode_v2: 32 -> 2097184 [0m                                                                                                                          |
 [0mFragment 0 [0m                                                                                                                                                  |
│    [0mBackendNum: 1 [0m                                                                                                                                           |
│    [0mInstancePeakMemoryUsage: 62.276 MB, InstanceAllocatedMemoryUsage: 820.973 MB [0m                                                                            |
│    [0mPrepareTime: ? [0m                                                                                                                                          |
└── [1m [38;2;250;128;114mRESULT_SINK [0m                                                                                                                           |
   │    [1m [38;2;250;128;114mTotalTime: 397.961ms (29.35%) [CPUTime: 397.961ms] [0m                                                                                |
   │    [1m [38;2;250;128;114mOutputRows: 957.054K (957054) [0m                                                                                                     |
   │    [1m [38;2;250;128;114mSinkType: MYSQL_PROTOCAL [0m                                                                                                          |
   └── [1m [31mEXCHANGE (id=5)  [0m                                                                                                                                 |
           [1m [31mEstimates: [row: ?, cpu: ?, memory: ?, network: ?, cost: ?] [0m                                                                                  |
           [1m [31mTotalTime: 512.645ms (37.80%) [CPUTime: 422.245ms, NetworkTime: 90.399ms] [0m                                                                    |
           [1m [31mOutputRows: 957.054K (957054) [0m                                                                                                                |
           [1m [31mPeakMemory: ?, AllocatedMemory: ? [0m                                                                                                            |
           [1m [31mDetail Timers:  [0m                                                                                                                              |
               [1m [31mClosureBlockTime: 7s908ms [min=5s615ms, max=12s99ms] [0m                                                                                     |
               [1m [31mOverallTime: 1s190ms [0m                                                                                                                     |
               [1m [31mSerializeChunkTime: 77.229ms [min=9.654ms, max=159.561ms] [0m                                                                                |
               [1m [31mWaitTime: 868.400ms [0m                                                                                                                      |
 [0m                                                                                                                                                                |
 [0mFragment 1 [0m                                                                                                                                                  |
│    [0mBackendNum: 1 [0m                                                                                                                                           |
│    [0mInstancePeakMemoryUsage: 280.384 MB, InstanceAllocatedMemoryUsage: 1.286 GB [0m                                                                             |
│    [0mPrepareTime: ? [0m                                                                                                                                          |
└── [0mDATA_STREAM_SINK (id=5) [0m                                                                                                                                  |
   │    [0mPartitionType: UNPARTITIONED [0m                                                                                                                         |
   └── [0mDECODE (id=4)  [0m                                                                                                                                        |
      │    [0mEstimates: [row: 956349, cpu: 0.00, memory: 0.00, network: 0.00, cost: 456675697.61] [0m                                                              |
      │    [0mTotalTime: 80.217ms (5.92%) [CPUTime: 80.217ms] [0m                                                                                                   |
      │    [0mOutputRows: 957.054K (957054) [0m                                                                                                                     |
      │    [0mSubordinateOperators:  [0m                                                                                                                            |
      │        [0mGROUP_EXCHANGE [0m                                                                                                                                |
      │        [0mLOCAL_EXCHANGE [null] [0m                                                                                                                         |
      └── [0mPROJECT (id=3)  [0m                                                                                                                                    |
         │    [0mEstimates: [row: ?, cpu: ?, memory: ?, network: ?, cost: ?] [0m                                                                                    |
         │    [0mTotalTime: 4.856ms (0.36%) [CPUTime: 4.856ms] [0m                                                                                                  |
         │    [0mOutputRows: 957.054K (957054) [0m                                                                                                                  |
         │    [0mExpression: [1: part, 2: seq_id, 3: task_id, 4: locus_id, 5: ad_ratio, 6: gq, 7: dp, ...] [0m                                                      |
         └── [1m [38;2;250;128;114mHASH_JOIN (id=2) [COLOCATE, LEFT OUTER JOIN] [0m                                                                                 |
            │    [1m [38;2;250;128;114mEstimates: [row: 956349, cpu: 619169492.64, memory: 65784.00, network: 0.00, cost: 456675697.61] [0m                         |
            │    [1m [38;2;250;128;114mTotalTime: 250.263ms (18.45%) [CPUTime: 250.263ms] [0m                                                                       |
            │    [1m [38;2;250;128;114mOutputRows: 957.054K (957054) [0m                                                                                            |
            │    [1m [38;2;250;128;114mPeakMemory: ?, AllocatedMemory: ? [0m                                                                                        |
            │    [1m [38;2;250;128;114mBuildTime: 68.416us [0m                                                                                                      |
            │    [1m [38;2;250;128;114mProbeTime: 250.194ms [0m                                                                                                     |
            │    [1m [38;2;250;128;114mEqJoinConjuncts: [4: locus_id = 60: locus_id] [0m                                                                            |
            │    [1m [38;2;250;128;114mDetail Timers:  [0m                                                                                                          |
            │        [1m [38;2;250;128;114mOutputProbeColumnTime: 70.124ms [min=11.174ms, max=170.119ms] [0m                                                        |
            ├── [0m<PROBE> OLAP_SCAN (id=0)  [0m                                                                                                                    |
            │       [0mEstimates: [row: 956349, cpu: 284476257.00, memory: 0.00, network: 0.00, cost: 142238128.50] [0m                                             |
            │       [0mTotalTime: 109.058ms (8.04%) [CPUTime: 4.926ms, ScanTime: 104.132ms] [0m                                                                     |
            │       [0mOutputRows: 956.349K (956349) [0m                                                                                                            |
            │       [0mTable: : germline__snv__occurrence [0m                                                                                                       |
            └── [0m<BUILD> OLAP_SCAN (id=1)  [0m                                                                                                                    |
                    [0mEstimates: [row: 2741, cpu: 169250.00, memory: 0.00, network: 0.00, cost: 84625.00] [0m                                                      |
                    [0mTotalTime: 1.074ms (0.08%) [CPUTime: 286.084us, ScanTime: 788.625us] [0m                                                                     |
                    [0mOutputRows: 2.741K (2741) [0m                                                                                                                |
                    [0mTable: : exomiser [0m                                                                                                                        |
 [0m                                                                                                                                                                |
 ```

### 4.1.3 Conclusion

Based on the benchmarks, removing Exomiser columns from the Occurrence table results in a decrease in query performance.

Because of this observation and the fact that re-using tasks for different cases should be a fairly rare event, we have decided to keep the Exomiser columns in the Occurrence table for now.

---
## 5. Design alternatives and trade-offs

| Alternative                          | Pros                                                                                                                                     | Cons                                                                                                                                                     | Decision |
|--------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| Keep `case_id` and `task_id` in ETL  | Possibly less join operations by avoiding to recreate the relationship between a Case and a Task from the Clinical `task_context` table. | Creates duplicate rows if we want to re-use the same Task for a different Case.                                                                          | Rejected |

---
## 6. Next Steps

- Complete `SJRA-867` implementation in the ETL codebase by: 
  - removing `case_id` references and replacing them with `task_id`.

---
## 7. References

- https://d3b.atlassian.net/browse/SJRA-712
- Data Model V2: https://whimsical.com/new-data-model-XBBcctECfLqNgAegjHY161

## 8. Appendix

### 8.1 Benchmark Script

```python
import time
from random import choice
from multiprocessing import Process, Queue

import mysql.connector
import numpy as np
from tabulate import tabulate


def load_locus_ids(host, port, user, database, limit=10000):
    conn = mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        database=database,
    )
    cursor = conn.cursor()
    cursor.execute(
        "SELECT locus_id FROM radiant.germline__snv__occurrence gso LIMIT %s;",
        (limit,),
    )
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return [r[0] for r in rows]


def run_benchmark(
    host,
    port,
    user,
    database,
    query,
    duration,
    locus_ids,
    process_id,
    result_queue=None,
):
    conn = mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        database=database,
    )
    cursor = conn.cursor()
    end_time = time.time() + duration
    total_queries = 0
    start_time = time.time()

    # collect per-query latencies (seconds)
    latencies = []

    while time.time() < end_time:
        locus_id = choice(locus_ids)

        q_start = time.time()
        cursor.execute(query, (locus_id,))
        if cursor.with_rows:
            result = cursor.fetchall()
            if not result:
                print("Warning: Query returned no results.")
        q_end = time.time()

        latencies.append(q_end - q_start)
        total_queries += 1

    elapsed = time.time() - start_time
    qps = total_queries / elapsed if elapsed > 0 else 0.0

    print(f"[Process {process_id}] Total queries: {total_queries}")
    print(f"[Process {process_id}] Elapsed time: {elapsed:.3f} s")
    print(f"[Process {process_id}] Approx QPS: {qps:.2f}")

    cursor.close()
    conn.close()

    # calculate percentiles per process
    lat_ms = [l * 1000.0 for l in latencies]
    p50 = np.percentile(lat_ms, 50)
    p90 = np.percentile(lat_ms, 90)
    p95 = np.percentile(lat_ms, 95)
    p99 = np.percentile(lat_ms, 99)

    if result_queue is not None:
        result_queue.put({
            "process_id": process_id,
            "qps": qps,
            "total_queries": total_queries,
            "p50_ms": p50,
            "p90_ms": p90,
            "p95_ms": p95,
            "p99_ms": p99,
        })


if __name__ == "__main__":

    query_no_join = """
    SELECT * FROM radiant.germline__snv__occurrence
    WHERE locus_id = %s;
    """

    query_joins = """
       SELECT 
           o.part,
           o.seq_id,
           o.task_id,
           o.locus_id,
           o.ad_ratio,
           o.gq,
           o.dp,
           o.ad_total,
           o.ad_ref,
           o.ad_alt,
           o.zygosity,
           o.calls,
           o.quality,
           o.filter,
           o.info_baseq_rank_sum,
           o.info_excess_het,
           o.info_fs,
           o.info_ds,
           o.info_fraction_informative_reads,
           o.info_inbreed_coeff,
           o.info_mleac,
           o.info_mleaf,
           o.info_mq,
           o.info_m_qrank_sum,
           o.info_qd,
           o.info_r2_5p_bias,
           o.info_read_pos_rank_sum,
           o.info_sor,
           o.info_vqslod,
           o.info_culprit,
           o.info_dp,
           o.info_haplotype_score,
           o.phased,
           o.parental_origin,
           o.father_dp,
           o.father_gq,
           o.father_ad_ref,
           o.father_ad_alt,
           o.father_ad_total,
           o.father_ad_ratio,
           o.father_calls,
           o.father_zygosity,
           o.mother_dp,
           o.mother_gq,
           o.mother_ad_ref,
           o.mother_ad_alt,
           o.mother_ad_total,
           o.mother_ad_ratio,
           o.mother_calls,
           o.mother_zygosity,
           o.transmission_mode,
           o.info_old_record,
           e.moi as exomiser_moi,
           e.acmg_classification as exomiser_acmg_classification,
           e.acmg_evidence as exomiser_acmg_evidence,
           e.variant_score as exomiser_variant_score,
           e.gene_combined_score as exomiser_gene_combined_score
       FROM radiant.germline__snv__occurrence o 
       LEFT JOIN radiant.exomiser e
       ON o.locus_id = e.locus_id
       WHERE o.locus_id = %s;
       """

    host = "localhost"
    port = 9030
    user = "root"
    database = "radiant"
    query = query_joins
    duration = 60
    num_processes = 5

    locus_ids = load_locus_ids(host, port, user, database, limit=10000)
    if not locus_ids:
        raise RuntimeError("No locus_ids loaded; check source query.")

    result_queue = Queue()
    processes = []

    for i in range(num_processes):
        p = Process(
            target=run_benchmark,
            args=(
                host,
                port,
                user,
                database,
                query,
                duration,
                locus_ids,
                i + 1,
                result_queue,
            ),
        )
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    results = []
    while not result_queue.empty():
        results.append(result_queue.get())

    # sort by process_id for cleaner display
    results.sort(key=lambda x: x["process_id"])

    # prepare table data
    table_data = []
    for r in results:
        table_data.append([
            r["process_id"],
            r["total_queries"],
            f"{r['qps']:.2f}",
            f"{r['p50_ms']:.2f}",
            f"{r['p90_ms']:.2f}",
            f"{r['p95_ms']:.2f}",
            f"{r['p99_ms']:.2f}",
        ])

    headers = ["Process ID", "Total Queries", "QPS", "P50 (ms)", "P90 (ms)", "P95 (ms)", "P99 (ms)"]
    print("\n" + "=" * 60)
    print("Benchmark Results Summary")
    print("=" * 60)
    print(tabulate(table_data, headers=headers, tablefmt="grid"))

    # aggregate stats
    total_qps = sum(r["qps"] for r in results)
    avg_qps = total_qps / len(results) if results else 0
    print(f"\nAverage QPS per process: {avg_qps:.2f}")
    print(f"Estimated aggregate QPS: {total_qps:.2f}")
```