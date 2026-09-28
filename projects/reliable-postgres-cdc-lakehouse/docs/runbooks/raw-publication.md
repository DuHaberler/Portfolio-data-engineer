# Runbook - Raw Publication 

## Purpose

Operate the Raw Publication Job responsible for publishing validated raw datasets from Raw Landing into Raw Compacted.
The job processes one dataset and one "ingestion_date_utc" partition per execution.

By default, the target partition is D-1 UTC.
A specific partition can be provided through "PARTITION_DATE" for testing, backfill or reprocessing.

## Inputs

Required environment variables:

- "DATASET"
- "LANDING_PATH"
- "COMPACTED_PATH"
- "CONTROL_PATH"
- "MINIO_ROOT_USER"
- "MINIO_ROOT_PASSWORD"

optional environment variables:

- "PARTITION_DATE"
- "RUNNING_STALE_AFTER_MINUTES"

If "PARTITION_DATE" is not provided, the job processes D-1 UTC.

Default stale timeout:

"30 minutes"

## Standard Execution

Powershell command example for the "orders" dataset:

docker compose run --rm `
-e DATASET=orders `
-e LANDING_PATH=s3a://lakehouse/raw/landing/orders `
-e COMPACTED_PATH=s3a://lakehouse/raw/compacted/orders `
-e CONTROL_PATH=s3a://lakehouse/_control/compaction/orders `
spark-orders `
/opt/spark/bin/spark-submit `
--packages org.apache.hadoop:hadoop-aws:3.4.2 `
/opt/spark/work-dir/compaction_job.py

Without "PARTITION_DATE", the execution targets D-1 UTC.

## Backfill or Specific Partition

To process a specific partition, provide "PARTITION_DATE".

Example:

docker compose run --rm `
-e DATASET=orders `
-e PARTITION_DATE=2026-09-17 `
-e LANDING_PATH=s3a://lakehouse/raw/landing/orders `
-e COMPACTED_PATH=s3a://lakehouse/raw/compacted/orders `
-e CONTROL_PATH=s3a://lakehouse/_control/compaction/orders `
spark-orders `
/opt/spark/bin/spark-submit `
--packages org.apache.hadoop:hadoop-aws:3.4.2 `
/opt/spark/work-dir/compaction_job.py


Expected format: YYYY-MM-DD

## Execution Flow

The job performs the following steps:
1. Resolve the target partition.
2. Read the existing execution control state.
3. Persist a new RUNNING state when execution is allowed.
4. Check wether the Raw Landing partition exists.
5. List the available Parquet files.
6. Evaluate the compaction policy.
7. Execute either DO_COMPACTION or NO_NEED.
8. Write result into a versioned run-<run_id path.
9. Calculate input and output identity fingerprints.
10. Validate the generated run.
11. Publish the run only when validation succeeds.

## Operations

### NO_NEED

Used when the partition does not meet the compaction criteria.
The original Parquet files are copied into the new versioned run without being rewritten.

### DO_COMPACTION

Used when the partition meets the configured compaction criteria.
Spark rewrites the partition into fewer Parquet files.

## Output

Successful physical runs are written to:

"raw/compacted/<dataset>/ingestion_date_utc=<YYYY-MM-DD>/run=<run_id>"

The existence of this path alone dows not mean that the run is published.
A run is published only after successful validation.

## Control State

Execution state is stored at:

"_control/compaction/<dataset>/<YYYY-MM-DD>.json"

Possible states:

### RUNNING

The partition is currently being processed
A recent RUNNING state prevents another normal execution from starting.

### COMPLETED

Processing finished successfully.
If validation was required, the successfully published run is available in: "published_path"
A partition already marked as COMPLETED is not processed again automatically.

### FAILED

The execution failed.
A new execution may retry the partition.

## Stale RUNNING State

A RUNNING state older than RUNNING_STALE_AFTER_MINUTES is considered stale.

Default: 30 minutes

A stale execution does not prevent a new attempt from starting.
This mechanism handles abandoned executions but not provide an atomic distributed lock between concurrent jobs.

## No Data

Two conditions are treated as successful conditions with no published output:
- the target Raw Landing partition does not exist;
- the partition exists but contains no Parquet files.

Expected output:
- status = COMPLETED
- input_files = 0
- output_files = 0
- input_rows = 0
- output_rows = 0
- no published_path

## Validation

Publication requires successful identity fingerprint validation.

Kafka event identity: (topic, kafka_partition, offset)

The validation compares input and output using:
- row count;
- minimum offset;
- maximum offset;
- aggregated xxhash64 identity hash.

Expected successful state:

status = COMPLETED
validation.result = PASSED
published_path = <validated run path>

If the fingerprints differ:

status = FAILED
validation.result = FAILED
published_path = null

## Verification

After a successful execution, verify:
1. The control JSON has status = COMPLETED
2. validation.result = PASSED
3. "published_path" points to the newly generated run-<>run_id
4. The referenced path exists in Raw Compacted.
5. "input_rows" and output"_rows are equal.
6. The expected Parquet files exist in the published path.

## Retry

For a failed execution:
1. Inspect the "error" filed in the control JSON.
2. Correct the underlying problem.
3. Execute the job again for the same dataset and partition.
4. Confirm that a new "run_id" is generated
5. Confirm that only successfully validated run becomes "published_path".

Previous failed physical runs may remain in storage until a retention or cleanup policy is introduced.

## Known Limitations

- Execution controldoes not currently provide an atomic distributed lock.
- Concurrent execution for the same dataset and partition must be prevented operationally.
- Failed or superseded runs are not automatically removed.
- Compaction thresholds are initial operational baselines and may require calibration.
- Retry orchestration is manual until Airflow is introduced.