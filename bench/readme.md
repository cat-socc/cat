# Bench reproductions

This directory contains the paper reproduction scripts for rangePut, Publish, and Aget.

Run from:

```bash
cd bench
```

## rangePut

This script reproduces the range write experiments. It first prepares base
objects, then runs range writes with `boto3` and `object_store`.

```bash
CLEAR_S3=1 ./rangePut/rangeput.sh
```

## Publish

This script reproduces the publish experiments. By default it prepares missing
S3 objects and runs the real publish runtime comparison for `naive`, `greedy`,
and `ptas`. Set `OFFLINE=1` to run only the local planning/cost summary.

```bash
./publish/publish.sh
```

## Aget

This script reproduces the read experiments. It runs no-delta reads first, then
creates the object-store delta layout from the bundled fio write pattern and
runs reads on that layout.

```bash
CLEAR_S3=1 ./Aget/run_aget_repro.sh
```

## Notes

- Default buckets are `rawiotest` and `rawiotest-shadow`.
- `CLEAR_S3=1` clears both buckets before the run.
- Results are written under each benchmark's result directory.
