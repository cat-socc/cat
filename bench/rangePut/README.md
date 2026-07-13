# rangePut benchmark

Run the reproduction script:

```bash
cd bench/rangePut
./rangeput.sh
```

The script reports:

- `boto3 initial_write`: base method, full-object PUT for 1GB, 2GB, 4GB, 8GB, and 16GB.
- `object_store range_write`: cat method, range updates on prepared base objects.

## Experiments

Base experiment uses boto3 to PUT these full object sizes:

- 1GB
- 2GB
- 4GB
- 8GB
- 16GB

Cat experiment 1 uses a 1GB base object and modifies these ranges:

- 1MB
- 4MB
- 16MB
- 64MB
- 256MB

Cat experiment 2 modifies a fixed 64MB range on these base object sizes:

- 1GB
- 2GB
- 4GB
- 8GB
- 16GB

## Options

By default, the script uses:

- bucket: `rawiotest`
- shadow bucket: `rawiotest-shadow`
- iterations: `3`
- details: `res/details`
- summary: `res/summary`

Override them with environment variables:

```bash
BUCKET_NAME=rawiotest ITERATIONS=1 ./rangeput.sh
```

By default, the script does not delete S3 data. To reproduce from empty buckets:

```bash
CLEAR_S3=1 ./rangeput.sh
```
