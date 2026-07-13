# CAT Proxy

CAT acts as a transparent layer between S3 clients and Amazon S3.
 It enhances S3’s in-place update capabilities for large objects and supports RangePut, Publish, and AGet.

## Quick Start

### Prerequisites
- Python 3.8 or higher
- AWS credentials configured
- Required packages: `boto3[crt]`, `fastapi`, `uvicorn`

### Installation
```bash
pip install -r requirements.txt
```

## Experiment Reproduction

The paper reproduction scripts for RangePut, Publish, and AGet are under `bench/`.

```bash
cd bench
cat readme.md
```

The benchmark entry points are:

```bash
CLEAR_S3=1 ./rangePut/rangeput.sh
./publish/publish.sh
CLEAR_S3=1 ./Aget/run_aget_repro.sh
```

### Running as a service
```bash
# Start the FastAPI service
uvicorn main:app --reload --port 8080
# or
python -m uvicorn main:app --reload --port 8080
```

```bash
# Write complete object (chunked storage)
curl -X POST "http://localhost:8080/put_object/my-bucket/my-file.dat" \
  -H "Content-Type: application/json" \
  -d '{"data": "SGVsbG8gV29ybGQ=", "ranges": []}'
  
# Read object range
curl -X GET "http://localhost:8080/get_object_range/my-bucket/my-file.dat?offset=0&length=1024"
```

### Running as the Python library

```python
# Read object range

import asyncio
from object_store.objects_manager import ObjectsManager
from core.dependencies import get_object_manager

manager = asyncio.run(get_object_manager())
data = asyncio.run(manager.read_object_range(bucket, key, offset, length))
```
