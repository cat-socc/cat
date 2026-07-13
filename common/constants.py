
import os


def _int_env(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


# AWS S3 configuration. Credentials use the default AWS provider chain.
AWS_REGION = "us-east-1"

# S3 object configuration
OBJECT_SIZE_MB = 64
MAX_POOL_CONNECTIONS = _int_env("MAX_POOL_CONNECTIONS", 200)
CONNECT_TIMEOUT = _int_env("CONNECT_TIMEOUT", 10)
READ_TIMEOUT = _int_env("READ_TIMEOUT", 600)
MAX_CONCURRENCY = _int_env("MAX_CONCURRENCY", 100)
CHUNK_SIZE = OBJECT_SIZE_MB * 1024 * 1024
MULTIPART_CHUNK_SIZE = _int_env("MULTIPART_CHUNK_SIZE", 5 * 1024 * 1024)
# Object store configuration
CHUNK_PREFIX = "chunk/"
LOG_PREFIX = "log/"
LOG_SLOT_A = "a"
LOG_SLOT_B = "b"
DEFAULT_LOG_VERSION = "0"

# Snapshot configuration
SNAPSHOT_THRESHOLD = 1000
PART_SIZE = MULTIPART_CHUNK_SIZE
MULTIPART_THRESHOLD = _int_env("MULTIPART_THRESHOLD", 5 * 1024 * 1024)
