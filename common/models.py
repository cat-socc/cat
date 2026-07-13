from pydantic import BaseModel, field_validator
import base64
from typing import Optional, List

class Range(BaseModel):
    offset: int
    length: int

# modified range for patching
class PatchRangeRequest(BaseModel):
    local_file_path: str
    total_modified_size: int
    file_size: int
    ranges:  List[Range]

class DataRangeRequest(BaseModel):
    ranges:  List[Range]
    data: bytes

class FilePathRequest(BaseModel):
    ranges:  List[Range]
    file_path: str
    file_size: int
    total_modified_size: Optional[int] = None

class AtomicPutRequest(BaseModel):
    ranges:  List[Range]
    file_path: Optional[str] = None
    file_size: Optional[int] = None
    total_modified_size: Optional[int] = None
    data: Optional[bytes] = None
