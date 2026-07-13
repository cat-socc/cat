from datetime import datetime, timezone
from typing import List, Optional

from object_store.object_meta.object_map import ObjectMap
from object_store.object_meta.segment import Segment, ZERO_SOURCE_PATH

"""
Improved object metadata manager
Provides more complete chunk meta and log meta information recording
Supports efficient data location query and status management
"""


class ObjectMetaManager:
    def __init__(
        self,
        primary_bucket: str,
        primary_key: str,
        file_size: int,
        base_size: Optional[int] = None,
    ):
        # header
        self.file_size: int = file_size
        self.primary_bucket: str = primary_bucket
        self.primary_key: str = primary_key
        self.modified_ts: datetime = datetime.now(timezone.utc)
        self.dirty_size: int = 0
        self.version: int = 1

        # object map: existing bytes point to the primary base object; logical
        # growth beyond primary EOF is represented as zero-fill.
        self.object_map: ObjectMap = ObjectMap(
            self._initial_segments(primary_bucket, primary_key, file_size, base_size)
        )

        self.shadow_bucket: Optional[str] = None
        self.shadow_key: Optional[str] = None

    @staticmethod
    def _initial_segments(
        primary_bucket: str,
        primary_key: str,
        file_size: int,
        base_size: Optional[int] = None,
    ) -> List[Segment]:
        logical_size = max(0, int(file_size))
        physical_size = logical_size if base_size is None else max(0, int(base_size))
        physical_size = min(physical_size, logical_size)
        segments: List[Segment] = []
        if physical_size > 0:
            segments.append(Segment(0, physical_size, f"{primary_bucket}/{primary_key}", 0))
        if logical_size > physical_size:
            segments.append(
                Segment(
                    physical_size,
                    logical_size - physical_size,
                    ZERO_SOURCE_PATH,
                    physical_size,
                )
            )
        return segments

    def repair_primary_segments_to_base_size(self, base_size: int) -> None:
        """
        Repair older metadata that mapped logical growth past primary EOF back to
        the primary object. Those bytes are sparse file growth and must read as zeros.
        """
        base_source = f"{self.primary_bucket}/{self.primary_key}"
        physical_size = max(0, int(base_size))
        repaired: List[Segment] = []

        for seg in self.object_map.segments:
            if seg.source_path != base_source:
                repaired.append(seg)
                continue
            if seg.source_offset >= physical_size:
                repaired.append(Segment(seg.offset, seg.length, ZERO_SOURCE_PATH, seg.offset))
                continue
            available = physical_size - seg.source_offset
            if available >= seg.length:
                repaired.append(seg)
                continue
            repaired.append(Segment(seg.offset, available, seg.source_path, seg.source_offset))
            repaired.append(
                Segment(
                    seg.offset + available,
                    seg.length - available,
                    ZERO_SOURCE_PATH,
                    seg.offset + available,
                )
            )

        self.object_map = ObjectMap(repaired)
    def range_put_commit(self, v_off: int, v_len: int, delta_segments: List[dict]) -> None:
        """
        Commit uploaded delta segments into the object map.

        delta_segments must be sorted or sortable by logical_offset. Each item has:
          {
            "logical_offset": int,
            "length": int,
            "source_path": "shadow_bucket/delta_key",
            "source_offset": int
          }
        """
        segs = [
            Segment(d["logical_offset"], d["length"], d["source_path"], d["source_offset"])
            for d in sorted(delta_segments, key=lambda x: x["logical_offset"])
        ]

        self.object_map.update_with_new_segments(v_off, v_len, segs)

        self.modified_ts = datetime.now(timezone.utc)
        self.dirty_size += v_len
        self.version += 1

    def plan_read(self, offset: int, length: int):
        return self.object_map.plan_read(offset, length)

    def plan_mpu_tasks(self) -> List[dict]:
        return self.object_map.plan_mpu_tasks()

    def to_dict(self) -> dict:
        return {
            "header": {
                "file_size": self.file_size,
                "primary_bucket": self.primary_bucket,
                "primary_key": self.primary_key,
                "modified_ts": self.modified_ts.isoformat(),
                "dirty_size": self.dirty_size,
                "version": self.version,
                "shadow_bucket": self.shadow_bucket,
                "shadow_key": self.shadow_key,
            },
            "object_map": self.object_map.to_dict(),
        }

    @classmethod
    def from_dict(cls, d: dict) -> "ObjectMetaManager":
        h = d["header"]
        inst = cls(h["primary_bucket"], h["primary_key"], h["file_size"])
        inst.modified_ts = datetime.fromisoformat(h["modified_ts"])
        inst.dirty_size = h["dirty_size"]
        inst.version = h["version"]
        inst.shadow_bucket = h.get("shadow_bucket")
        inst.shadow_key = h.get("shadow_key")
        inst.object_map = ObjectMap.from_dict(d["object_map"])
        return inst
