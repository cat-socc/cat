from dataclasses import dataclass, field
from typing import List, Literal, Optional

SegKind = Literal["remote", "local"]


@dataclass
class Segment:
    """Logical segment such as R1, L1, R2, or L2."""
    name: str
    kind: SegKind      # "remote" or "local"
    length: float      # total length (bytes)
    original_segments: List[tuple[str, float]] = field(default_factory=list)


@dataclass
class PartSegment:
    """A slice of a Segment used by one part."""
    seg: Segment
    start: float       # in [0, seg.length) (bytes)
    length: float      # in (0, seg.length - start) (bytes)


@dataclass
class Part:
    """A contiguous part composed of ordered PartSegment slices."""
    pieces: List[PartSegment]

    @property
    def size(self) -> float:
        return sum(p.length for p in self.pieces)

    @property
    def remote_size(self) -> float:
        return sum(p.length for p in self.pieces if p.seg.kind == "remote")

    @property
    def is_local(self) -> bool:
        """A single remote segment is a remote part; all other parts are local."""
        is_remote = len(self.pieces) == 1 and self.pieces[0].seg.kind == "remote"
        return not is_remote

    @property
    def fetch_cost(self) -> float:
        """Network fetch cost, counted only for remote bytes inside local parts."""
        return self.remote_size if self.is_local else 0.0
