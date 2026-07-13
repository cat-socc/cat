ZERO_SOURCE_PATH = "zero://fill"


def is_zero_source(path: str) -> bool:
    return path == ZERO_SOURCE_PATH


class Segment:
    """
    Logical interval [offset, offset + length) mapped to source_path
    [source_offset, source_offset + length).
    """
    __slots__ = ("offset", "length", "source_path", "source_offset")

    def __init__(self, offset: int, length: int, source_path: str, source_offset: int):
        self.offset = offset
        self.length = length
        self.source_path = source_path
        self.source_offset = source_offset

    def end(self) -> int:
        return self.offset + self.length

    def is_adjacent_and_contiguous(self, other: "Segment") -> bool:
        return (self.end() == other.offset and
                self.source_path == other.source_path and
                self.source_offset + self.length == other.source_offset)

    def to_dict(self) -> dict:
        return {
            "offset": self.offset,
            "length": self.length,
            "source_path": self.source_path,
            "source_offset": self.source_offset,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Segment":
        return cls(d["offset"], d["length"], d["source_path"], d["source_offset"])
