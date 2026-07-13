from typing import List

from common.logger import print_debug, print_info
from object_store.mpu_algo.segment import Segment, Part


class PTASPrepare:
    def __init__(self, lengths: List[float], tau: float):
        self.lengths = lengths
        self.tau = tau
        self.block_ranges = None
        self.name_prefix = "S"
        self.normalized = None
        self.raw: List[Segment] = []

    def _normalize_segments_from_lengths(self) -> List[Segment]:
        """
        Normalize a length-only m-state input.

        Segment names are generated as S0/S1/..., remote/local kind is inferred
        from tau, and consecutive local segments are merged.
        """
        for idx, length in enumerate(self.lengths):
            kind = "remote" if length >= self.tau else "local"
            self.raw.append(
                Segment(
                    name=f"{self.name_prefix}{idx}",
                    kind=kind,
                    length=length,
                )
            )
        return self._merge_consecutive_locals(self.raw)

    def _split_no_consecutive_remote(self, segments: List[Segment]) -> List[List[Segment]]:
        """
        Split a sequence at consecutive remote boundaries.

        This keeps each subproblem free of consecutive remote segments, which is
        convenient for the b=2 planner.
        """
        if not segments:
            return []

        blocks: List[List[Segment]] = []
        cur: List[Segment] = [segments[0]]

        for seg in segments[1:]:
            if seg.kind == "remote" and cur[-1].kind == "remote":
                blocks.append(cur)
                cur = [seg]
            else:
                cur.append(seg)

        blocks.append(cur)
        return blocks


    def _merge_consecutive_locals(self, segments: List[Segment]) -> List[Segment]:
        """Convert raw m-state into an R/L sequence with no consecutive locals."""
        merged: List[Segment] = []
        for seg in segments:
            if seg.kind == "local" and merged and merged[-1].kind == "local":
                last = merged[-1]
                orig_segs = []
                if last.original_segments:
                    orig_segs.extend(last.original_segments)
                else:
                    orig_segs.append((last.name, last.length))
                if seg.original_segments:
                    orig_segs.extend(seg.original_segments)
                else:
                    orig_segs.append((seg.name, seg.length))
                merged[-1] = Segment(
                    name=f"{last.name}+{seg.name}",
                    kind="local",
                    length=last.length + seg.length,
                    original_segments=orig_segs,
                )
            else:
                if not seg.original_segments:
                    seg.original_segments = [(seg.name, seg.length)]
                merged.append(seg)
        return merged


    def _find_b2_block_ranges(self, normalized: List[Segment]) -> List[tuple[int, int]]:
        """
        Find non-overlapping R-L-R-L blocks.

        Returns half-open (start, end) ranges, where each range contains exactly
        four segments.
        """
        ranges: List[tuple[int, int]] = []
        i = 0
        n = len(normalized)

        while i + 4 <= n:
            s0, s1, s2, s3 = normalized[i:i + 4]
            if (s0.kind == "remote" and s1.kind == "local"
                    and s2.kind == "remote" and s3.kind == "local"):
                ranges.append((i, i + 4))
                i += 4
            else:
                i += 1

        return ranges

    def _is_b_block(self, segs: List[Segment], b: int) -> bool:
        """b-block: (R,L) repeated b times, i.e., length=2b, starts with remote, ends with local."""
        if len(segs) != 2 * b:
            return False
        for j, s in enumerate(segs):
            if j % 2 == 0 and s.kind != "remote":
                return False
            if j % 2 == 1 and s.kind != "local":
                return False
        return True


    def _find_b_block_ranges(self, normalized: List[Segment], b: int) -> List[tuple[int, int]]:
        """
        Find non-overlapping b-blocks: (R,L)^b.

        Returns half-open (start, end) ranges containing exactly 2b segments.
        """
        ranges: List[tuple[int, int]] = []
        i = 0
        n = len(normalized)

        while i + 2 * b <= n:
            window = normalized[i:i + 2 * b]
            if self._is_b_block(window, b):
                ranges.append((i, i + 2 * b))
                i += 2 * b
            else:
                i += 1
        return ranges

    def _plan_segments_with_b2(self, normalized: List[Segment]) -> List[tuple[int, int]]:
        """
        Select all R-L-R-L 2-blocks for the b=2 core planner.

        Prefixes, gaps, and suffixes fall back to the greedy planner.
        """
        n = len(normalized)
        if n == 0:
            return []

        block_ranges = self._find_b2_block_ranges(normalized)
        return block_ranges

    def ptas_prepare(self, split_rr: bool = True) -> tuple[List[tuple[int, int]], List[Segment]]:
        """
        Prepare PTAS input data and return block ranges.

        Args:
            split_rr: Whether to split at consecutive remote boundaries.

        Returns:
            Block ranges as (start, end) index intervals.
        """
        normalized = self._normalize_segments_from_lengths()
        if split_rr:
            blocks = self._split_no_consecutive_remote(normalized)
            all_ranges = []
            offset = 0
            for block in blocks:
                block_ranges = self._find_b2_block_ranges(block)
                for start, end in block_ranges:
                    all_ranges.append((offset + start, offset + end))
                offset += len(block)
            self.block_ranges = all_ranges
        else:
            self.block_ranges = self._plan_segments_with_b2(normalized)
        return self.block_ranges, normalized, self.raw

    def ptas_prepare_with_b_num(self, b_num: int, split_rr: bool = True) -> tuple[List[tuple[int, int]], List[Segment]]:
        normalized = self._normalize_segments_from_lengths()
        self.normalized = normalized

        if split_rr:
            blocks = self._split_no_consecutive_remote(normalized)
            all_ranges: List[tuple[int, int]] = []
            offset = 0
            for block in blocks:
                block_ranges = self._find_b_block_ranges(block, b_num)
                for start, end in block_ranges:
                    all_ranges.append((offset + start, offset + end))
                offset += len(block)
            self.block_ranges = all_ranges
        else:
            self.block_ranges = self._find_b_block_ranges(normalized, b_num)

        return self.block_ranges, normalized, self.raw

    def print_normalized(self, normalized: List[Segment]) -> None:
        for seg in normalized:
            print_debug(f"{seg.name} {seg.kind} {seg.length}")
        print_debug("-" * 10)

if __name__ == "__main__":
    lengths=[3, 3, 3, 9, 3, 3, 14, 6, 3, 3, 6, 3, 26, 3, 9, 3, 3]
    tau = 5.0
    prepare = PTASPrepare(lengths, tau)
    block_ranges, normalized, raw = prepare.ptas_prepare()
    print_info("=" * 60)
    print_info("Raw")
    print_info("=" * 60)
    for seg in raw:
        print_info(f"{seg.name} {seg.kind} {seg.length}")
    print_info("=" * 60)
    print_info("Normalized")
    print_info("=" * 60)
    for seg in normalized:
        print_info(f"{seg.name} {seg.kind} {seg.length}")
    print_info("=" * 60)
    print_info("Block Ranges")
    print_info("=" * 60)
    for start, end in block_ranges:
        print_info(f"{start} {end}")
