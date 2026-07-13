from __future__ import annotations
from bisect import bisect_left, bisect_right
from typing import List, Optional, Tuple

from object_store.object_meta.segment import Segment, is_zero_source
from object_store.mpu_algo.executor import Executor


def _is_local_source(path: str) -> bool:
    return path.startswith("file://")


class ObjectMap:
    def __init__(self, segments: Optional[List[Segment]] = None):
        self.segments: List[Segment] = segments or []
        self.segments.sort(key=lambda s: s.offset)
        self._assert_invariants()

    def _assert_invariants(self) -> None:
        for i in range(1, len(self.segments)):
            prev, cur = self.segments[i-1], self.segments[i]
            if prev.end() > cur.offset:
                raise ValueError("ObjectMap invariant violated: segments overlap or unsorted")

    def _coalesce_last(self) -> None:
        if len(self.segments) < 2:
            return
        a, b = self.segments[-2], self.segments[-1]
        if a.is_adjacent_and_contiguous(b):
            self.segments[-2] = Segment(a.offset, a.length + b.length, a.source_path, a.source_offset)
            self.segments.pop()

    def append_segment(self, seg: Segment) -> None:
        if self.segments and seg.offset < self.segments[-1].end():
            raise ValueError("Segments must be appended in non-overlapping order")
        self.segments.append(seg)
        self._coalesce_last()

    @staticmethod
    def _trunc_right_keep_left(seg: Segment, cut: int) -> Segment:
        new_len = cut - seg.offset
        return Segment(seg.offset, new_len, seg.source_path, seg.source_offset)

    @staticmethod
    def _trunc_left_keep_right(seg: Segment, cut: int) -> Segment:
        shift = cut - seg.offset
        new_len = seg.end() - cut
        return Segment(cut, new_len, seg.source_path, seg.source_offset + shift)


    def transform_for_virtual(self, v_off: int, v_len: int) -> None:
        v_end = v_off + v_len
        out: List[Segment] = []

        for seg in self.segments:
            s, e = seg.offset, seg.end()

            # Case A: Disjoint
            if e <= v_off or s >= v_end:
                out.append(seg)
                continue

            # Case B: Fully covered -> drop
            if v_off <= s and e <= v_end:
                continue

            # Case C: keep the left side of the overlapped segment.
            if s < v_off and e <= v_end:
                out.append(self._trunc_right_keep_left(seg, v_off))
                continue

            # Case D: keep the right side of the overlapped segment.
            if s >= v_off and e > v_end:
                out.append(self._trunc_left_keep_right(seg, v_end))
                continue

            # Case E: Inside -> split to left & right
            left = self._trunc_right_keep_left(seg, v_off)
            right = self._trunc_left_keep_right(seg, v_end)
            out.append(left)
            out.append(right)

        out.sort(key=lambda x: x.offset)
        merged: List[Segment] = []
        for seg in out:
            if merged and merged[-1].is_adjacent_and_contiguous(seg):
                last = merged[-1]
                merged[-1] = Segment(last.offset, last.length + seg.length, last.source_path, last.source_offset)
            else:
                merged.append(seg)
        self.segments = merged
        self._assert_invariants()

    def update_with_new_segments(self, v_off: int, v_len: int, new_segments: List[Segment]) -> None:
        """
        new_segments must contiguously cover [v_off, v_off + v_len).
        """
        self.transform_for_virtual(v_off, v_len)

        cur = v_off
        for seg in new_segments:
            if seg.offset != cur:
                raise ValueError(f"Gap not contiguously covered: expect {cur}, got {seg.offset}")
            cur += seg.length
        if cur != v_off + v_len:
            raise ValueError("New segments do not exactly cover the virtual range")

        left  = [s for s in self.segments if s.end() <= v_off]
        right = [s for s in self.segments if s.offset >= v_off + v_len]

        merged: List[Segment] = []

        def push(seg: Segment):
            if merged and merged[-1].is_adjacent_and_contiguous(seg):
                last = merged[-1]
                merged[-1] = Segment(last.offset, last.length + seg.length, last.source_path, last.source_offset)
            else:
                merged.append(seg)

        for s in left:
            push(s)
        for s in new_segments:
            push(s)
        for s in right:
            push(s)

        self.segments = merged
        self._assert_invariants()

    def update_with_new_segments_bulk(
        self, new_segments: List[Segment], *, assume_sorted: bool = False
    ) -> None:
        """
        Batch variant for many sparse writes.

        Each new segment replaces the old mapping for its own logical interval. Unlike
        update_with_new_segments(), the segments do not need to form one contiguous range, but
        they must be non-overlapping. This avoids rebuilding the full object map once per 4KB page.
        """
        new_segments = [s for s in new_segments if s.length > 0]
        if not new_segments:
            return
        if not assume_sorted:
            new_segments.sort(key=lambda s: s.offset)
        for i in range(1, len(new_segments)):
            if new_segments[i - 1].end() > new_segments[i].offset:
                raise ValueError("Bulk new segments overlap or are unsorted")

        new_start = new_segments[0].offset
        new_end = max(seg.end() for seg in new_segments)
        old_starts = [seg.offset for seg in self.segments]
        old_ends = [seg.end() for seg in self.segments]
        first_old = bisect_right(old_ends, new_start)
        last_old = bisect_left(old_starts, new_end)
        left = self.segments[:first_old]
        overlapping_old = self.segments[first_old:last_old]
        right = self.segments[last_old:]

        old_remainders: List[Segment] = []
        new_idx = 0
        new_count = len(new_segments)

        for old in overlapping_old:
            old_start = old.offset
            old_end = old.end()
            cursor = old_start

            while new_idx < new_count and new_segments[new_idx].end() <= old_start:
                new_idx += 1

            j = new_idx
            while j < new_count and new_segments[j].offset < old_end:
                new = new_segments[j]
                if new.offset > cursor:
                    keep_end = min(new.offset, old_end)
                    old_remainders.append(Segment(
                        cursor,
                        keep_end - cursor,
                        old.source_path,
                        old.source_offset + (cursor - old_start),
                    ))
                cursor = max(cursor, new.end())
                if cursor >= old_end:
                    break
                j += 1

            if cursor < old_end:
                old_remainders.append(Segment(
                    cursor,
                    old_end - cursor,
                    old.source_path,
                    old.source_offset + (cursor - old_start),
                ))

        merged: List[Segment] = []
        i = 0
        j = 0

        def push(seg: Segment):
            if merged and merged[-1].is_adjacent_and_contiguous(seg):
                last = merged[-1]
                merged[-1] = Segment(
                    last.offset,
                    last.length + seg.length,
                    last.source_path,
                    last.source_offset,
                )
            else:
                merged.append(seg)

        for seg in left:
            push(seg)

        while i < len(old_remainders) or j < len(new_segments):
            if j >= len(new_segments) or (
                i < len(old_remainders) and old_remainders[i].offset <= new_segments[j].offset
            ):
                push(old_remainders[i])
                i += 1
            else:
                push(new_segments[j])
                j += 1

        for seg in right:
            push(seg)

        self.segments = merged
        self._assert_invariants()

    def plan_read(self, offset: int, length: int) -> List[Tuple[str, int, int]]:
        """Build a sequential read plan as [(source_path, source_offset, read_len), ...]."""
        if length == 0:
            return []
        end = offset + length
        plan: List[Tuple[str, int, int]] = []
        covered = 0

        for seg in self.segments:
            if seg.end() <= offset:
                continue
            if seg.offset >= end:
                break
            s = max(offset, seg.offset)
            e = min(end, seg.end())
            src_off = seg.source_offset + (s - seg.offset)
            plan.append((seg.source_path, src_off, e - s))
            covered += (e - s)

        if covered != length:
            raise ValueError(f"Read plan under-covered: need {length}, got {covered}")
        return plan

    def plan_mpu_tasks(self,
                   min_part_size: int = 5 * 1024 * 1024,      # L = 5MB
                   max_copy_chunk: int = 128 * 1024 * 1024,    # H = 128MB
                   algo: str = "ptas"
                   ) -> List[dict]:
        """
        Build MPU tasks from the current object map.

        Returns tasks in part order:
        - {'type':'copy', 'source_path', 'source_offset', 'length', 'logical_offset'}
        - {'type':'fetch', 'pieces':[{'source_path','source_offset','length','logical_offset'}, ...],
            'length': total_len}
        """
        segs = self.segments
        if not segs:
            return []
        lengths = [float(seg.length) for seg in segs]
        executor = Executor(lengths, float(min_part_size), print_flag=False)
        executor.prepare()
        if algo == "ptas":
            parts = executor.plan_with_ptas()
        elif algo == "greedy":
            parts = executor.plan_with_greedy()
        else:
            raise ValueError(f"Invalid algorithm: {algo}")
        tasks: List[dict] = []

        for part in parts:
            if not part.is_local:
                if len(part.pieces) == 1:
                    ps = part.pieces[0]
                    pieces_data = self._partsegment_to_pieces(ps, segs)
                    if pieces_data:
                        # Local file sources cannot use UploadPartCopy, so execute
                        # them through the same fetch+upload path as mixed parts.
                        if (
                            len(pieces_data) > 1
                            or any(
                                _is_local_source(p["source_path"])
                                or is_zero_source(p["source_path"])
                                for p in pieces_data
                            )
                        ):
                            total_length = sum(p["length"] for p in pieces_data)
                            tasks.append({
                                "type": "fetch",
                                "pieces": pieces_data,
                                "length": total_length,
                            })
                            continue

                        piece = pieces_data[0]
                        actual_length = piece["length"]

                        if actual_length > max_copy_chunk:
                            remain = actual_length
                            src_off = piece["source_offset"]
                            log_off = piece["logical_offset"]
                            while remain > 0:
                                chunk = max_copy_chunk if remain > max_copy_chunk else remain
                                tasks.append({
                                    "type": "copy",
                                    "source_path": piece["source_path"],
                                    "source_offset": src_off,
                                    "length": chunk,
                                    "logical_offset": log_off,
                                })
                                remain -= chunk
                                src_off += chunk
                                log_off += chunk
                        else:
                            tasks.append({
                                "type": "copy",
                                "source_path": piece["source_path"],
                                "source_offset": piece["source_offset"],
                                "length": actual_length,
                                "logical_offset": piece["logical_offset"],
                            })
            else:
                pieces = []
                for ps in part.pieces:
                    pieces_data = self._partsegment_to_pieces(ps, segs)
                    pieces.extend(pieces_data)
                if pieces:
                    total_length = sum(p["length"] for p in pieces)
                    tasks.append({
                        "type": "fetch",
                        "pieces": pieces,
                        "length": total_length,
                    })

        return tasks

    def _partsegment_to_pieces(self, ps, segs: List[Segment]) -> List[dict]:
        """Convert a PartSegment into executable piece dictionaries."""
        pieces = []

        if ps.seg.original_segments and len(ps.seg.original_segments) > 1:
            offset = 0.0
            remaining = ps.length
            start_pos = ps.start

            for orig_name, orig_len in ps.seg.original_segments:
                if remaining <= 0:
                    break

                if start_pos < offset + orig_len:
                    orig_start = start_pos - offset
                    take = min(remaining, offset + orig_len - start_pos)

                    seg_idx = self._extract_segment_index(orig_name)
                    if seg_idx is not None and 0 <= seg_idx < len(segs):
                        orig_seg = segs[seg_idx]
                        pieces.append({
                            "source_path": orig_seg.source_path,
                            "source_offset": int(orig_seg.source_offset + orig_start),
                            "length": int(take),
                            "logical_offset": int(orig_seg.offset + orig_start),
                        })

                    remaining -= take
                    if remaining <= 0:
                        break
                    start_pos = offset + orig_len
                offset += orig_len
        else:
            seg_idx = self._extract_segment_index(ps.seg.name)
            if seg_idx is not None and 0 <= seg_idx < len(segs):
                orig_seg = segs[seg_idx]
                pieces.append({
                    "source_path": orig_seg.source_path,
                    "source_offset": int(orig_seg.source_offset + ps.start),
                    "length": int(ps.length),
                    "logical_offset": int(orig_seg.offset + ps.start),
                })

        return pieces

    def _extract_segment_index(self, name: str) -> Optional[int]:
        """Extract the first segment index from names like S0 or S0+S1."""
        if name.startswith("S"):
            try:
                idx_str = name.split("+")[0][1:]
                return int(idx_str)
            except (ValueError, IndexError):
                pass
        return None

    def to_dict(self) -> dict:
        return {
            "segments": [s.to_dict() for s in self.segments]
        }

    @classmethod
    def from_dict(cls, d: dict) -> "ObjectMap":
        segs = [Segment.from_dict(x) for x in d["segments"]]
        return cls(segs)
