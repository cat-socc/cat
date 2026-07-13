from typing import List
from object_store.mpu_algo.segment import Segment, Part, PartSegment

class GreedyBaseline:
    def __init__(self, normalized: List[Segment], tau: float):
        self.normalized = normalized
        self.tau = tau

    def greedy_baseline_plan(self) -> List[Part]:
        parts: List[Part] = []
        i = 0
        offset = 0.0
        n = len(self.normalized)

        def advance_if_empty():
            nonlocal i, offset
            while i < n and offset >= self.normalized[i].length:
                i += 1
                offset = 0.0

        advance_if_empty()

        while i < n:
            seg = self.normalized[i]
            remaining = seg.length - offset
            if remaining <= 0:
                i += 1
                offset = 0.0
                advance_if_empty()
                continue

            # ---------- Case A: prefer ONE remote segment as ONE remote part (no splitting) ----------
            if seg.kind == "remote" and remaining >= self.tau:
                pieces = [PartSegment(seg=seg, start=offset, length=remaining)]
                parts.append(Part(pieces=pieces))
                # consume this segment entirely
                i += 1
                offset = 0.0
                advance_if_empty()
                continue

            # ---------- Case B: build a LOCAL part by extending forward until size >= tau ----------
            pieces: List[PartSegment] = []
            size = 0.0

            while size < self.tau and i < n:
                seg = self.normalized[i]
                remaining = seg.length - offset
                if remaining <= 0:
                    i += 1
                    offset = 0.0
                    continue

                need = self.tau - size
                take = min(remaining, need)

                pieces.append(PartSegment(seg=seg, start=offset, length=take))
                size += take
                offset += take

                if offset >= seg.length:
                    i += 1
                    offset = 0.0

            if size < self.tau and len(pieces) > 0:
                pass

            parts.append(Part(pieces=pieces))
            advance_if_empty()

        # merge tail if < tau
        if len(parts) >= 2 and parts[-1].size < self.tau:
            tail = parts.pop()
            parts[-1].pieces.extend(tail.pieces)

        return parts
