from dataclasses import dataclass
import copy as pycopy
from typing import List, Optional, Sequence, Tuple

from object_store.mpu_algo.segment import Segment, Part, PartSegment


@dataclass
class WindowPlan:
    """Sliding-window plan result."""
    borrow_L1_from_R1: float  # x
    borrow_L1_from_R2: float  # y, borrowed from the left side of R2_rem
    mode: str                 # "none" / "only_R1" / "only_R2" / "both"


@dataclass
class B2WindowPlan:
    """Length-level planning result for b=2."""
    layout: str                # "case1", "window", "fallback"
    borrow_L2_from_R2: float   # alpha: R2 tail -> L2
    borrow_L1_from_R1: float   # x: R1 tail -> L1
    borrow_L1_from_R2: float   # y: R2_rem prefix -> L1
    mode: str                  # "none" / "only_R1" / "only_R2" / "both"


class PTAS2B:
    """PTAS optimizer for one R-L-R-L 2-block."""

    def __init__(self, tau: float = 5.0 * 1024 * 1024):
        """
        Initialize the PTAS 2-block optimizer.

        Args:
            tau: Minimum part size in bytes, for example 5 * 1024 * 1024.
        """
        self.tau = tau

    def optimize(self, R1: Segment, L1: Segment, R2: Segment, L2: Segment) -> List[Part]:
        """
        Optimize one R-L-R-L 2-block.

        Args:
            R1: First remote segment.
            L1: First local segment.
            R2: Second remote segment.
            L2: Second local segment.

        Returns:
            Optimized parts.
        """
        assert R1.kind == "remote" and R2.kind == "remote"
        assert L1.kind == "local" and L2.kind == "local"

        plan = self._plan_b2_lengths_with_window(
            R1.length, L1.length, R2.length, L2.length
        )

        if plan.layout == "case1":
            return self._build_case1_parts(R1, L1, R2, L2)

        part_L2, R2_rem_len = self._build_L2_part(R2, L2, plan.borrow_L2_from_R2)

        if plan.layout == "window":
            return self._build_window_parts(
                R1, L1, R2, L2, part_L2, R2_rem_len, plan
            )

        return self._fallback_b2_parts(R1, L1, R2, part_L2, R2_rem_len)

    def _preprocess_L2(self, R2_len: float, L2_len: float) -> tuple[float, float]:
        """Preprocess L2 so its part reaches tau when possible."""
        need_L2 = max(0.0, self.tau - L2_len)
        borrow_L2 = min(need_L2, R2_len)
        R2_rem = R2_len - borrow_L2
        return borrow_L2, R2_rem

    def _sliding_window_L1(
        self, r1: float, l1: float, r2: float
    ) -> Optional[WindowPlan]:
        """Plan a sliding window over R1 | L1 | R2_rem."""
        need_L1 = max(0.0, self.tau - l1)

        if need_L1 == 0.0:
            if r1 >= self.tau and r2 >= self.tau:
                return WindowPlan(0.0, 0.0, mode="none")
            return None

        x = need_L1
        y = 0.0
        if (x <= r1 and
            r1 - x >= self.tau and
            r2       >= self.tau):
            return WindowPlan(x, y, mode="only_R1")

        x = 0.0
        y = need_L1
        if (y <= r2 and
            r2 - y >= self.tau and
            r1       >= self.tau):
            return WindowPlan(x, y, mode="only_R2")

        lower_x = max(0.0, need_L1 - (r2 - self.tau))
        upper_x = min(need_L1, r1 - self.tau)

        if lower_x < upper_x:
            x = lower_x
            y = need_L1 - x
            if (0.0 < x < r1 and
                0.0 < y < r2 and
                r1 - x >= self.tau and
                r2 - y >= self.tau):
                return WindowPlan(x, y, mode="both")

        return None

    def _plan_b2_lengths_with_window(
        self, R1_len: float, L1_len: float, R2_len: float, L2_len: float
    ) -> B2WindowPlan:
        """Build the length-level plan for b=2."""
        borrow_L2, R2_rem = self._preprocess_L2(R2_len, L2_len)

        if R2_rem < self.tau:
            return B2WindowPlan(
                layout="case1",
                borrow_L2_from_R2=borrow_L2,
                borrow_L1_from_R1=0.0,
                borrow_L1_from_R2=0.0,
                mode="none",
            )

        wp = self._sliding_window_L1(R1_len, L1_len, R2_rem)
        if wp is not None:
            return B2WindowPlan(
                layout="window",
                borrow_L2_from_R2=borrow_L2,
                borrow_L1_from_R1=wp.borrow_L1_from_R1,
                borrow_L1_from_R2=wp.borrow_L1_from_R2,
                mode=wp.mode,
            )

        return B2WindowPlan(
            layout="fallback",
            borrow_L2_from_R2=borrow_L2,
            borrow_L1_from_R1=0.0,
            borrow_L1_from_R2=0.0,
            mode="none",
        )

    def _build_L2_part(self, R2: Segment, L2: Segment, borrow_L2: float) -> tuple[Part, float]:
        """Build the L2 part and keep it at least tau when possible."""
        R2_len = R2.length
        R2_rem_len = R2_len - borrow_L2

        if borrow_L2 > 0:
            p_R2_for_L2 = PartSegment(
                seg=R2,
                start=R2_len - borrow_L2,
                length=borrow_L2,
            )
            p_L2 = PartSegment(seg=L2, start=0.0, length=L2.length)
            part_L2 = Part(pieces=[p_R2_for_L2, p_L2])
        else:
            p_L2 = PartSegment(seg=L2, start=0.0, length=L2.length)
            part_L2 = Part(pieces=[p_L2])

        if part_L2.size < self.tau:
            need = self.tau - part_L2.size
            additional_borrow = min(need, R2_rem_len)
            if additional_borrow > 0:
                p_R2_additional = PartSegment(
                    seg=R2,
                    start=R2_len - borrow_L2 - additional_borrow,
                    length=additional_borrow,
                )
                part_L2.pieces.insert(0, p_R2_additional)
                R2_rem_len -= additional_borrow

        return part_L2, R2_rem_len

    def _build_case1_parts(
        self, R1: Segment, L1: Segment, R2: Segment, L2: Segment
    ) -> List[Part]:
        """Build parts for case1."""
        p_R1 = PartSegment(seg=R1, start=0.0, length=R1.length)
        part_remote = Part(pieces=[p_R1])

        p_L1 = PartSegment(seg=L1, start=0.0, length=L1.length)
        p_R2 = PartSegment(seg=R2, start=0.0, length=R2.length)
        p_L2 = PartSegment(seg=L2, start=0.0, length=L2.length)
        part_big_local = Part(pieces=[p_L1, p_R2, p_L2])

        if part_big_local.size < self.tau:
            need = self.tau - part_big_local.size
            borrow = min(need, R1.length)
            if borrow > 0:
                p_R1_tail = PartSegment(seg=R1, start=R1.length - borrow, length=borrow)
                part_big_local.pieces.insert(0, p_R1_tail)
                part_remote = Part(pieces=[PartSegment(seg=R1, start=0.0, length=R1.length - borrow)])

        return [part_remote, part_big_local]

    def _build_window_parts(
        self, R1: Segment, L1: Segment, R2: Segment, L2: Segment,
        part_L2: Part, R2_rem_len: float, plan: B2WindowPlan
    ) -> List[Part]:
        """Build parts for the window layout."""
        x = plan.borrow_L1_from_R1
        y = plan.borrow_L1_from_R2
        mode = plan.mode

        parts: List[Part] = []

        if mode == "none":
            part_R1 = Part(pieces=[PartSegment(R1, 0.0, R1.length)])
            parts.append(part_R1)

            part_L1 = Part(pieces=[PartSegment(L1, 0.0, L1.length)])
            if part_L1.size < self.tau:
                need = self.tau - part_L1.size
                borrow = min(need, R1.length)
                if borrow > 0:
                    p_R1_tail = PartSegment(seg=R1, start=R1.length - borrow, length=borrow)
                    part_L1.pieces.insert(0, p_R1_tail)
                    parts[-1] = Part(pieces=[PartSegment(R1, 0.0, R1.length - borrow)])
            parts.append(part_L1)

            if R2_rem_len > 0:
                part_R2_rem = Part(
                    pieces=[PartSegment(R2, 0.0, R2_rem_len)]
                )
                parts.append(part_R2_rem)

            parts.append(part_L2)
            return parts

        if mode == "only_R1":
            R1_left_len = R1.length - x

            if R1_left_len > 0:
                part_R1_left = Part(
                    pieces=[PartSegment(R1, 0.0, R1_left_len)]
                )
                parts.append(part_R1_left)

            p_R1_tail = PartSegment(
                seg=R1,
                start=R1.length - x,
                length=x,
            )
            p_L1 = PartSegment(L1, 0.0, L1.length)
            part_L1 = Part(pieces=[p_R1_tail, p_L1])
            parts.append(part_L1)

            if R2_rem_len > 0:
                part_R2_rem = Part(
                    pieces=[PartSegment(R2, 0.0, R2_rem_len)]
                )
                parts.append(part_R2_rem)

            parts.append(part_L2)
            return parts

        if mode == "only_R2":
            part_R1 = Part(pieces=[PartSegment(R1, 0.0, R1.length)])
            parts.append(part_R1)

            p_L1 = PartSegment(L1, 0.0, L1.length)
            p_R2_prefix = PartSegment(R2, 0.0, y)
            part_L1 = Part(pieces=[p_L1, p_R2_prefix])
            parts.append(part_L1)

            if R2_rem_len - y > 0:
                p_R2_rest = PartSegment(
                    seg=R2,
                    start=y,
                    length=R2_rem_len - y,
                )
                part_R2_rest = Part(pieces=[p_R2_rest])
                parts.append(part_R2_rest)

            parts.append(part_L2)
            return parts

        if mode == "both":
            R1_left_len = R1.length - x

            if R1_left_len > 0:
                part_R1_left = Part(
                    pieces=[PartSegment(R1, 0.0, R1_left_len)]
                )
                parts.append(part_R1_left)

            p_R1_tail = PartSegment(
                seg=R1,
                start=R1.length - x,
                length=x,
            )
            p_L1 = PartSegment(L1, 0.0, L1.length)
            p_R2_prefix = PartSegment(R2, 0.0, y)
            part_L1 = Part(pieces=[p_R1_tail, p_L1, p_R2_prefix])
            parts.append(part_L1)

            if R2_rem_len - y > 0:
                p_R2_rest = PartSegment(
                    seg=R2,
                    start=y,
                    length=R2_rem_len - y,
                )
                part_R2_rest = Part(pieces=[p_R2_rest])
                parts.append(part_R2_rest)

            parts.append(part_L2)
            return parts

        raise ValueError(f"Unknown window mode: {mode}")

    def _fallback_b2_parts(
        self, R1: Segment, L1: Segment, R2: Segment,
        L2_part: Part, R2_rem_len: float
    ) -> List[Part]:
        """Fallback plan for b=2."""
        parts_A: List[Part] = []

        part_R1_A = Part(pieces=[PartSegment(R1, 0.0, R1.length)])
        parts_A.append(part_R1_A)

        p_L1_A = PartSegment(L1, 0.0, L1.length)
        if R2_rem_len > 0:
            p_R2_rem_A = PartSegment(R2, 0.0, R2_rem_len)
            part_L1_A = Part(pieces=[p_L1_A, p_R2_rem_A])
        else:
            part_L1_A = Part(pieces=[p_L1_A])

        if part_L1_A.size < self.tau:
            need = self.tau - part_L1_A.size
            borrow = min(need, R1.length)
            if borrow > 0:
                p_R1_tail = PartSegment(seg=R1, start=R1.length - borrow, length=borrow)
                part_L1_A.pieces.insert(0, p_R1_tail)
                parts_A[-1] = Part(pieces=[PartSegment(R1, 0.0, R1.length - borrow)])

        parts_A.append(part_L1_A)
        parts_A.append(L2_part)

        cost_A = sum(p.fetch_cost for p in parts_A)

        parts_B: List[Part] = []

        p_R1_B = PartSegment(R1, 0.0, R1.length)
        p_L1_B = PartSegment(L1, 0.0, L1.length)
        part_L1_B = Part(pieces=[p_R1_B, p_L1_B])

        if part_L1_B.size < self.tau:
            need = self.tau - part_L1_B.size
            borrow = min(need, R2_rem_len)
            if borrow > 0:
                p_R2_prefix = PartSegment(seg=R2, start=0.0, length=borrow)
                part_L1_B.pieces.append(p_R2_prefix)
                R2_rem_len -= borrow

        parts_B.append(part_L1_B)

        if R2_rem_len > 0:
            part_R2_B = Part(pieces=[PartSegment(R2, 0.0, R2_rem_len)])
            parts_B.append(part_R2_B)

        parts_B.append(L2_part)

        cost_B = sum(p.fetch_cost for p in parts_B)

        return parts_A if cost_A <= cost_B else parts_B

@dataclass
class SegView:
    seg: Segment
    start: float
    length: float

    @property
    def kind(self):
        return self.seg.kind


class BoundaryAwareB2Planner:
    def __init__(
        self,
        normalized: Sequence[Segment],
        tau: float,
        *,
        max_copy_chunk: int = 128 * 1024 * 1024,
    ):
        self.views = [SegView(s, 0.0, float(s.length)) for s in normalized if s.length > 0]
        self.tau = float(tau)
        self.max_copy_chunk = float(max_copy_chunk)

    def plan(self, ptas2b: PTAS2B) -> List[Part]:
        views = list(self.views)
        if not views:
            return []

        parts: List[Part] = []

        prefix_parts, views = self._peel_leading_local_prefix(views)
        parts.extend(prefix_parts)

        if not views:
            return self._drop_empty_parts(parts)

        block_ranges = self._find_rlrl_view_blocks(views)

        if len(block_ranges) <= 2:
            parts.extend(self._plan_boundary_views(views, is_final=True))
            return self._drop_empty_parts(parts)

        interior_blocks = block_ranges[1:-1]
        cur = 0

        for start, end in interior_blocks:
            if cur < start:
                parts.extend(self._plan_boundary_views(views[cur:start], is_final=False))

            block = views[start:end]
            if self._can_call_ptas2b(block):
                R1, L1, R2, L2 = [v.seg for v in block]
                parts.extend(ptas2b.optimize(R1, L1, R2, L2))
            else:
                parts.extend(self._plan_boundary_views(block, is_final=False))

            cur = end

        if cur < len(views):
            parts.extend(self._plan_boundary_views(views[cur:], is_final=True))

        return self._drop_empty_parts(parts)

    def _peel_leading_local_prefix(
        self,
        views: List[SegView],
    ) -> Tuple[List[Part], List[SegView]]:
        if not views:
            return [], []

        if views[0].kind != "local":
            return [], views

        if len(views) == 1:
            return [self._part_from_view_slices([(views[0], 0.0, views[0].length)])], []

        pieces = []
        remaining = []
        need = self.tau
        idx = 0

        while idx < len(views) and need > 0:
            v = views[idx]
            take = min(v.length, need)

            if take > 0:
                pieces.append((v, 0.0, take))
                need -= take

            if take < v.length:
                remaining.append(SegView(v.seg, v.start + take, v.length - take))
                remaining.extend(views[idx + 1:])
                break

            idx += 1

        if idx >= len(views):
            remaining = []

        return [self._part_from_view_slices(pieces)], remaining

    def _plan_boundary_views(self, views: Sequence[SegView], *, is_final: bool) -> List[Part]:
        work = [v for v in views if v.length > 0]
        parts: List[Part] = []

        while work:
            if is_final and self._total_view_len(work) <= self.tau:
                parts.append(self._part_from_full_views(work))
                break

            first = work[0]

            if first.kind == "remote":
                if len(work) == 1:
                    copy_parts, tail = self._split_remote_view_for_copy(
                        first,
                        has_following=False,
                    )
                    parts.extend(copy_parts)
                    if tail is not None:
                        parts.append(self._part_from_full_views([tail]))
                    break

                copy_parts, tail = self._split_remote_view_for_copy(
                    first,
                    has_following=True,
                )
                parts.extend(copy_parts)

                if tail is None:
                    work = work[1:]
                    continue

                work[0] = tail
                mixed, work = self._consume_front_until_tau(
                    work,
                    allow_small_final=is_final and self._total_view_len(work) <= self.tau,
                )
                parts.append(mixed)
                continue

            mixed, work = self._consume_front_until_tau(
                work,
                allow_small_final=is_final and self._total_view_len(work) <= self.tau,
            )
            parts.append(mixed)

        return self._drop_empty_parts(parts)

    def _consume_front_until_tau(
        self,
        views: List[SegView],
        *,
        allow_small_final: bool,
    ) -> Tuple[Part, List[SegView]]:
        pieces = []
        remaining: List[SegView] = []

        target = 0.0 if allow_small_final else self.tau
        cur_size = 0.0
        idx = 0

        while idx < len(views):
            v = views[idx]

            if not allow_small_final and cur_size >= target:
                remaining.extend(views[idx:])
                break

            if allow_small_final:
                take = v.length
            else:
                take = min(v.length, target - cur_size)

            if take > 0:
                pieces.append((v, 0.0, take))
                cur_size += take

            if take < v.length:
                remaining.append(SegView(v.seg, v.start + take, v.length - take))
                remaining.extend(views[idx + 1:])
                break

            idx += 1

        return self._part_from_view_slices(pieces), remaining

    def _split_remote_view_for_copy(
        self,
        view: SegView,
        *,
        has_following: bool,
    ) -> Tuple[List[Part], Optional[SegView]]:
        parts: List[Part] = []
        cur = 0.0
        remain = view.length

        while remain > 0:
            chunk = min(self.max_copy_chunk, remain)

            if has_following and chunk == remain and chunk < self.tau:
                return parts, SegView(view.seg, view.start + cur, remain)

            parts.append(
                self._part_from_view_slices([(view, cur, chunk)])
            )

            cur += chunk
            remain -= chunk

        return parts, None

    def _part_from_view_slices(self, slices: Sequence[Tuple[SegView, float, float]]) -> Part:
        return Part(
            pieces=[
                PartSegment(
                    seg=v.seg,
                    start=v.start + rel_start,
                    length=length,
                )
                for v, rel_start, length in slices
                if length > 0
            ]
        )

    def _part_from_full_views(self, views: Sequence[SegView]) -> Part:
        return self._part_from_view_slices([(v, 0.0, v.length) for v in views])

    def _find_rlrl_view_blocks(self, views: Sequence[SegView]) -> List[Tuple[int, int]]:
        blocks: List[Tuple[int, int]] = []
        i = 0
        while i + 3 < len(views):
            block = views[i:i + 4]
            if self._is_rlrl_views(block):
                blocks.append((i, i + 4))
                i += 4
            else:
                i += 1
        return blocks

    @staticmethod
    def _is_rlrl_views(block: Sequence[SegView]) -> bool:
        return (
            len(block) == 4
            and block[0].kind == "remote"
            and block[1].kind == "local"
            and block[2].kind == "remote"
            and block[3].kind == "local"
        )

    @staticmethod
    def _can_call_ptas2b(block: Sequence[SegView]) -> bool:
        # PTAS2B can only consume complete original segments because it creates
        # PartSegment(start=...) relative to the original segment.
        return (
            len(block) == 4
            and BoundaryAwareB2Planner._is_rlrl_views(block)
            and all(v.start == 0.0 and v.length == v.seg.length for v in block)
        )

    @staticmethod
    def _total_view_len(views: Sequence[SegView]) -> float:
        return sum(v.length for v in views)

    @staticmethod
    def _drop_empty_parts(parts: Sequence[Part]) -> List[Part]:
        return [p for p in parts if p.pieces and p.size > 0]
