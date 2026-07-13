from typing import List

from common.logger import print_debug, print_info
from object_store.mpu_algo.ptas_prepare import PTASPrepare
from object_store.mpu_algo.greedy import GreedyBaseline
from object_store.mpu_algo.ptas_2b import (
    PTAS2B,
    BoundaryAwareB2Planner,
)
from object_store.mpu_algo.ptas_b import PTASB
from object_store.mpu_algo.segment import Segment, Part, PartSegment


class Executor:
    def __init__(self, lengths: List[float], tau: float, print_flag: bool = True):
        self.lengths = lengths
        self.tau = tau
        self.normalized = None
        self.raw = None
        self.block_ranges = None
        self.print_flag = print_flag
        self.max_copy_chunk = 128 * 1024 * 1024

    def prepare(self, b_num: int = None) -> tuple[List[tuple[int, int]], List[Segment]]:
        prepare = PTASPrepare(self.lengths, self.tau)
        if b_num is None:
            self.block_ranges, self.normalized, self.raw = prepare.ptas_prepare_with_b_num(2)
        else:
            self.block_ranges, self.normalized, self.raw = prepare.ptas_prepare_with_b_num(b_num)
        return self.block_ranges, self.normalized, self.raw

    def plan_with_greedy(self) -> List[Part]:
        greedy = GreedyBaseline(self.raw, self.tau)
        return greedy.greedy_baseline_plan()

    def plan_with_ptas(self, b_num: int = None) -> List[Part]:
        if b_num is None:
            ptas2b = PTAS2B(self.tau)
            return self.plan_with_b2(ptas2b)
        else:
            ptasb = PTASB(self.tau)
            return self.plan_with_b_num(b_num, ptasb)

    def plan_with_ptasb(self, b_num: int) -> List[Part]:
        ptasb = PTASB(self.tau)
        return self.plan_with_b_num(b_num, ptasb)

    def plan_with_b2(self, ptas2b: PTAS2B) -> List[Part]:
        planner = BoundaryAwareB2Planner(
            normalized=self.normalized,
            tau=self.tau,
            max_copy_chunk=self.max_copy_chunk,
        )
        return planner.plan(ptas2b)

    def plan_with_b_num(self, b_num: int, ptasb: PTASB) -> List[Part]:
        parts: List[Part] = []
        n = len(self.normalized)
        if n == 0:
            return parts

        cur = 0

        for (start, end) in self.block_ranges:
            if cur < start:
                leftover = self.normalized[cur:start]
                greedy = GreedyBaseline(leftover, self.tau)
                parts.extend(greedy.greedy_baseline_plan())

            segments = self.normalized[start:end]
            parts.extend(ptasb.optimize_dp(segments))

            cur = end

        if cur < n:
            leftover = self.normalized[cur:n]
            greedy = GreedyBaseline(leftover, self.tau)
            parts.extend(greedy.greedy_baseline_plan())

        return parts

    def print_plan(self, parts: List[Part]) -> tuple[float, float]:
        """
        Print a plan and return (total_cost, total_sent).

        total_cost is fetched remote MB. total_sent is total local-part MB,
        including payload and fetched bytes.
        """
        MB = 1024 * 1024
        if self.print_flag:
            print_debug("Plan:")
        for i, p in enumerate(parts):
            if self.print_flag:
                print_debug(
                    f"  Part {i}: size={p.size/1024/1024:.2f}, "
                    f"remote={p.remote_size/1024/1024:.2f}, "
                    f"fetch_cost={p.fetch_cost/MB:.6f}, "
                    f"type={'LOCAL' if p.is_local else 'REMOTE'}"
                )
            for ps in p.pieces:
                if ps.seg.original_segments and len(ps.seg.original_segments) > 1:
                    offset = 0.0
                    remaining = ps.length
                    start_pos = ps.start

                    for orig_name, orig_len in ps.seg.original_segments:
                        if start_pos < offset + orig_len:
                            orig_start = start_pos - offset
                            take = min(remaining, offset + orig_len - start_pos)
                            orig_end = orig_start + take
                            if self.print_flag:
                                print_debug(
                                    f"    - {orig_name} "
                                    f"[{orig_start/MB:.6f}, {orig_end/MB:.6f}) "
                                    f"len={take/MB:.6f}, kind={ps.seg.kind}"
                                )

                            remaining -= take
                            if remaining <= 0:
                                break
                            start_pos = offset + orig_len
                        offset += orig_len
                else:
                    start = int(ps.start) if ps.start == int(ps.start) else ps.start
                    end = int(ps.start + ps.length) if (ps.start + ps.length) == int(ps.start + ps.length) else ps.start + ps.length
                    if self.print_flag:
                        print_debug(
                            f"    - {ps.seg.name} "
                            f"[{start/MB:.6f}, {end/MB:.6f}) "
                            f"len={ps.length/MB:.6f}, kind={ps.seg.kind}"
                        )
        total_remote = sum(p.remote_size for p in parts) / MB
        total_cost = sum(p.fetch_cost for p in parts) / MB
        total_sent = sum(p.size for p in parts if p.is_local) / MB
        total_payload = total_sent - total_cost
        if self.print_flag:
            print_debug(f"Total remote MB (covered)   = {total_remote:.6f}")
            print_debug(f"Total fetched remote MB     = {total_cost:.6f}  (extra)")
            print_debug(f"Total sent MB (local parts) = {total_sent:.6f}  (=payload+fetched)")
            print_debug(f"Total payload MB            = {total_payload:.6f}")
            if self.normalized is not None:
                seg_local = sum(s.length for s in self.normalized if s.kind == "local") / MB
                seg_remote = sum(s.length for s in self.normalized if s.kind == "remote") / MB
                print_debug(f"Segments local/remote MB    = {seg_local:.6f} / {seg_remote:.6f}")
            print_debug("-" * 50)
        return (total_cost, total_sent)

if __name__ == "__main__":
    lengths=[5, 3, 3, 9, 3, 3, 14, 6, 3, 3, 6, 3, 26, 3, 9, 1]
    lengths = [float(l) * 1024 * 1024 for l in lengths]
    tau = 5.0 * 1024 * 1024
    executor = Executor(lengths, tau, True)
    block_ranges, normalized = executor.prepare()
    block_ranges2, normalized2 = executor.prepare(2)
    assert block_ranges == block_ranges2
    assert normalized == normalized2
    greedy_parts = executor.plan_with_greedy()
    ptas_parts = executor.plan_with_ptas()
    print_info("=" * 60)
    print_info("Greedy Baseline")
    print_info("=" * 60)
    executor.print_plan(greedy_parts)
    print_info("=" * 60)
    print_info("PTAS 2B")
    print_info("=" * 60)
    executor.print_plan(ptas_parts)

    print_info("=" * 60)
    print_info("PTAS B")
    print_info("=" * 60)
    ptasb_parts = executor.plan_with_ptas(2)
    executor.print_plan(ptasb_parts)
