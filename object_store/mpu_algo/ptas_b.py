from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional
import math
from object_store.mpu_algo.segment import Segment, Part, PartSegment


EPS = 1e-9


@dataclass(frozen=True)
class Borrow:
    left: float    # from Ri tail to Li (x)
    right: float   # from R{i+1} head to Li (y)


class PTASB:
    """
    General PTAS-b planner, where b may be 2, 3, 4, and so on.

    Input must be an alternating [R1,L1,R2,L2,...,Rb,Lb] sequence.
    Semantics match PTAS-2B:
    1. Li may borrow x from the tail of Ri and y from the head of R{i+1}.
    2. Any remote-only part must have size >= tau.
    3. Any Ri leftover must be either zero or >= tau after borrowed prefixes
       and tails are removed.
    4. Swallow/bridge is allowed: Li + R{i+1} + L{i+1} can become one local
       part, with fetch cost covering the swallowed remote bytes.
    """

    def __init__(self, tau: float):
        self.tau = tau

    # -------------------------
    # small candidate generator
    # -------------------------
    def _cand_amounts(self, need: float, cap: float) -> List[float]:
        """Generate boundary candidates: 0, need, cap, and cap - tau."""
        tau = self.tau
        cands = {0.0}
        if need > EPS:
            cands.add(min(need, cap))
        cands.add(max(0.0, min(cap, cap)))  # cap
        # leave >=tau
        if cap - tau > EPS:
            cands.add(cap - tau)
        # clamp + dedup
        out = []
        for v in cands:
            v = max(0.0, min(cap, v))
            out.append(v)
        out = sorted(set(round(x, 9) for x in out))
        return [float(x) for x in out]

    def _ok_remote_leftover(self, rem: float) -> bool:
        """A remote-only leftover must be zero or at least tau."""
        tau = self.tau
        return rem <= EPS or rem + EPS >= tau

    # -------------------------
    # Main DP (with swallow)
    # State: (i, prefix_taken) where i is local index
    # We process Li (0-based), with Ri is same index.
    # -------------------------
    def optimize_dp(self, segments: List[Segment]) -> List[Part]:
        assert len(segments) % 2 == 0
        b = len(segments) // 2
        R = [segments[2 * i] for i in range(b)]
        L = [segments[2 * i + 1] for i in range(b)]

        for i in range(b):
            assert R[i].kind == "remote"
            assert L[i].kind == "local"

        tau = self.tau

        # memo: (i, prefix_taken_on_Ri) -> (min_cost, decision)
        # decision encodes:
        #   kind="normal": (x, y) and go to (i+1, y)
        #   kind="swallow": swallow R{i+1} and L{i+1} into Li's part, and go to (i+2, 0)
        memo: Dict[Tuple[int, float], Tuple[float, Tuple]] = {}

        def solve(i: int, prefix: float) -> float:
            key = (i, round(prefix, 9))
            if key in memo:
                return memo[key][0]

            # finished
            if i >= b:
                return 0.0

            # invalid prefix
            if prefix < -EPS or prefix > R[i].length + EPS:
                return math.inf

            best_cost = math.inf
            best_dec = None

            # ---------- Option A: normal part for Li ----------
            # Li may take x from Ri tail, y from R{i+1} head
            need_i = max(0.0, tau - L[i].length)

            left_cap = R[i].length - prefix
            if left_cap < -EPS:
                left_cap = -1.0

            has_right = (i + 1 < b)
            right_len = R[i + 1].length if has_right else 0.0

            # enumerate x candidates
            if left_cap >= -EPS:
                x_cands = self._cand_amounts(need_i, max(0.0, left_cap))
                # enumerate y candidates (only if has_right)
                y_cands = self._cand_amounts(max(0.0, need_i), right_len) if has_right else [0.0]

                for x in x_cands:
                    # current Ri leftover (middle) after consuming prefix + x
                    cur_leftover = R[i].length - prefix - x
                    if not self._ok_remote_leftover(cur_leftover):
                        continue

                    for y in y_cands:
                        if not has_right and y > EPS:
                            continue

                        # Li must be >= tau
                        if x + L[i].length + y + EPS < tau:
                            continue

                        # window semantic: y is from head of R{i+1}, and next state's prefix is y
                        # But we must also ensure R{i+1} will be feasible later:
                        # If next step consumes tail x_{i+1}, then R{i+1} leftover is (R{i+1} - y - x_{i+1}),
                        # which DP will check when i+1 is processed.
                        # Here we only require: after taking prefix=y later, it's within range -> ensured.

                        sub = solve(i + 1, y if has_right else 0.0)
                        if sub == math.inf:
                            continue

                        cost_here = x + y  # remote bytes included in this local part
                        total = cost_here + sub
                        if total + EPS < best_cost:
                            best_cost = total
                            best_dec = ("normal", x, y)

            # ---------- Option B: swallow (Li + R{i+1} + L{i+1}) ----------
            # This is the generalized PTAS-2B case1: we make ONE local part that spans across the whole R{i+1}.
            # Only meaningful if i+1 exists.
            if i + 1 < b:
                # Li part will include:
                #   - optionally x from Ri tail (still allowed, but not necessary if Li already >=tau)
                #   - whole remaining of R{i+1} (from 0 to R{i+1}.len), i.e., y = R{i+1}.len
                #   - L{i+1} fully
                # We also must ensure Ri leftover is legal (0 or >=tau).
                need_sw = max(0.0, tau - (L[i].length + R[i + 1].length + L[i + 1].length))
                # Usually need_sw is 0, but keep it correct: if still <tau, we can borrow from Ri tail by x.
                left_cap = R[i].length - prefix
                if left_cap >= -EPS:
                    x_cands = self._cand_amounts(need_sw, max(0.0, left_cap))
                    for x in x_cands:
                        cur_leftover = R[i].length - prefix - x
                        if not self._ok_remote_leftover(cur_leftover):
                            continue

                        # swallow includes entire R{i+1} => no remote-only part for R{i+1} at all
                        part_size = x + L[i].length + R[i + 1].length + L[i + 1].length
                        if part_size + EPS < tau:
                            continue

                        sub = solve(i + 2, 0.0)  # because R{i+1} is fully swallowed, next prefix for R{i+2} is 0
                        if sub == math.inf:
                            continue

                        cost_here = x + R[i + 1].length  # remote bytes inside this merged local part
                        total = cost_here + sub
                        if total + EPS < best_cost:
                            best_cost = total
                            best_dec = ("swallow", x)

            memo[key] = (best_cost, best_dec)
            return best_cost

        best = solve(0, 0.0)
        if best == math.inf:
            return self._fallback(segments)

        # -------- reconstruct decisions --------
        decisions: List[Tuple] = []
        i = 0
        prefix = 0.0
        while i < b:
            _, dec = memo[(i, round(prefix, 9))]
            if dec is None:
                return self._fallback(segments)
            decisions.append((i, prefix, dec))
            if dec[0] == "normal":
                _, x, y = dec
                prefix = y if (i + 1 < b) else 0.0
                i += 1
            else:
                # swallow
                _, x = dec
                prefix = 0.0
                i += 2

        # -------- build parts (consume, no overlap) --------
        parts = self._build_from_decisions(R, L, decisions)
        if parts is None:
            return self._fallback(segments)
        return parts

    # -------------------------
    # Builder: strictly consume each Ri region once.
    # - normal: local part uses Ri tail x and R{i+1} head y
    # - swallow: local part uses Ri tail x and ENTIRE R{i+1}, and includes L{i+1}
    # Remote-only parts are the leftover middles of Ri that are >= tau.
    # -------------------------
    def _build_from_decisions(
        self,
        R: List[Segment],
        L: List[Segment],
        decisions: List[Tuple[int, float, Tuple]],
    ) -> Optional[List[Part]]:
        tau = self.tau
        b = len(L)

        # track for each Ri: how much prefix is taken by left local, and how much tail by right local
        prefix = [0.0] * b
        tail = [0.0] * b
        swallowed = [False] * b  # swallowed[i]=True means Ri is fully included in a merged local part (as bridging remote)

        # fill prefix from decisions (normal uses y as next prefix)
        # prefix[i] is head taken from Ri by previous normal local (i-1)
        # We'll compute by simulation.
        i = 0
        cur_prefix = 0.0
        while i < b:
            prefix[i] = cur_prefix
            dec = None
            # find decision for this i
            for (ii, pref, d) in decisions:
                if ii == i and abs(pref - cur_prefix) <= 1e-6:
                    dec = d
                    break
            if dec is None:
                return None

            if dec[0] == "normal":
                _, x, y = dec
                tail[i] = x
                # next prefix
                cur_prefix = y if (i + 1 < b) else 0.0
                i += 1
            else:
                # swallow: consumes full R{i+1}
                _, x = dec
                tail[i] = x
                swallowed[i + 1] = True  # R{i+1} is fully taken
                cur_prefix = 0.0
                i += 2

        # validate leftovers for non-swallowed remotes
        for k in range(b):
            if swallowed[k]:
                continue
            if prefix[k] + tail[k] > R[k].length + EPS:
                return None
            mid = R[k].length - prefix[k] - tail[k]
            if mid > EPS and mid + EPS < tau:
                return None  # forbidden

        # Build local parts and remote-only parts in original order.
        parts: List[Part] = []

        def emit_remote_mid(k: int):
            if swallowed[k]:
                return
            mid_start = prefix[k]
            mid_end = R[k].length - tail[k]
            mid_len = mid_end - mid_start
            if mid_len <= EPS:
                return
            if mid_len + EPS < tau:
                # forbidden by construction
                return
            parts.append(Part([PartSegment(R[k], mid_start, mid_len)]))

        # Walk through locals; swallow means we emit one merged local part and skip next local.
        i = 0
        while i < b:
            # emit Ri mid BEFORE local that uses its tail (matches PTAS2B print style: remote then local)
            emit_remote_mid(i)

            # find decision for i with its prefix
            dec = None
            for (ii, pref, d) in decisions:
                if ii == i:
                    dec = d
                    break
            if dec is None:
                return None

            if dec[0] == "normal":
                _, x, y = dec
                pieces: List[PartSegment] = []
                if x > EPS:
                    pieces.append(PartSegment(R[i], R[i].length - x, x))
                pieces.append(PartSegment(L[i], 0.0, L[i].length))
                if i + 1 < b and y > EPS:
                    pieces.append(PartSegment(R[i + 1], 0.0, y))
                p = Part(pieces)
                if p.size + EPS < tau:
                    return None
                parts.append(p)
                i += 1
            else:
                # swallow: Li + R{i+1} + L{i+1}
                _, x = dec
                if i + 1 >= b:
                    return None
                pieces: List[PartSegment] = []
                if x > EPS:
                    pieces.append(PartSegment(R[i], R[i].length - x, x))
                pieces.append(PartSegment(L[i], 0.0, L[i].length))
                pieces.append(PartSegment(R[i + 1], 0.0, R[i + 1].length))
                pieces.append(PartSegment(L[i + 1], 0.0, L[i + 1].length))
                p = Part(pieces)
                if p.size + EPS < tau:
                    return None
                parts.append(p)
                i += 2

        # NOTE: last remote mid for R[b-1] already emitted in loop at i=b-1.
        return parts

    # -------------------------
    # Fallback: merge sequentially until tau is satisfied.
    # -------------------------
    def _fallback(self, segments: List[Segment]) -> List[Part]:
        tau = self.tau
        parts: List[Part] = []
        cur: List[PartSegment] = []
        size = 0.0
        for s in segments:
            cur.append(PartSegment(s, 0.0, s.length))
            size += s.length
            if size + EPS >= tau:
                parts.append(Part(cur))
                cur = []
                size = 0.0
        if cur:
            if not parts:
                parts.append(Part(cur))
            else:
                parts[-1].pieces.extend(cur)
        return parts
