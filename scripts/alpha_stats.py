#!/usr/bin/env python3
"""Alpha stats for rh-canary results-*.jsonl.

只取 lag<=2 的 fresh 真实 route 样本（candidate 行），按 token 与 pool-pair
分组统计 gross / net_1x / net_2x / net_3x，输出 gross-positive 出现率、
最大/中位毛利。用法：

    python3 scripts/alpha_stats.py data/canary/results-*.jsonl [--min-lag N] [--top N]
"""
import argparse
import collections
import json
import sys

WETH = 10**18

def wei_to_weth(v):
    if v is None or v == "":
        return None
    return int(v) / WETH

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("files", nargs="+")
    ap.add_argument("--min-lag", type=int, default=2, help="只统计 lag<=N 的样本")
    ap.add_argument("--top", type=int, default=15)
    args = ap.parse_args()

    cands = []
    for f in args.files:
        with open(f) as fh:
            for line in fh:
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if rec.get("kind") == "candidate" or "route" in rec and "decision" in rec:
                    cands.append(rec)

    fresh = [c for c in cands if c.get("state_lag_blocks", 999) <= args.min_lag]
    # 一次评估 = 一个 (block, tx, log) 事件身份；候选行共享触发事件身份
    by_eval = collections.defaultdict(list)
    for c in fresh:
        by_eval[(c["block"], c["tx_hash"], c["log_index"])].append(c)

    print(f"candidate rows: {len(cands)}  fresh(lag<={args.min_lag}): {len(fresh)}  "
          f"fresh evaluations: {len(by_eval)}")

    evals = []
    for evid, rows in by_eval.items():
        prof = [r for r in rows if r.get("decision") == "local_profitable_observed"]
        gross = [int(r["gross_profit_wei"]) for r in prof if r.get("gross_profit_wei")]
        best = max(gross) if gross else 0
        net1 = [int(r["net1x_wei"]) for r in prof if r.get("net1x_wei")]
        net2 = [int(r["net2x_wei"]) for r in prof if r.get("net2x_wei")]
        net3 = [int(r["net3x_wei"]) for r in prof if r.get("net3x_wei")]
        evals.append({
            "id": evid,
            "token": rows[0].get("token", ""),
            "pools": rows[0].get("pool", ""),
            "routes": {r.get("route", "") for r in rows},
            "gross_pos": best > 0,
            "best_gross": best,
            "best_net1": max(net1) if net1 else 0,
            "best_net2": max(net2) if net2 else 0,
            "best_net3": max(net3) if net3 else 0,
            "n_profitable_routes": len(prof),
        })

    n = len(evals)
    if n == 0:
        print("no fresh evaluations")
        return
    gp = sum(1 for e in evals if e["gross_pos"])
    print(f"\ngross-positive evaluations: {gp}/{n} ({gp/n*100:.2f}%)")
    print(f"profitable route rows: {sum(e['n_profitable_routes'] for e in evals)}")

    def stats(evals, key):
        vals = sorted(e[key] for e in evals if e[key] > 0)
        if not vals:
            return "none"
        med = vals[len(vals)//2] if len(vals) % 2 else (vals[len(vals)//2-1]+vals[len(vals)//2])//2
        return f"n={len(vals)} max={wei_to_weth(max(vals)):.6f} WETH med={wei_to_weth(med):.6f} WETH"

    print(f"gross    : {stats(evals, 'best_gross')}")
    print(f"net_1x   : {stats(evals, 'best_net1')}")
    print(f"net_2x   : {stats(evals, 'best_net2')}")
    print(f"net_3x   : {stats(evals, 'best_net3')}")

    print(f"\n--- by token (top {args.top} by gross-positive count) ---")
    by_token = collections.defaultdict(list)
    for e in evals:
        by_token[e["token"]].append(e)
    for tok, es in sorted(by_token.items(), key=lambda kv: -sum(1 for e in kv[1] if e["gross_pos"]))[:args.top]:
        gp_t = sum(1 for e in es if e["gross_pos"])
        mx = max(e["best_gross"] for e in es)
        print(f"  {tok}: evals={len(es)} gross_pos={gp_t} ({gp_t/len(es)*100:.1f}%) "
              f"max_gross={wei_to_weth(mx):.6f}")

    print(f"\n--- by pool-pair (top {args.top}) ---")
    by_pair = collections.defaultdict(list)
    for e in evals:
        for r in e["routes"]:
            by_pair[r].append(e)
    for pr, es in sorted(by_pair.items(), key=lambda kv: -sum(1 for e in kv[1] if e["gross_pos"]))[:args.top]:
        gp_p = sum(1 for e in es if e["gross_pos"])
        mx = max(e["best_gross"] for e in es)
        print(f"  {pr}: evals={len(es)} gross_pos={gp_p} ({gp_p/len(es)*100:.1f}%) "
              f"max_gross={wei_to_weth(mx):.6f}")

if __name__ == "__main__":
    sys.exit(main())
