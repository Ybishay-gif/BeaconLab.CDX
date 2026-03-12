/**
 * Plan Outcome Service — groups PE recommended testing points into outcome groups.
 *
 * Algorithm: Greedy rectangle packing with strict state+channel uniqueness.
 * Each state and each channel may appear in at most ONE outcome group.
 * Pairs that can't fit into a rectangle become individual remainder rows.
 */

import type { PriceExplorationRow } from "./analyticsService.js";

/* ── Types ─────────────────────────────────────────────────────────── */

export interface OutcomeGroup {
  group_id: number;
  testing_point: number;
  states: string[];
  channels: string[];
  pair_count: number;
  bids: number;
  sold: number;
  binds: number;
  win_rate: number | null;
  cpc: number | null;
  expected_cpb: number | null;
  current_cpb: number | null;
  additional_clicks: number;
  additional_binds: number;
  additional_budget: number;
  win_rate_uplift: number | null;
  cpc_uplift: number | null;
  cpb_uplift: number | null;
  stat_sig_summary: string;
}

export interface PlanOutcomeResult {
  groups: OutcomeGroup[];
  remainder: OutcomeGroup[];
  summary: {
    total_groups: number;
    total_remainder: number;
    total_states: number;
    total_channels: number;
    total_pairs: number;
  };
  generated_at: string;
}

/* ── Helpers ───────────────────────────────────────────────────────── */

interface RecommendedPair {
  state: string;
  channel: string;
  tp: number;
  row: PriceExplorationRow;
}

function weightedAvg(
  items: { value: number | null; weight: number }[]
): number | null {
  let sumW = 0;
  let sumVW = 0;
  for (const { value, weight } of items) {
    if (value != null && weight > 0) {
      sumVW += value * weight;
      sumW += weight;
    }
  }
  return sumW > 0 ? sumVW / sumW : null;
}

function aggregateGroup(
  groupId: number,
  tp: number,
  states: string[],
  channels: string[],
  rows: PriceExplorationRow[]
): OutcomeGroup {
  let bids = 0,
    sold = 0,
    binds = 0,
    additionalClicks = 0,
    additionalBinds = 0,
    additionalBudget = 0;

  const wrItems: { value: number | null; weight: number }[] = [];
  const cpcItems: { value: number | null; weight: number }[] = [];
  const cpbItems: { value: number | null; weight: number }[] = [];
  const currentCpbItems: { value: number | null; weight: number }[] = [];
  const wrUpliftItems: { value: number | null; weight: number }[] = [];
  const cpcUpliftItems: { value: number | null; weight: number }[] = [];
  const cpbUpliftItems: { value: number | null; weight: number }[] = [];

  const statSigSet = new Set<string>();

  for (const r of rows) {
    bids += r.bids;
    sold += r.sold;
    binds += r.binds;
    additionalClicks += r.additional_clicks ?? 0;
    additionalBinds += r.expected_bind_change ?? 0;
    additionalBudget += r.additional_budget_needed ?? 0;

    wrItems.push({ value: r.win_rate, weight: r.bids });
    cpcItems.push({ value: r.cpc, weight: r.sold });
    cpbItems.push({ value: r.expected_cpb, weight: r.binds });
    currentCpbItems.push({ value: r.current_cpb, weight: r.binds });
    wrUpliftItems.push({ value: r.win_rate_uplift, weight: r.bids });
    cpcUpliftItems.push({ value: r.cpc_uplift, weight: r.sold });
    cpbUpliftItems.push({ value: r.cpb_uplift, weight: r.binds });

    if (r.stat_sig) statSigSet.add(r.stat_sig);
  }

  let statSigSummary: string;
  if (statSigSet.size === 1) {
    statSigSummary = [...statSigSet][0];
  } else if (statSigSet.size === 0) {
    statSigSummary = "unknown";
  } else {
    statSigSummary = "mixed";
  }

  return {
    group_id: groupId,
    testing_point: tp,
    states: [...states].sort(),
    channels: [...channels].sort(),
    pair_count: states.length * channels.length,
    bids,
    sold,
    binds,
    win_rate: weightedAvg(wrItems),
    cpc: weightedAvg(cpcItems),
    expected_cpb: weightedAvg(cpbItems),
    current_cpb: weightedAvg(currentCpbItems),
    additional_clicks: additionalClicks,
    additional_binds: additionalBinds,
    additional_budget: additionalBudget,
    win_rate_uplift: weightedAvg(wrUpliftItems),
    cpc_uplift: weightedAvg(cpcUpliftItems),
    cpb_uplift: weightedAvg(cpbUpliftItems),
    stat_sig_summary: statSigSummary,
  };
}

/* ── Main ──────────────────────────────────────────────────────────── */

export function buildPlanOutcome(
  peRows: PriceExplorationRow[]
): PlanOutcomeResult {
  // 1. Filter to recommended rows only
  const recommended: RecommendedPair[] = [];
  for (const r of peRows) {
    if (
      r.recommended_testing_point != null &&
      r.recommended_testing_point !== 0 &&
      r.testing_point === r.recommended_testing_point
    ) {
      recommended.push({
        state: r.state,
        channel: r.channel_group_name,
        tp: r.recommended_testing_point,
        row: r,
      });
    }
  }

  // 2. Build lookup: "state|channel" → row for recommended TP
  const pairRowMap = new Map<string, PriceExplorationRow>();
  for (const p of recommended) {
    pairRowMap.set(`${p.state}|${p.channel}`, p.row);
  }

  // 3. Group by TP, sort by pair count descending
  const byTP = new Map<number, RecommendedPair[]>();
  for (const p of recommended) {
    if (!byTP.has(p.tp)) byTP.set(p.tp, []);
    byTP.get(p.tp)!.push(p);
  }
  const sortedTPs = [...byTP.entries()].sort(
    (a, b) => b[1].length - a[1].length
  );

  // 4. Greedy rectangle packing
  const assignedStates = new Set<string>();
  const assignedChannels = new Set<string>();
  const groups: OutcomeGroup[] = [];
  let nextGroupId = 1;

  for (const [tp, pairs] of sortedTPs) {
    // Keep extracting rectangles from this TP until no more available pairs
    let changed = true;
    while (changed) {
      changed = false;

      // Filter to pairs where both state AND channel are unassigned
      const available = pairs.filter(
        (p) => !assignedStates.has(p.state) && !assignedChannels.has(p.channel)
      );
      if (available.length === 0) break;

      // Build state → set of available channels
      const stateChannels = new Map<string, Set<string>>();
      for (const p of available) {
        if (!stateChannels.has(p.state))
          stateChannels.set(p.state, new Set());
        stateChannels.get(p.state)!.add(p.channel);
      }

      // Group states by their channel-set signature
      const sigGroups = new Map<string, string[]>();
      for (const [state, channels] of stateChannels) {
        const sig = [...channels].sort().join("|");
        if (!sigGroups.has(sig)) sigGroups.set(sig, []);
        sigGroups.get(sig)!.push(state);
      }

      // Find the signature with largest area (states × channels)
      let bestSig = "";
      let bestArea = 0;
      for (const [sig, states] of sigGroups) {
        const channelCount = sig.split("|").length;
        const area = states.length * channelCount;
        if (area > bestArea) {
          bestArea = area;
          bestSig = sig;
        }
      }

      if (bestArea <= 0) break;

      const groupStates = sigGroups.get(bestSig)!;
      const groupChannels = bestSig.split("|");

      // Collect the PE rows for this rectangle
      const groupRows: PriceExplorationRow[] = [];
      for (const s of groupStates) {
        for (const c of groupChannels) {
          const row = pairRowMap.get(`${s}|${c}`);
          if (row) groupRows.push(row);
        }
      }

      // Create the outcome group
      groups.push(
        aggregateGroup(nextGroupId++, tp, groupStates, groupChannels, groupRows)
      );

      // Mark states and channels as assigned
      for (const s of groupStates) assignedStates.add(s);
      for (const c of groupChannels) assignedChannels.add(c);
      changed = true;
    }
  }

  // 5. Collect remainder — pairs where state or channel was already assigned
  const remainder: OutcomeGroup[] = [];
  for (const p of recommended) {
    if (!assignedStates.has(p.state) || !assignedChannels.has(p.channel)) {
      // This pair's state or channel wasn't in any group (shouldn't happen
      // unless the pair was skipped). Check if it's truly unassigned.
      if (!assignedStates.has(p.state) && !assignedChannels.has(p.channel)) {
        // Both unassigned — this pair was somehow missed (shouldn't happen)
        remainder.push(
          aggregateGroup(nextGroupId++, p.tp, [p.state], [p.channel], [p.row])
        );
        assignedStates.add(p.state);
        assignedChannels.add(p.channel);
      }
    }
  }

  // Also handle pairs where one dimension is assigned but not both.
  // Under strict uniqueness, these pairs are "orphaned" — they can't form
  // new groups. We still list them as remainder for visibility.
  for (const p of recommended) {
    const inGroup = groups.some(
      (g) => g.states.includes(p.state) && g.channels.includes(p.channel)
    );
    const inRemainder = remainder.some(
      (g) => g.states.includes(p.state) && g.channels.includes(p.channel)
    );
    if (!inGroup && !inRemainder) {
      remainder.push(
        aggregateGroup(nextGroupId++, p.tp, [p.state], [p.channel], [p.row])
      );
    }
  }

  // Sort groups by testing_point ascending, then by pair_count descending
  groups.sort(
    (a, b) => a.testing_point - b.testing_point || b.pair_count - a.pair_count
  );
  remainder.sort(
    (a, b) => a.testing_point - b.testing_point || a.states[0].localeCompare(b.states[0])
  );

  // 6. Summary
  const allStates = new Set<string>();
  const allChannels = new Set<string>();
  for (const g of [...groups, ...remainder]) {
    for (const s of g.states) allStates.add(s);
    for (const c of g.channels) allChannels.add(c);
  }

  return {
    groups,
    remainder,
    summary: {
      total_groups: groups.length,
      total_remainder: remainder.length,
      total_states: allStates.size,
      total_channels: allChannels.size,
      total_pairs: recommended.length,
    },
    generated_at: new Date().toISOString(),
  };
}
