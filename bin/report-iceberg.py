#!/usr/bin/env python3
"""Report only paired, validated measurements; retain every raw observation alongside them."""
import collections
import json
import math
from pathlib import Path
import statistics
import sys

root = Path(sys.argv[1])
method = json.loads((root / 'method.json').read_text())
rows = [json.loads(line) for line in (root / 'observations.jsonl').read_text().splitlines()]
paired = {(row['query'], row['mode'], row['variant'], row['trial'])
          for row in rows if row['kind'] == 'parity' and row['matched']}
groups = collections.defaultdict(lambda: collections.defaultdict(list))
for row in rows:
    if row['kind'] != 'measurement' or row['warmup']:
        continue
    key = (row['query'], row['mode'], row['variant'])
    if (*key, row['trial']) not in paired:
        raise SystemExit(f'Unpaired measurement: {key}, trial {row["trial"]}')
    groups[key][row['engine']].append(row)
if 'expectedCells' in method and set(groups) != {tuple(cell) for cell in method['expectedCells']}:
    raise SystemExit('Missing or unexpected query/mode/variant cells')
lines = ['# Streaming Iceberg Nexmark observations', '',
         f'{method["events"]:,} deterministic events; parallelism {method["parallelism"]}; '
         'four-bucket input/output tables. All times include streaming completion and final drain/commit.', '',
         '| Query | State / mini-batch | Variant | Flink median [range], s | StreamFusion median [range], s | Median ratio |',
         '|---|---|---|---:|---:|---:|']
for key, engines in sorted(groups.items(), key=lambda item: (int(item[0][0][1:]), item[0][1:])):
    samples = {}
    for engine in ('flink', 'streamfusion'):
        observations = sorted(engines[engine], key=lambda row: row['trial'])
        if [row['trial'] for row in observations] != list(range(method['runs'])):
            raise SystemExit(f'Incomplete measured repetitions: {key}, {engine}')
        samples[engine] = [row['seconds'] for row in observations]
    def cell(values):
        return f'{statistics.median(values):.3f} [{min(values):.3f}–{max(values):.3f}]'
    ratio = statistics.median(samples['flink']) / statistics.median(samples['streamfusion'])
    lines.append(f'| {key[0]} | {key[1]} | {key[2]} | {cell(samples["flink"])} | {cell(samples["streamfusion"])} | {ratio:.2f}× |')
lines += ['', '## Exact-query aggregates', '',
          'Geometric means use every exact-query cell in each mode; labelled expression variants '
          'remain separate above. Throughput is nominal corpus events divided by median end-to-end '
          'time, including startup, checkpoints and drain.', '',
          '| State / mini-batch | Queries | Geomean ratio | Flink corpus events/s, range | StreamFusion corpus events/s, range |',
          '|---|---:|---:|---:|---:|']
for mode in sorted({key[1] for key in groups}):
    ratios = []
    throughput = {'flink': [], 'streamfusion': []}
    for key, engines in groups.items():
        if key[1] != mode or key[2] != 'exact':
            continue
        medians = {engine: statistics.median(row['seconds'] for row in engines[engine])
                   for engine in throughput}
        ratios.append(medians['flink'] / medians['streamfusion'])
        for engine in throughput:
            throughput[engine].append(method['events'] / medians[engine])
    if ratios:
        gm = math.exp(statistics.mean(math.log(ratio) for ratio in ratios))
        def rate(engine):
            values = throughput[engine]
            return f'{min(values):,.0f}–{max(values):,.0f}'
        lines.append(f'| {mode} | {len(ratios)} | {gm:.2f}× | {rate("flink")} | {rate("streamfusion")} |')
if method['runs'] >= 2:
    lines += ['', '## Headline sampling convention', '',
              'Memory uses the first measured sample; disk uses the best of the first two. '
              'The repeated medians and complete ranges above remain the primary results.', '',
              '| Query | State / mini-batch | Variant | Flink, s | StreamFusion, s | Ratio |',
              '|---|---|---|---:|---:|---:|']
    for key, engines in sorted(groups.items(), key=lambda item: (int(item[0][0][1:]), item[0][1:])):
        selected = {}
        for engine in ('flink', 'streamfusion'):
            values = [row['seconds'] for row in sorted(engines[engine], key=lambda row: row['trial'])]
            selected[engine] = min(values[:2]) if key[1].startswith('disk') else values[0]
        ratio = selected['flink'] / selected['streamfusion']
        lines.append(f'| {key[0]} | {key[1]} | {key[2]} | {selected["flink"]:.3f} | {selected["streamfusion"]:.3f} | {ratio:.2f}× |')
lines += ['', 'Raw samples, warmups, parity checks, plans, committed table locations and native/fallback '
          'task/file witnesses are retained in this artifact directory. PROCTIME q12 validates per-bidder '
          'count conservation and includes waiting for the final processing-time window.', '']
(root / 'results.md').write_text('\n'.join(lines))
print('\n'.join(lines))
