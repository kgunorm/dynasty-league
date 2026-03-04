"""Generate a self-contained HTML dashboard from MDM pipeline output CSVs."""
from __future__ import annotations
import json
import os
import math
import pandas as pd
import yaml

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
CONFIG_PATH = os.path.join(BASE_DIR, "config.yaml")


def load_data():
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)

    golden = pd.read_csv(os.path.join(OUTPUT_DIR, "golden_records.csv"))
    match_report = pd.read_csv(os.path.join(OUTPUT_DIR, "match_report.csv"))
    conflicts = pd.read_csv(os.path.join(OUTPUT_DIR, "conflicts.csv"))
    rq_path = os.path.join(OUTPUT_DIR, "review_queue.csv")
    try:
        review_queue = pd.read_csv(rq_path)
    except Exception:
        review_queue = pd.DataFrame()

    # Load source records with normalized field names
    source_records = []
    for src in config["sources"]:
        path = os.path.join(BASE_DIR, src["path"])
        kwargs = {}
        if "sheet_name" in src:
            kwargs["sheet_name"] = src["sheet_name"]
        if path.endswith(".xlsx"):
            df = pd.read_excel(path, **kwargs)
        else:
            df = pd.read_csv(path)
        inv_map = {v: k for k, v in src["field_map"].items()}
        df = df.rename(columns=inv_map)
        df["_source_system"] = src["system_id"]
        df["_row_index"] = range(len(df))
        source_records.append(df)

    sources_df = pd.concat(source_records, ignore_index=True)

    # Merge match_report into sources to get golden_id per source record
    match_lookup = match_report[["source_record_id", "golden_id", "match_tier"]].copy()
    # match_report deduped already; use source_row_index + source_system to join
    # We need to link by source_system + source_row_index
    match_report2 = match_report.copy()
    sources_df["_join_key"] = (
        sources_df["_source_system"] + "_" + sources_df["_row_index"].astype(str)
    )
    match_report2["_join_key"] = (
        match_report2["source_system"] + "_" + match_report2["source_row_index"].astype(str)
    )
    join = match_report2[["_join_key", "golden_id", "match_tier", "composite_score"]].drop_duplicates("_join_key")
    sources_df = sources_df.merge(join, on="_join_key", how="left")

    # For sources not in match_report, look up their golden_id via golden_records._source_record_ids
    # (singletons) — handled by linking via golden._source_ids system matching
    # Singletons: match_tier NaN → singleton
    sources_df["match_tier"] = sources_df["match_tier"].fillna("SINGLETON")
    sources_df["composite_score"] = sources_df["composite_score"].fillna(0.0)

    # Assign golden_id to singletons from golden records
    singleton_golden_map = {}
    for _, g in golden.iterrows():
        src_ids = str(g.get("_source_ids", "")).split(",")
        src_rids = str(g.get("_source_record_ids", "")).split(",")
        if g["_source_count"] == 1:
            system = src_ids[0].strip()
            singleton_golden_map[system] = g["golden_id"]

    def fill_singleton_golden(row):
        if pd.isna(row["golden_id"]) or row["golden_id"] == "":
            return singleton_golden_map.get(row["_source_system"], "")
        return row["golden_id"]

    sources_df["golden_id"] = sources_df.apply(fill_singleton_golden, axis=1)

    return golden, match_report, conflicts, review_queue, sources_df, config


def safe_val(v):
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    if hasattr(v, "item"):
        return v.item()
    return v


def df_to_records(df):
    rows = []
    for _, row in df.iterrows():
        rows.append({k: safe_val(v) for k, v in row.items()})
    return rows


def build_json_data(golden, match_report, conflicts, review_queue, sources_df, config):
    # Overview stats
    total_sources = len(sources_df)
    total_golden = len(golden)
    total_merges = len(golden[golden["_source_count"] > 1])
    total_conflicts = len(conflicts)

    # Records per source system
    source_counts = sources_df["_source_system"].value_counts().to_dict()

    # Merge outcomes: merged vs singleton
    merged_count = int(golden[golden["_source_count"] > 1]["_source_count"].sum())
    singleton_count = int(len(golden[golden["_source_count"] == 1]))

    # Score distribution (only AUTO_MERGE pairs)
    auto_scores = match_report[match_report["match_tier"] == "AUTO_MERGE"]["composite_score"].dropna().tolist()

    # Build golden records list with contributing source info
    golden_rows = []
    for _, g in golden.iterrows():
        src_ids = [s.strip() for s in str(g.get("_source_ids", "")).split(",")]
        src_rids = [r.strip() for r in str(g.get("_source_record_ids", "")).split(",")]
        row = {
            "golden_id": g["golden_id"],
            "full_name": safe_val(g["full_name"]),
            "email": safe_val(g["email"]),
            "phone": safe_val(g["phone"]),
            "address": safe_val(g["address"]),
            "_source_count": int(g["_source_count"]),
            "_source_ids": src_ids,
            "_source_record_ids": src_rids,
            "_match_score": safe_val(g["_match_score"]),
            "_created_at": str(g["_created_at"]),
        }
        golden_rows.append(row)

    # Source records enriched
    source_rows = []
    for _, r in sources_df.iterrows():
        source_rows.append({
            "full_name": safe_val(r.get("full_name")),
            "email": safe_val(r.get("email")),
            "phone": safe_val(r.get("phone")),
            "address": safe_val(r.get("address")),
            "updated_at": str(safe_val(r.get("updated_at", ""))),
            "_source_system": str(r["_source_system"]),
            "_row_index": int(r["_row_index"]),
            "golden_id": safe_val(r.get("golden_id")),
            "match_tier": str(r.get("match_tier", "")),
            "composite_score": safe_val(r.get("composite_score")),
        })

    # Conflicts
    conflict_rows = df_to_records(conflicts) if len(conflicts) > 0 else []

    # Review queue
    review_rows = df_to_records(review_queue) if len(review_queue) > 0 else []

    # Per-golden source breakdown (for drill-down)
    # Map golden_id -> list of source record dicts
    golden_source_map = {}
    for g_row in golden_rows:
        gid = g_row["golden_id"]
        contribs = []
        for src in source_rows:
            if src.get("golden_id") == gid:
                contribs.append(src)
        golden_source_map[gid] = contribs

    return {
        "stats": {
            "total_sources": total_sources,
            "total_golden": total_golden,
            "total_merges": total_merges,
            "total_conflicts": total_conflicts,
        },
        "source_counts": source_counts,
        "merge_outcomes": {
            "Merged": merged_count,
            "Singleton": singleton_count,
        },
        "auto_scores": auto_scores,
        "golden_records": golden_rows,
        "source_records": source_rows,
        "conflicts": conflict_rows,
        "review_queue": review_rows,
        "golden_source_map": golden_source_map,
        "fields": ["full_name", "email", "phone", "address"],
        "config": {
            "thresholds": config["matching"]["thresholds"],
            "weights": config["matching"]["weights"],
        },
    }


HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>MDM Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.4/dist/chart.umd.min.js"></script>
<style>
  :root {
    --bg: #0a0f0b;
    --surface: #111a13;
    --surface2: #162019;
    --border: #1e3022;
    --green: #69f0ae;
    --cyan: #00e5ff;
    --amber: #ffd54f;
    --red: #ef5350;
    --purple: #ce93d8;
    --blue: #42a5f5;
    --orange: #ffa726;
    --text: #e8f5e9;
    --muted: #78909c;
    --win-bg: rgba(105,240,174,0.12);
    --override-bg: rgba(255,213,79,0.12);
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: 'Segoe UI', system-ui, sans-serif; font-size: 14px; }

  /* Nav */
  nav { background: var(--surface); border-bottom: 1px solid var(--border); display: flex; align-items: center; padding: 0 24px; gap: 4px; position: sticky; top: 0; z-index: 100; }
  nav .logo { color: var(--green); font-weight: 700; font-size: 16px; padding: 14px 16px 14px 0; border-right: 1px solid var(--border); margin-right: 8px; letter-spacing: 1px; }
  nav button { background: none; border: none; color: var(--muted); cursor: pointer; padding: 14px 16px; font-size: 13px; font-weight: 500; border-bottom: 2px solid transparent; transition: color .15s, border-color .15s; }
  nav button:hover { color: var(--text); }
  nav button.active { color: var(--green); border-bottom-color: var(--green); }

  /* Sections */
  .section { display: none; padding: 24px; max-width: 1400px; margin: 0 auto; }
  .section.active { display: block; }

  /* Cards */
  .stat-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 16px; margin-bottom: 28px; }
  .stat-card { background: var(--surface); border: 1px solid var(--border); border-radius: 10px; padding: 20px; }
  .stat-card .label { color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 8px; }
  .stat-card .value { font-size: 32px; font-weight: 700; color: var(--green); }
  .stat-card .sub { color: var(--muted); font-size: 11px; margin-top: 4px; }

  /* Charts row */
  .charts-row { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 16px; margin-bottom: 24px; }
  .chart-box { background: var(--surface); border: 1px solid var(--border); border-radius: 10px; padding: 20px; }
  .chart-box h3 { color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 16px; }
  .chart-box canvas { max-height: 240px; }

  /* Tables */
  .table-wrap { background: var(--surface); border: 1px solid var(--border); border-radius: 10px; overflow: hidden; }
  table { width: 100%; border-collapse: collapse; }
  thead th { background: var(--surface2); color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: 0.8px; padding: 10px 14px; text-align: left; border-bottom: 1px solid var(--border); cursor: pointer; user-select: none; white-space: nowrap; }
  thead th:hover { color: var(--text); }
  tbody tr { border-bottom: 1px solid var(--border); cursor: pointer; transition: background .1s; }
  tbody tr:last-child { border-bottom: none; }
  tbody tr:hover { background: var(--surface2); }
  tbody tr.selected { background: rgba(105,240,174,0.07); }
  td { padding: 10px 14px; vertical-align: middle; }

  /* Chips */
  .chip { display: inline-block; padding: 2px 8px; border-radius: 20px; font-size: 11px; font-weight: 600; margin: 1px; }
  .chip-crm { background: rgba(66,165,245,0.2); color: var(--blue); }
  .chip-marketing { background: rgba(206,147,216,0.2); color: var(--purple); }
  .chip-erp { background: rgba(255,167,38,0.2); color: var(--orange); }
  .chip-singleton { background: rgba(120,144,156,0.2); color: var(--muted); }
  .chip-merged { background: rgba(105,240,174,0.15); color: var(--green); }
  .chip-win { background: rgba(105,240,174,0.2); color: var(--green); font-size: 10px; }
  .chip-override { background: rgba(255,213,79,0.2); color: var(--amber); font-size: 10px; }

  /* Score badge */
  .score-badge { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 12px; font-weight: 700; }
  .score-high { background: rgba(105,240,174,0.15); color: var(--green); }
  .score-mid { background: rgba(255,213,79,0.15); color: var(--amber); }
  .score-none { background: rgba(120,144,156,0.15); color: var(--muted); }

  /* Filter bar */
  .filter-bar { display: flex; gap: 8px; align-items: center; margin-bottom: 16px; flex-wrap: wrap; }
  .filter-bar input { background: var(--surface); border: 1px solid var(--border); color: var(--text); border-radius: 6px; padding: 7px 12px; font-size: 13px; outline: none; min-width: 220px; }
  .filter-bar input:focus { border-color: var(--green); }
  .filter-bar button { background: var(--surface); border: 1px solid var(--border); color: var(--muted); cursor: pointer; border-radius: 6px; padding: 7px 14px; font-size: 12px; font-weight: 600; transition: all .15s; }
  .filter-bar button:hover, .filter-bar button.active { border-color: var(--green); color: var(--green); background: rgba(105,240,174,0.07); }

  /* Drill-down panel */
  #drill-panel { display: none; margin-top: 20px; background: var(--surface); border: 1px solid var(--green); border-radius: 10px; padding: 24px; }
  #drill-panel.open { display: block; }
  #drill-panel h2 { color: var(--green); font-size: 18px; margin-bottom: 4px; }
  #drill-panel .meta { color: var(--muted); font-size: 12px; margin-bottom: 20px; }
  #drill-close { float: right; background: none; border: 1px solid var(--border); color: var(--muted); cursor: pointer; padding: 4px 12px; border-radius: 4px; font-size: 12px; }
  #drill-close:hover { color: var(--text); }

  .drill-field-table { width: 100%; border-collapse: collapse; margin-top: 16px; }
  .drill-field-table th { background: var(--surface2); color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: 0.8px; padding: 8px 12px; text-align: left; border-bottom: 1px solid var(--border); }
  .drill-field-table td { padding: 10px 12px; border-bottom: 1px solid var(--border); font-size: 13px; vertical-align: middle; }
  .drill-field-table tr:last-child td { border-bottom: none; }
  td.cell-win { background: var(--win-bg); }
  td.cell-override { background: var(--override-bg); }
  td.cell-same { color: var(--muted); }
  .strike { text-decoration: line-through; color: var(--muted); }

  /* Empty state */
  .empty { text-align: center; padding: 48px; color: var(--muted); }
  .empty .icon { font-size: 36px; margin-bottom: 12px; }

  /* Section title */
  .section-title { font-size: 20px; font-weight: 700; margin-bottom: 20px; color: var(--text); }
  .section-title span { color: var(--green); }

  h3.sub-head { color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: 1px; margin: 24px 0 12px; }

  /* Conflict field colors */
  tr[data-field="full_name"] td:first-child { border-left: 3px solid var(--blue); }
  tr[data-field="email"] td:first-child { border-left: 3px solid var(--purple); }
  tr[data-field="phone"] td:first-child { border-left: 3px solid var(--green); }
  tr[data-field="address"] td:first-child { border-left: 3px solid var(--orange); }

  .review-pair { background: var(--surface); border: 1px solid var(--border); border-radius: 10px; padding: 20px; margin-bottom: 16px; }
  .review-pair .pair-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 24px; }
  .review-pair h4 { color: var(--cyan); font-size: 13px; margin-bottom: 10px; }
  .review-pair .field-row { display: flex; justify-content: space-between; padding: 4px 0; border-bottom: 1px solid var(--border); font-size: 12px; }
  .review-pair .field-row:last-child { border-bottom: none; }
  .review-pair .field-key { color: var(--muted); }
  .review-score { text-align: center; padding: 12px; border-top: 1px solid var(--border); margin-top: 16px; }

  @media (max-width: 900px) {
    .charts-row { grid-template-columns: 1fr; }
  }
</style>
</head>
<body>

<nav>
  <div class="logo">MDM</div>
  <button class="active" onclick="showTab('overview',this)">Overview</button>
  <button onclick="showTab('golden',this)">Golden Records</button>
  <button onclick="showTab('sources',this)">Source Records</button>
  <button onclick="showTab('conflicts',this)">Conflicts</button>
  <button onclick="showTab('review',this)">Review Queue</button>
</nav>

<!-- ===== OVERVIEW ===== -->
<div id="overview" class="section active">
  <div class="section-title">Pipeline <span>Overview</span></div>
  <div class="stat-grid" id="stat-grid"></div>
  <div class="charts-row">
    <div class="chart-box"><h3>Records per Source System</h3><canvas id="chart-sources"></canvas></div>
    <div class="chart-box"><h3>Merge Outcomes</h3><canvas id="chart-outcomes"></canvas></div>
    <div class="chart-box"><h3>Match Score Distribution</h3><canvas id="chart-scores"></canvas></div>
  </div>
</div>

<!-- ===== GOLDEN RECORDS ===== -->
<div id="golden" class="section">
  <div class="section-title">Golden <span>Records</span></div>
  <div class="filter-bar">
    <input id="golden-search" type="text" placeholder="Search name, email..." oninput="renderGolden()">
  </div>
  <div class="table-wrap">
    <table id="golden-table">
      <thead><tr>
        <th onclick="sortGolden('full_name')">Name</th>
        <th onclick="sortGolden('email')">Email</th>
        <th onclick="sortGolden('phone')">Phone</th>
        <th onclick="sortGolden('address')">Address</th>
        <th onclick="sortGolden('_source_count')">Sources</th>
        <th onclick="sortGolden('_match_score')">Score</th>
        <th>Systems</th>
      </tr></thead>
      <tbody id="golden-tbody"></tbody>
    </table>
  </div>
  <div id="drill-panel">
    <button id="drill-close" onclick="closeDrill()">✕ Close</button>
    <h2 id="drill-name"></h2>
    <div class="meta" id="drill-meta"></div>
    <div id="drill-content"></div>
  </div>
</div>

<!-- ===== SOURCE RECORDS ===== -->
<div id="sources" class="section">
  <div class="section-title">All Source <span>Records</span></div>
  <div class="filter-bar">
    <button class="active" onclick="filterSources('all',this)">All</button>
    <button onclick="filterSources('crm',this)">CRM</button>
    <button onclick="filterSources('marketing',this)">Marketing</button>
    <button onclick="filterSources('erp',this)">ERP</button>
    <input id="source-search" type="text" placeholder="Search..." oninput="renderSources()">
  </div>
  <div class="table-wrap">
    <table>
      <thead><tr>
        <th>System</th><th>Name</th><th>Email</th><th>Phone</th><th>Address</th>
        <th>Updated</th><th>Merged Into</th><th>Tier</th>
      </tr></thead>
      <tbody id="sources-tbody"></tbody>
    </table>
  </div>
</div>

<!-- ===== CONFLICTS ===== -->
<div id="conflicts" class="section">
  <div class="section-title">Field <span>Conflicts</span></div>
  <div class="filter-bar">
    <input id="conflict-search" type="text" placeholder="Filter by name, source..." oninput="renderConflicts()">
    <button class="active" onclick="filterConflicts('all',this)">All Fields</button>
    <button onclick="filterConflicts('full_name',this)">Name</button>
    <button onclick="filterConflicts('email',this)">Email</button>
    <button onclick="filterConflicts('phone',this)">Phone</button>
    <button onclick="filterConflicts('address',this)">Address</button>
  </div>
  <div class="table-wrap">
    <table>
      <thead><tr>
        <th>Field</th><th>Golden Record</th>
        <th>Winning Value</th><th>Source</th><th>Date</th>
        <th>Overridden Value</th><th>Source</th><th>Date</th>
      </tr></thead>
      <tbody id="conflicts-tbody"></tbody>
    </table>
  </div>
</div>

<!-- ===== REVIEW QUEUE ===== -->
<div id="review" class="section">
  <div class="section-title">Review <span>Queue</span></div>
  <div id="review-content"></div>
</div>

<script>
const DATA = __DATA__;

// ── Helpers ──────────────────────────────────────────────────────────────
function showTab(id, el) {
  document.querySelectorAll('.section').forEach(s => s.classList.remove('active'));
  document.querySelectorAll('nav button').forEach(b => b.classList.remove('active'));
  document.getElementById(id).classList.add('active');
  el.classList.add('active');
}

function scoreBadge(s) {
  if (!s || s === 0) return '<span class="score-badge score-none">Singleton</span>';
  const cls = s >= 85 ? 'score-high' : 'score-mid';
  return `<span class="score-badge ${cls}">${s.toFixed(1)}</span>`;
}

function systemChip(sys) {
  return `<span class="chip chip-${sys}">${sys}</span>`;
}

function goldenName(gid) {
  const g = DATA.golden_records.find(r => r.golden_id === gid);
  return g ? (g.full_name || '—') : '—';
}

// ── Overview ──────────────────────────────────────────────────────────────
function initOverview() {
  const s = DATA.stats;
  const statDefs = [
    { label: 'Source Records', value: s.total_sources, sub: 'Across all systems' },
    { label: 'Golden Records', value: s.total_golden, sub: 'Unique entities' },
    { label: 'Merges', value: s.total_merges, sub: 'Multi-source groups' },
    { label: 'Conflicts', value: s.total_conflicts, sub: 'Field-level overrides' },
  ];
  document.getElementById('stat-grid').innerHTML = statDefs.map(d => `
    <div class="stat-card">
      <div class="label">${d.label}</div>
      <div class="value">${d.value}</div>
      <div class="sub">${d.sub}</div>
    </div>`).join('');

  // Bar: records per source
  const srcLabels = Object.keys(DATA.source_counts);
  const srcColors = srcLabels.map(l => l === 'crm' ? '#42a5f5' : l === 'marketing' ? '#ce93d8' : '#ffa726');
  new Chart(document.getElementById('chart-sources'), {
    type: 'bar',
    data: {
      labels: srcLabels.map(l => l.toUpperCase()),
      datasets: [{ data: Object.values(DATA.source_counts), backgroundColor: srcColors, borderRadius: 5 }]
    },
    options: { plugins: { legend: { display: false } }, scales: {
      x: { grid: { color: '#1e3022' }, ticks: { color: '#78909c' } },
      y: { grid: { color: '#1e3022' }, ticks: { color: '#78909c', stepSize: 1 } }
    }}
  });

  // Donut: merge outcomes
  new Chart(document.getElementById('chart-outcomes'), {
    type: 'doughnut',
    data: {
      labels: Object.keys(DATA.merge_outcomes),
      datasets: [{ data: Object.values(DATA.merge_outcomes), backgroundColor: ['#69f0ae', '#37474f'], borderColor: '#0a0f0b', borderWidth: 3 }]
    },
    options: { plugins: { legend: { labels: { color: '#78909c', boxWidth: 14 } } }, cutout: '65%' }
  });

  // Histogram: score distribution
  const scores = DATA.auto_scores;
  const buckets = [70,75,80,85,90,95,100];
  const counts = new Array(buckets.length - 1).fill(0);
  scores.forEach(s => {
    for (let i = 0; i < buckets.length - 1; i++) {
      if (s >= buckets[i] && s < buckets[i+1]) { counts[i]++; break; }
    }
  });
  const histLabels = buckets.slice(0,-1).map((b,i) => `${b}–${buckets[i+1]}`);
  new Chart(document.getElementById('chart-scores'), {
    type: 'bar',
    data: {
      labels: histLabels,
      datasets: [{ data: counts, backgroundColor: '#00e5ff88', borderColor: '#00e5ff', borderWidth: 1, borderRadius: 4 }]
    },
    options: { plugins: { legend: { display: false } }, scales: {
      x: { grid: { color: '#1e3022' }, ticks: { color: '#78909c', font: { size: 11 } } },
      y: { grid: { color: '#1e3022' }, ticks: { color: '#78909c', stepSize: 1 } }
    }}
  });
}

// ── Golden Records ────────────────────────────────────────────────────────
let goldenSort = { key: '_match_score', dir: -1 };

function sortGolden(key) {
  if (goldenSort.key === key) goldenSort.dir *= -1;
  else { goldenSort.key = key; goldenSort.dir = -1; }
  renderGolden();
}

function renderGolden() {
  const q = (document.getElementById('golden-search').value || '').toLowerCase();
  let rows = DATA.golden_records.filter(r => {
    if (!q) return true;
    return [r.full_name, r.email, r.phone, r.address].some(v => v && v.toLowerCase().includes(q));
  });
  rows.sort((a, b) => {
    const av = a[goldenSort.key] ?? '';
    const bv = b[goldenSort.key] ?? '';
    return typeof av === 'number' ? (av - bv) * goldenSort.dir : String(av).localeCompare(String(bv)) * goldenSort.dir;
  });

  document.getElementById('golden-tbody').innerHTML = rows.map(r => `
    <tr onclick="openDrill('${r.golden_id}')" data-gid="${r.golden_id}">
      <td><strong>${r.full_name || '—'}</strong></td>
      <td>${r.email || '—'}</td>
      <td>${r.phone || '—'}</td>
      <td style="max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${r.address || ''}">${r.address || '—'}</td>
      <td><span style="color:var(--cyan);font-weight:700">${r._source_count}</span></td>
      <td>${scoreBadge(r._match_score)}</td>
      <td>${[...new Set(r._source_ids)].map(systemChip).join('')}</td>
    </tr>`).join('') || '<tr><td colspan="7" class="empty">No records</td></tr>';
}

function openDrill(gid) {
  document.querySelectorAll('#golden-tbody tr').forEach(r => r.classList.remove('selected'));
  const sel = document.querySelector(`#golden-tbody tr[data-gid="${gid}"]`);
  if (sel) sel.classList.add('selected');

  const g = DATA.golden_records.find(r => r.golden_id === gid);
  const contrib = DATA.golden_source_map[gid] || [];
  const FIELDS = DATA.fields;

  document.getElementById('drill-name').textContent = g.full_name || '—';
  document.getElementById('drill-meta').innerHTML =
    `Golden ID: <code style="color:var(--cyan)">${gid}</code> &nbsp;|&nbsp; ` +
    `Sources: ${g._source_count} &nbsp;|&nbsp; Score: ${g._match_score > 0 ? g._match_score.toFixed(2) : 'Singleton'} &nbsp;|&nbsp; ` +
    `Systems: ${[...new Set(g._source_ids)].map(systemChip).join('')}`;

  if (contrib.length === 0) {
    document.getElementById('drill-content').innerHTML = '<div class="empty"><div class="icon">📭</div>No source records found.</div>';
  } else {
    // Determine winning value per field
    const winners = {};
    FIELDS.forEach(f => { winners[f] = g[f]; });

    // Build table: rows = source records, cols = fields
    const header = `<tr><th>System</th><th>Row</th>${FIELDS.map(f => `<th>${f}</th>`).join('')}<th>Updated</th></tr>`;
    const bodyRows = contrib.map(src => {
      const cells = FIELDS.map(f => {
        const val = src[f];
        const win = winners[f];
        let cls = 'cell-same';
        let badge = '';
        let display = val || '<span style="color:var(--muted)">—</span>';
        if (val && win) {
          if (val === win) { cls = 'cell-win'; badge = '<span class="chip chip-win">WINNER</span> '; }
          else { cls = 'cell-override'; display = `<span class="strike">${val}</span>`; badge = '<span class="chip chip-override">OVERRIDDEN</span> '; }
        } else if (!val) {
          display = '<span style="color:var(--muted)">—</span>';
        }
        return `<td class="${cls}" title="Updated: ${src.updated_at || ''}">${badge}${display}</td>`;
      }).join('');
      return `<tr><td>${systemChip(src._source_system)}</td><td style="color:var(--muted)">${src._row_index}</td>${cells}<td style="color:var(--muted);font-size:12px">${src.updated_at || '—'}</td></tr>`;
    }).join('');

    document.getElementById('drill-content').innerHTML = `
      <table class="drill-field-table">
        <thead>${header}</thead>
        <tbody>${bodyRows}</tbody>
      </table>`;
  }

  document.getElementById('drill-panel').classList.add('open');
  document.getElementById('drill-panel').scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

function closeDrill() {
  document.getElementById('drill-panel').classList.remove('open');
  document.querySelectorAll('#golden-tbody tr').forEach(r => r.classList.remove('selected'));
}

// ── Source Records ────────────────────────────────────────────────────────
let sourceFilter = 'all';

function filterSources(sys, el) {
  sourceFilter = sys;
  document.querySelectorAll('.filter-bar button').forEach(b => b.classList.remove('active'));
  el.classList.add('active');
  renderSources();
}

function renderSources() {
  const q = (document.getElementById('source-search').value || '').toLowerCase();
  let rows = DATA.source_records.filter(r => {
    if (sourceFilter !== 'all' && r._source_system !== sourceFilter) return false;
    if (!q) return true;
    return [r.full_name, r.email, r.phone, r.address].some(v => v && v.toLowerCase().includes(q));
  });

  document.getElementById('sources-tbody').innerHTML = rows.map(r => {
    const gname = r.golden_id ? goldenName(r.golden_id) : '—';
    const tierBadge = r.match_tier === 'SINGLETON'
      ? '<span class="chip chip-singleton">Singleton</span>'
      : `<span class="chip chip-merged">${r.match_tier}</span>`;
    return `<tr>
      <td>${systemChip(r._source_system)}</td>
      <td>${r.full_name || '—'}</td>
      <td>${r.email || '—'}</td>
      <td>${r.phone || '—'}</td>
      <td style="max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${r.address||''}">${r.address || '—'}</td>
      <td style="color:var(--muted);font-size:12px">${r.updated_at || '—'}</td>
      <td style="color:var(--cyan)">${gname}</td>
      <td>${tierBadge}</td>
    </tr>`;
  }).join('') || '<tr><td colspan="8" class="empty">No records</td></tr>';
}

// ── Conflicts ─────────────────────────────────────────────────────────────
let conflictFieldFilter = 'all';

function filterConflicts(field, el) {
  conflictFieldFilter = field;
  document.querySelectorAll('#conflicts .filter-bar button').forEach(b => b.classList.remove('active'));
  el.classList.add('active');
  renderConflicts();
}

function renderConflicts() {
  const q = (document.getElementById('conflict-search').value || '').toLowerCase();
  const rows = DATA.conflicts.filter(r => {
    if (conflictFieldFilter !== 'all' && r.field !== conflictFieldFilter) return false;
    if (!q) return true;
    const gname = goldenName(r.golden_id);
    return [gname, r.winning_source, r.overridden_source, r.winning_value, r.overridden_value]
      .some(v => v && String(v).toLowerCase().includes(q));
  });

  document.getElementById('conflicts-tbody').innerHTML = rows.map(r => `
    <tr data-field="${r.field}">
      <td><strong>${r.field}</strong></td>
      <td style="color:var(--cyan)">${goldenName(r.golden_id)}</td>
      <td style="color:var(--green)">${r.winning_value || '—'}</td>
      <td>${systemChip(r.winning_source)}</td>
      <td style="color:var(--muted);font-size:12px">${r.winning_updated_at || '—'}</td>
      <td style="color:var(--amber);text-decoration:line-through">${r.overridden_value || '—'}</td>
      <td>${systemChip(r.overridden_source)}</td>
      <td style="color:var(--muted);font-size:12px">${r.overridden_updated_at || '—'}</td>
    </tr>`).join('') || '<tr><td colspan="8" class="empty">No conflicts</td></tr>';
}

// ── Review Queue ──────────────────────────────────────────────────────────
function renderReview() {
  const el = document.getElementById('review-content');
  if (!DATA.review_queue || DATA.review_queue.length === 0) {
    el.innerHTML = `<div class="empty"><div class="icon">✅</div><strong>No items in review queue.</strong><br><br><span style="color:var(--muted)">All candidate pairs were either auto-merged or rejected.</span></div>`;
    return;
  }
  const SCORE_FIELDS = ['name_score','email_score','phone_score','address_score'];
  el.innerHTML = DATA.review_queue.map(p => `
    <div class="review-pair">
      <div style="color:var(--amber);font-size:11px;text-transform:uppercase;letter-spacing:1px;margin-bottom:12px">
        Review Pair — Composite Score: <strong>${p.composite_score?.toFixed(2) ?? '—'}</strong>
      </div>
      <div class="pair-grid">
        <div>
          <h4>${systemChip(p.source_a)} Record A</h4>
          ${['full_name','email','phone','address'].map(f => `
            <div class="field-row"><span class="field-key">${f}</span><span>${p[f+'_a'] || '—'}</span></div>`).join('')}
        </div>
        <div>
          <h4>${systemChip(p.source_b)} Record B</h4>
          ${['full_name','email','phone','address'].map(f => `
            <div class="field-row"><span class="field-key">${f}</span><span>${p[f+'_b'] || '—'}</span></div>`).join('')}
        </div>
      </div>
      <div class="review-score">
        ${SCORE_FIELDS.map(sf => {
          const label = sf.replace('_score','');
          const val = p[sf] ?? 0;
          const pct = Math.min(100, val);
          return `<span style="display:inline-block;margin:4px 12px;font-size:12px">
            <span style="color:var(--muted)">${label}:</span>
            <span style="color:var(--cyan)">${val?.toFixed(1)}</span>
          </span>`;
        }).join('')}
      </div>
    </div>`).join('');
}

// ── Init ──────────────────────────────────────────────────────────────────
initOverview();
renderGolden();
renderSources();
renderConflicts();
renderReview();
</script>
</body>
</html>
"""


def main():
    print("Loading data...")
    golden, match_report, conflicts, review_queue, sources_df, config = load_data()

    print("Building JSON payload...")
    data = build_json_data(golden, match_report, conflicts, review_queue, sources_df, config)

    json_str = json.dumps(data, default=str, ensure_ascii=False)
    html = HTML_TEMPLATE.replace("__DATA__", json_str)

    out_path = os.path.join(OUTPUT_DIR, "mdm_dashboard.html")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\nDashboard written to: {out_path}")
    print(f"  Golden records : {data['stats']['total_golden']}")
    print(f"  Source records : {data['stats']['total_sources']}")
    print(f"  Conflicts      : {data['stats']['total_conflicts']}")
    print(f"  Review items   : {len(data['review_queue'])}")
    print("\nOpen in browser: file://" + out_path.replace("\\", "/"))


if __name__ == "__main__":
    main()
