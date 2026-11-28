// Web interface for the multi-pipeline ETL tool. Vanilla JS, no build step.
// Every value that comes from the logs (resource paths!) is inserted with textContent, never innerHTML.
"use strict";

const $ = (sel) => document.querySelector(sel);
const state = { info: null, runs: [], report: null, tab: "Q1", polling: null, filter: "" };

// ------------------------------------------------------------------ helpers
function el(tag, attrs = {}, ...kids) {
  const n = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (v === null || v === undefined || v === false) continue;
    if (k === "class") n.className = v;
    else if (k === "text") n.textContent = v;
    else if (k.startsWith("on")) n.addEventListener(k.slice(2), v);
    else n.setAttribute(k, v === true ? "" : v);
  }
  for (const c of kids.flat()) if (c !== null && c !== undefined) n.append(c instanceof Node ? c : String(c));
  return n;
}
const fmtInt = (v) => (v === null || v === undefined ? "NULL" : Number(v).toLocaleString("en-US"));
const fmtSec = (v) => `${Number(v).toFixed(3)} s`;
const store = {
  get(k) { try { return localStorage.getItem(k); } catch { return null; } },
  set(k, v) { try { localStorage.setItem(k, v); } catch { /* private window: ignore */ } },
};
// Same idea as report._text: show control / non-ASCII characters of real paths as escapes.
function printable(s) {
  s = String(s);
  if (/^[\x20-\x7e]*$/.test(s)) return s;
  return s.replace(/[^\x20-\x7e]/g, (c) => {
    if (c === "\t") return "\\t";
    if (c === "\n") return "\\n";
    if (c === "\r") return "\\r";
    const code = c.charCodeAt(0);
    return code < 256 ? "\\x" + code.toString(16).padStart(2, "0") : "\\u" + code.toString(16).padStart(4, "0");
  });
}
async function api(path, opts = {}) {
  const res = await fetch(path, { headers: { "Content-Type": "application/json" }, ...opts });
  let body = null;
  try { body = await res.json(); } catch { /* not JSON */ }
  if (!res.ok) throw new Error((body && body.error) || `${res.status} ${res.statusText}`);
  return body;
}
function pipeLabel(name) {
  const p = state.info && state.info.pipelines.find((x) => x.name === name);
  return p ? p.label : name;
}
function runLabel(r) {
  return `${pipeLabel(r.pipeline)} · ${r.run_id} · batch ${fmtInt(r.batch_size)} · ${Number(r.runtime_sec).toFixed(1)} s`;
}

// ------------------------------------------------------------------ tooltip
const tip = $("#tip");
function showTip(ev, text) {
  tip.textContent = text;
  tip.hidden = false;
  const x = Math.min(ev.clientX + 12, window.innerWidth - tip.offsetWidth - 8);
  tip.style.left = `${x}px`;
  tip.style.top = `${ev.clientY + 14}px`;
}
const hideTip = () => { tip.hidden = true; };

// Single-series horizontal bars: thin marks, value at the tip, hover tooltip. The table view is the report itself.
function barChart(container, items, unit) {
  container.replaceChildren();
  if (!items.length) { container.append(el("div", { class: "empty", text: "Nothing to show yet." })); return; }
  const max = Math.max(...items.map((i) => i.value), 1e-9);
  for (const it of items) {
    const pct = Math.max(0.4, (it.value / max) * 100);
    const tipText = `${it.label}: ${it.value.toFixed(3)} ${unit}${it.hint ? " · " + it.hint : ""}`;
    container.append(el("div", {
      class: "bar-row", tabindex: "0", "aria-label": tipText,
      onmousemove: (e) => showTip(e, tipText), onmouseleave: hideTip,
    },
      el("span", { class: "lab", text: it.label, title: it.label }),
      el("div", { class: "bar-track bar-axis" },
        el("div", { class: "bar", style: `width:${pct}%` }),
        el("span", { class: "bar-val", text: it.value.toFixed(it.value >= 100 ? 1 : 3) })),
    ));
  }
}

// ------------------------------------------------------------------ setup
async function loadInfo() {
  const info = await api("/api/info");
  state.info = info;
  const db = $("#db-status");
  db.classList.toggle("ok", !!info.database);
  db.classList.toggle("bad", !info.database);
  db.querySelector(".txt").textContent = info.database ? `Results stored in ${info.database}` : `Database: ${info.database_error}`;

  const last = store.get("etl.pipeline");
  const list = $("#pipe-list");
  list.replaceChildren();
  info.pipelines.forEach((p, i) => {
    const checked = last ? last === p.name : i === 0;
    list.append(el("label", { class: "pipe" + (p.enabled ? "" : " off") },
      el("input", { type: "radio", name: "pipeline", value: p.name, checked: checked && p.enabled, disabled: !p.enabled, required: true }),
      el("span", { class: "name", text: p.label }),
      el("span", { class: "eng", text: p.engine }),
      el("span", { class: "owner", text: p.enabled ? `pipeline "${p.name}"` : "disabled in config.yaml" }),
    ));
  });
  $("#batch-size").value = store.get("etl.batch") || info.default_batch_size;
  const sel = $("#input-sel");
  sel.replaceChildren();
  for (const [key, inp] of Object.entries(info.inputs)) {
    const mb = (inp.bytes / 1048576).toFixed(inp.bytes > 1048576 ? 0 : 2);
    sel.append(el("option", { value: key, disabled: !inp.available, selected: key === (store.get("etl.input") || "full") },
      `${inp.label} — ${inp.files.join(", ")} (${mb} MB)${inp.available ? "" : " — files missing"}`));
  }
}

async function loadRuns(selectId) {
  try {
    state.runs = (await api("/api/runs")).runs;
  } catch (e) {
    state.runs = [];
    $("#report-empty").textContent = `Cannot read stored runs: ${e.message}`;
  }
  const runSel = $("#run-sel");
  const keep = selectId || runSel.value;
  runSel.replaceChildren(el("option", { value: "", text: state.runs.length ? "— choose a stored run —" : "— no stored runs —" }));
  for (const r of state.runs) runSel.append(el("option", { value: r.run_id, selected: r.run_id === keep, text: runLabel(r) }));
  // Default comparison: the newest run against the newest run of ANOTHER pipeline in the same experiment.
  const newest = state.runs[0];
  const partner = newest && (state.runs.find((r) => r.pipeline !== newest.pipeline && expKey(r) === expKey(newest))
    || state.runs.find((r) => r.run_id !== newest.run_id));
  for (const [id, pick] of [["#cmp-a", newest], ["#cmp-b", partner]]) {
    const s = $(id);
    s.replaceChildren();
    state.runs.forEach((r) => s.append(el("option", { value: r.run_id, selected: !!pick && pick.run_id === r.run_id, text: runLabel(r) })));
  }
  $("#cmp-out").replaceChildren();
  renderRunsTable();
  renderExperiments(newest ? expKey(newest) : null);
}
const expKey = (r) => `${r.total_records}|${r.batch_size}`;

// ------------------------------------------------------------------ run + poll
$("#run-form").addEventListener("submit", async (ev) => {
  ev.preventDefault();
  const pipeline = (document.querySelector("input[name=pipeline]:checked") || {}).value;
  const batch = Number($("#batch-size").value);
  const input = $("#input-sel").value;
  if (!pipeline) return showJobError("Choose one pipeline first.");
  store.set("etl.pipeline", pipeline); store.set("etl.batch", String(batch)); store.set("etl.input", input);
  $("#run-btn").disabled = true;
  try {
    const { job } = await api("/api/runs", { method: "POST", body: JSON.stringify({ pipeline, batch_size: batch, input }) });
    renderJob(job);
    poll();
  } catch (e) {
    $("#run-btn").disabled = false;
    showJobError(e.message);
  }
});

function showJobError(msg) {
  const box = $("#job");
  box.hidden = false;
  box.className = "job fail";
  box.replaceChildren(el("div", {}, el("b", { text: "Could not start the run" }), el("pre", { text: msg })));
}

function renderJob(job) {
  const box = $("#job");
  if (!job) { box.hidden = true; return; }
  box.hidden = false;
  const what = `${pipeLabel(job.pipeline)} · batch size ${fmtInt(job.batch_size)} · ${job.input === "full" ? "full dataset" : "test sample"}`;
  if (job.state === "running") {
    box.className = "job";
    box.replaceChildren(el("div", { class: "spinner", "aria-hidden": "true" }),
      el("div", {}, el("b", { text: `Running ${what}` }),
        el("div", { class: "muted", text: `${job.elapsed_sec.toFixed(0)} s elapsed. The engine is reading, parsing, querying and loading into PostgreSQL. Hadoop jobs take a few minutes.` })));
  } else if (job.state === "succeeded") {
    box.className = "job ok";
    box.replaceChildren(el("div", {}, el("b", { text: `Finished: ${what}` }),
      el("div", { text: `Run ID ${job.run_id} · runtime ${fmtSec(job.runtime_sec)} (input read → results committed). Contract check passed; results and metadata are stored.` })));
  } else {
    box.className = "job fail";
    box.replaceChildren(el("div", {}, el("b", { text: `Failed: ${what}` }),
      el("div", { class: "muted", text: "Nothing was stored for this run." }),
      el("pre", { text: job.error || "unknown error" })));
  }
}

async function poll() {
  clearTimeout(state.polling);
  let job;
  try { job = (await api("/api/jobs/current")).job; } catch { state.polling = setTimeout(poll, 2000); return; }
  renderJob(job);
  if (job && job.state === "running") {
    $("#run-btn").disabled = true;
    state.polling = setTimeout(poll, 1000);
    return;
  }
  $("#run-btn").disabled = false;
  if (job && job.state === "succeeded" && state.lastShownJob !== job.job_id) {
    state.lastShownJob = job.job_id;
    await loadRuns(job.run_id);
    await showRun(job.run_id);
    document.getElementById("rep-h").scrollIntoView({ behavior: "smooth", block: "start" });
  }
}

// ------------------------------------------------------------------ report
$("#run-sel").addEventListener("change", (e) => { if (e.target.value) showRun(e.target.value); });

async function showRun(runId) {
  try {
    state.report = await api(`/api/runs/${encodeURIComponent(runId)}`);
  } catch (e) {
    $("#report").hidden = true;
    $("#report-empty").hidden = false;
    $("#report-empty").textContent = `Cannot load run ${runId}: ${e.message}`;
    return;
  }
  $("#run-sel").value = runId;
  $("#report-empty").hidden = true;
  $("#report").hidden = false;
  renderReport();
  renderRunsTable();
}

function tile(k, v, hint, small) {
  return el("div", { class: "tile" }, el("div", { class: "k", text: k }), el("div", { class: "v" + (small ? " small" : ""), text: v }), hint ? el("div", { class: "h", text: hint }) : null);
}

function renderReport() {
  const { meta, stage_timings: st, queries } = state.report;
  $("#meta").replaceChildren(
    tile("Selected pipeline", pipeLabel(meta.pipeline), `"${meta.pipeline}"`),
    tile("Runtime", fmtSec(meta.runtime_sec), "input read → results committed"),
    tile("Batch size", fmtInt(meta.batch_size), "records per batch"),
    tile("Batch count", fmtInt(meta.batch_count), "non-empty batches"),
    tile("Average batch size", Number(meta.avg_batch_size).toFixed(2), "total records / batches"),
    tile("Total records", fmtInt(meta.total_records), "every input line"),
    tile("Malformed records", fmtInt(meta.malformed_count), "counted, never dropped"),
    tile("Run ID", meta.run_id, null, true),
    tile("Stored at (UTC)", String(meta.loaded_at), null, true),
  );
  barChart($("#stages"), Object.entries(st).map(([k, v]) => ({ label: k, value: v })), "s");

  const n = meta.batch_count, size = meta.batch_size;
  const lastSize = meta.total_records - (n - 1) * size;
  const ids = queries.Q1.batch_ids.join(", ") || "-";
  $("#batches").replaceChildren(
    el("table", { class: "data" },
      el("tbody", {},
        el("tr", {}, el("td", { text: "Batch IDs" }), el("td", { class: "num", text: n > 1 ? `1 … ${fmtInt(n)}` : "1" })),
        el("tr", {}, el("td", { text: "Full batches" }), el("td", { class: "num", text: `${fmtInt(n - 1)} × ${fmtInt(size)} records` })),
        el("tr", {}, el("td", { text: "Last batch" }), el("td", { class: "num", text: `${fmtInt(lastSize)} records (batch ${fmtInt(n)})` })),
        el("tr", {}, el("td", { text: "batch_id on result rows" }), el("td", { class: "num", text: ids })))),
    el("p", { class: "note", text: "A record's batch is (line number − 1) ÷ batch size + 1. The queries run once over all batches, so every result row carries the final batch ID." }),
  );

  const tabs = $("#q-tabs");
  tabs.replaceChildren();
  for (const [qid, q] of Object.entries(queries)) {
    tabs.append(el("button", { class: "tab", role: "tab", type: "button", "aria-selected": String(qid === state.tab),
      onclick: () => { state.tab = qid; state.filter = ""; renderReport(); } }, `${qid} · ${q.name} (${q.rows.length})`));
  }
  renderQuery();
}

function renderQuery() {
  const { meta, queries } = state.report;
  const q = queries[state.tab];
  const panel = $("#q-panel");
  const head = el("div", { class: "q-head" },
    el("span", {}, "Query: ", el("b", { text: `${state.tab} ${q.name}` })),
    el("span", {}, "Rows: ", el("b", { text: fmtInt(q.rows.length) })),
    el("span", {}, "Batch ID: ", el("b", { text: `${q.batch_ids.join(", ") || "-"} of ${meta.batch_count}` })),
    el("span", {}, "Executed at: ", el("b", { text: q.executed_at ? `${q.executed_at} UTC` : "-" })),
    q.query_runtime_sec !== null && q.query_runtime_sec !== undefined ? el("span", {}, "Query runtime: ", el("b", { text: fmtSec(q.query_runtime_sec) })) : null,
  );
  const filter = el("input", { type: "search", placeholder: "Filter rows…", value: state.filter, "aria-label": "Filter rows" });
  const tableWrap = el("div", { class: "table-wrap" });
  const count = el("span", { class: "muted" });
  const draw = () => {
    const f = state.filter.toLowerCase();
    const rows = f ? q.rows.filter((r) => q.columns.some((c) => printable(r[c] ?? "NULL").toLowerCase().includes(f))) : q.rows;
    count.textContent = f ? `${rows.length} of ${q.rows.length} rows match` : "";
    const numeric = (c) => c !== "resource_path" && c !== "log_date";
    const thead = el("thead", {}, el("tr", {}, el("th", { class: "num", text: "#" }),
      q.columns.map((c) => el("th", { class: numeric(c) ? "num" : null, text: c === "error_rate" ? "error_rate (%)" : c }))));
    const tbody = el("tbody");
    rows.forEach((r, i) => {
      tbody.append(el("tr", {}, el("td", { class: "num muted", text: String(i + 1) }), q.columns.map((c) => {
        const v = r[c];
        let txt;
        if (v === null || v === undefined) txt = "NULL";
        else if (c === "error_rate") txt = (v * 100).toFixed(4);
        else if (c === "resource_path") txt = printable(v);
        else txt = String(v);
        return el("td", { class: c === "resource_path" ? "path" : numeric(c) ? "num" : null, text: txt });
      })));
    });
    tableWrap.replaceChildren(el("table", { class: "data" }, thead, tbody));
  };
  filter.addEventListener("input", () => { state.filter = filter.value; draw(); });
  panel.replaceChildren(head, el("div", { class: "q-tools" }, filter, count), tableWrap);
  draw();
}

// ------------------------------------------------------------------ stored runs + compare
function renderRunsTable() {
  const t = $("#runs-table");
  const current = state.report && state.report.meta.run_id;
  t.replaceChildren(
    el("thead", {}, el("tr", {}, ["Pipeline", "Run ID", "Batch size", "Batches", "Records", "Malformed", "Runtime (s)", "Stored at (UTC)"]
      .map((h, i) => el("th", { class: i >= 2 && i <= 6 ? "num" : null, text: h })))),
    el("tbody", {}, state.runs.map((r) => el("tr", { class: r.run_id === current ? "sel" : null },
      el("td", { text: pipeLabel(r.pipeline) }),
      el("td", {}, el("button", { class: "link", type: "button", text: r.run_id, onclick: () => showRun(r.run_id) })),
      el("td", { class: "num", text: fmtInt(r.batch_size) }),
      el("td", { class: "num", text: fmtInt(r.batch_count) }),
      el("td", { class: "num", text: fmtInt(r.total_records) }),
      el("td", { class: "num", text: fmtInt(r.malformed_count) }),
      el("td", { class: "num", text: Number(r.runtime_sec).toFixed(3) }),
      el("td", { text: String(r.loaded_at) }),
    ))),
  );
  if (!state.runs.length) t.append(el("tbody", {}, el("tr", {}, el("td", { colspan: "8", class: "muted", text: "No stored runs yet." }))));
}

function experiments() {
  const groups = new Map();
  for (const r of state.runs) {
    const key = expKey(r);
    if (!groups.has(key)) groups.set(key, { total: r.total_records, size: r.batch_size, runs: [] });
    groups.get(key).runs.push(r);
  }
  return [...groups.entries()];
}

function renderExperiments(prefer) {
  const sel = $("#exp-sel"), prev = prefer || sel.value;
  const exps = experiments();
  sel.replaceChildren();
  exps.forEach(([key, g], i) => {
    const pipes = new Set(g.runs.map((r) => r.pipeline)).size;
    sel.append(el("option", { value: key, selected: prev ? prev === key : i === 0,
      text: `${fmtInt(g.total)} records · batch size ${fmtInt(g.size)} · ${pipes} pipeline${pipes === 1 ? "" : "s"}` }));
  });
  renderRuntimeChart();
}
$("#exp-sel").addEventListener("change", renderRuntimeChart);

function renderRuntimeChart() {
  const key = $("#exp-sel").value;
  const g = experiments().find(([k]) => k === key);
  if (!g) { barChart($("#runtime-chart"), [], "s"); return; }
  const latest = new Map();
  for (const r of g[1].runs) if (!latest.has(r.pipeline)) latest.set(r.pipeline, r); // runs are newest first
  const order = state.info.pipelines.map((p) => p.name);
  const items = [...latest.values()]
    .filter((r) => order.includes(r.pipeline))  // only the selectable pipelines, never the stub's fake numbers
    .sort((a, b) => order.indexOf(a.pipeline) - order.indexOf(b.pipeline))
    .map((r) => ({ label: pipeLabel(r.pipeline), value: r.runtime_sec, hint: r.run_id }));
  barChart($("#runtime-chart"), items, "s");
}

$("#cmp-btn").addEventListener("click", async () => {
  const a = $("#cmp-a").value, b = $("#cmp-b").value, out = $("#cmp-out");
  if (!a || !b) { out.replaceChildren(el("div", { class: "verdict warn", text: "Store at least two runs first." })); return; }
  if (a === b) { out.replaceChildren(el("div", { class: "verdict warn", text: "Pick two different runs." })); return; }
  out.replaceChildren(el("div", { class: "muted", text: "Comparing…" }));
  try {
    const r = await api(`/api/compare?a=${encodeURIComponent(a)}&b=${encodeURIComponent(b)}`);
    const names = `${pipeLabel(r.a.pipeline)} (${r.a.run_id}) vs ${pipeLabel(r.b.pipeline)} (${r.b.run_id})`;
    const sameBatch = r.a.batch_size === r.b.batch_size;
    if (r.equivalent) {
      out.replaceChildren(el("div", { class: "verdict ok" }, el("span", { class: "badge", text: "EQUIVALENT" }),
        `${names}: record counts and every Q1, Q2 and Q3 row match.`));
    } else {
      out.replaceChildren(el("div", { class: "verdict fail" }, el("span", { class: "badge", text: "DIFFERENT" }),
        `${names}: ${r.difference_count} difference(s).`,
        sameBatch ? null : el("div", { text: "The batch sizes differ, so batch_count differs too. Compare runs with the same batch size." }),
        el("ul", {}, r.differences.map((d) => el("li", { text: printable(d) })))));
    }
  } catch (e) {
    out.replaceChildren(el("div", { class: "verdict fail", text: e.message }));
  }
});

// ------------------------------------------------------------------ boot
(async function boot() {
  try {
    await loadInfo();
  } catch (e) {
    showJobError(`Cannot reach the ETL server: ${e.message}`);
    return;
  }
  await loadRuns();
  if (state.runs.length) await showRun(state.runs[0].run_id);
  poll();
})();
