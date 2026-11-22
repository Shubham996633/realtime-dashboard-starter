"use client";

import { useEffect, useMemo, useState } from "react";
import { API_URL, type JobEvent, start, subscribe, useLive } from "@/lib/live";

const SERVICES = ["checkout", "search", "billing", "notifications", "media"];

function p95(values: number[]) {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * 0.95))];
}

export default function Dashboard() {
  const { status, events, gaps, services } = useLive();
  const [older, setOlder] = useState<JobEvent[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => start(), []);

  const stats = useMemo(() => {
    const finished = events.filter((e) => e.kind !== "job.started");
    const failed = finished.filter((e) => e.kind === "job.failed").length;
    return {
      total: events.length,
      failRate: finished.length ? (100 * failed) / finished.length : 0,
      p95: p95(finished.map((e) => e.latency_ms)),
    };
  }, [events]);

  // History goes over REST with pagination; the socket only carries what's happening now.
  async function loadOlder() {
    const oldest = (older.at(-1) || events.at(-1))?.seq;
    if (!oldest) return;
    setLoading(true);
    const qs = new URLSearchParams({ before_seq: String(oldest), limit: "100", ...(services.length === 1 ? { service: services[0] } : {}) });
    try {
      const res = await fetch(`${API_URL}/api/events?${qs}`);
      const page: JobEvent[] = await res.json();
      setOlder((o) => [...o, ...page]);
    } finally {
      setLoading(false);
    }
  }

  const toggle = (s: string) => {
    const next = services.includes(s) ? services.filter((x) => x !== s) : [...services, s];
    setOlder([]);
    subscribe(next);
  };

  const rows = [...events.slice(0, 200), ...older]; // live window + pages loaded over REST

  return (
    <main className="wrap">
      <header className="top">
        <h1>Live jobs</h1>
        <span className={`status status--${status}`} role="status">{status === "live" ? "● Live" : status}</span>
      </header>

      <section className="cards" aria-label="Summary">
        <div className="card"><span>Events on screen</span><strong>{stats.total}</strong></div>
        <div className="card"><span>Failure rate</span><strong>{stats.failRate.toFixed(1)}%</strong></div>
        <div className="card"><span>p95 latency</span><strong>{stats.p95} ms</strong></div>
        <div className="card"><span>Missed events</span><strong>{gaps}</strong></div>
      </section>

      <fieldset className="filters">
        <legend>Services (filtered on the server)</legend>
        {SERVICES.map((s) => (
          <label key={s}>
            <input type="checkbox" checked={services.includes(s)} onChange={() => toggle(s)} /> {s}
          </label>
        ))}
      </fieldset>

      <div className="table-wrap">
      <table className="table">
        <thead>
          <tr><th>Seq</th><th>Time</th><th>Service</th><th>Region</th><th>Event</th><th>Latency</th></tr>
        </thead>
        <tbody>
          {rows.map((e) => (
            <tr key={e.seq} className={e.kind === "job.failed" ? "bad" : undefined}>
              <td>{e.seq}</td>
              <td>{new Date(e.at).toLocaleTimeString()}</td>
              <td>{e.service}</td>
              <td>{e.region}</td>
              <td>{e.kind.replace("job.", "")}</td>
              <td>{e.latency_ms} ms</td>
            </tr>
          ))}
        </tbody>
      </table>
      </div>
      <button className="more" onClick={loadOlder} disabled={loading}>{loading ? "Loading…" : "Load older (REST)"}</button>
    </main>
  );
}
