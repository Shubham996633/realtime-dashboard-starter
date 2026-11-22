// Live data layer: one WebSocket, one store, renders batched per animation frame.
// React components read it with useSyncExternalStore (see useLive below).

import { useSyncExternalStore } from "react";

export type JobEvent = {
  seq: number;
  id: string;
  kind: "job.started" | "job.finished" | "job.failed";
  service: string;
  region: string;
  latency_ms: number;
  at: string;
};

export type Status = "connecting" | "live" | "reconnecting" | "offline";

type State = {
  status: Status;
  events: JobEvent[]; // newest first, capped
  lastSeq: number;
  gaps: number; // events missed (sequence jumps), e.g. dropped while this tab was slow
  services: string[]; // current server-side filter; [] = all
};

const MAX_ROWS = 500;
const WS_URL = process.env.NEXT_PUBLIC_WS_URL || "ws://localhost:8765/ws";
export const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8765";

let state: State = { status: "connecting", events: [], lastSeq: 0, gaps: 0, services: [] };
const listeners = new Set<() => void>();
const set = (patch: Partial<State>) => {
  state = { ...state, ...patch };
  listeners.forEach((l) => l());
};

// Validate at the edge: a malformed message is logged and dropped, never rendered.
const isEvent = (e: any): e is JobEvent =>
  e && typeof e.seq === "number" && typeof e.id === "string" && typeof e.service === "string" &&
  typeof e.latency_ms === "number" && ["job.started", "job.finished", "job.failed"].includes(e.kind);

// Incoming events are buffered and applied once per frame, so 100s of messages/second
// cause at most one React update per frame.
let buffer: JobEvent[] = [];
let frame = 0;
function flush() {
  frame = 0;
  if (!buffer.length) return;
  let { lastSeq, gaps } = state;
  for (const e of buffer) {
    if (lastSeq && e.seq > lastSeq + 1 && !state.services.length) gaps += e.seq - lastSeq - 1;
    lastSeq = Math.max(lastSeq, e.seq);
  }
  const events = [...buffer.reverse(), ...state.events].slice(0, MAX_ROWS);
  buffer = [];
  set({ events, lastSeq, gaps });
}
// The server filters too, but events already in flight when the filter changes can still
// arrive, so the client checks again before anything reaches the screen.
const wanted = (e: JobEvent) => !state.services.length || state.services.includes(e.service);

function enqueue(e: JobEvent) {
  if (!wanted(e)) return;
  buffer.push(e);
  if (!frame) frame = requestAnimationFrame(flush);
}

let socket: WebSocket | null = null;
let attempt = 0;
let retryTimer: ReturnType<typeof setTimeout> | undefined;
let watchdog: ReturnType<typeof setTimeout> | undefined;
let started = false;

// Server pings every 15 s; if nothing arrives for 40 s the connection is treated as dead.
function armWatchdog() {
  clearTimeout(watchdog);
  watchdog = setTimeout(() => socket?.close(), 40_000);
}

function connect() {
  set({ status: attempt ? "reconnecting" : "connecting" });
  const ws = new WebSocket(WS_URL);
  socket = ws;

  ws.onopen = () => {
    attempt = 0;
    armWatchdog();
    if (state.services.length) ws.send(JSON.stringify({ type: "subscribe", services: state.services }));
  };

  ws.onmessage = (msg) => {
    armWatchdog();
    let data: any;
    try { data = JSON.parse(msg.data); } catch { return console.warn("bad JSON from server"); }
    if (data.type === "snapshot") {
      // After (re)connecting, trust the snapshot, not whatever was on screen.
      const events = (data.events || []).filter(isEvent).filter(wanted).reverse().slice(0, MAX_ROWS);
      buffer = [];
      set({ status: "live", events, lastSeq: data.seq });
    } else if (data.type === "event") {
      if (isEvent(data.event)) enqueue(data.event);
      else console.warn("dropped malformed event", data.event);
    }
  };

  ws.onclose = () => {
    clearTimeout(watchdog);
    if (socket !== ws) return;
    socket = null;
    if (!navigator.onLine) return set({ status: "offline" });
    // Exponential backoff with jitter, so a server restart isn't hit by every client at once.
    const delay = Math.min(30_000, 500 * 2 ** attempt) * (0.5 + Math.random() / 2);
    attempt += 1;
    set({ status: "reconnecting" });
    retryTimer = setTimeout(connect, delay);
  };
}

export function start() {
  if (started || typeof window === "undefined") return;
  started = true;
  window.addEventListener("online", () => { if (!socket) { clearTimeout(retryTimer); attempt = 0; connect(); } });
  window.addEventListener("offline", () => socket?.close());
  connect();
}

export function subscribe(services: string[]) {
  buffer = buffer.filter((e) => !services.length || services.includes(e.service));
  set({ services, events: state.events.filter((e) => !services.length || services.includes(e.service)) });
  if (socket?.readyState === WebSocket.OPEN) socket.send(JSON.stringify({ type: "subscribe", services }));
}

export function useLive(): State {
  return useSyncExternalStore(
    (l) => { listeners.add(l); return () => listeners.delete(l); },
    () => state,
    () => state,
  );
}
