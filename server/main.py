"""Real-time dashboard starter: FastAPI WebSocket server.

Streams simulated job events (a stand-in for your real feed) to many browser clients:
- snapshot on connect, then live events, with a sequence number so clients can detect gaps
- one bounded queue per client: a slow client drops its own oldest events, never stalls others
- per-connection filters ("subscribe" message)
- heartbeat pings so dead connections are noticed
- history over REST (/api/events), not over the socket
"""

import asyncio
import os
import random
import uuid
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Literal

from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ValidationError

RATE = float(os.getenv("EVENTS_PER_SECOND", "20"))
HISTORY = 5000
SNAPSHOT = 200
CLIENT_QUEUE = 1000
PING_SECONDS = 15

SERVICES = ["checkout", "search", "billing", "notifications", "media"]
REGIONS = ["us-east", "us-west", "eu-west"]


class Event(BaseModel):
    seq: int
    id: str
    kind: Literal["job.started", "job.finished", "job.failed"]
    service: str
    region: str
    latency_ms: int
    at: datetime


class Subscribe(BaseModel):
    type: Literal["subscribe"]
    services: list[str] = []  # empty = everything


class Hub:
    def __init__(self) -> None:
        self.history: deque[Event] = deque(maxlen=HISTORY)
        self.clients: dict[WebSocket, tuple[asyncio.Queue, set[str]]] = {}
        self.seq = 0

    def publish(self, event: Event) -> None:
        self.history.append(event)
        for queue, services in self.clients.values():
            if services and event.service not in services:
                continue
            if queue.full():  # backpressure: drop this client's oldest event, keep the newest
                queue.get_nowait()
            queue.put_nowait(event)

    def next_event(self) -> Event:
        self.seq += 1
        kind = random.choices(["job.started", "job.finished", "job.failed"], [5, 4, 1])[0]
        return Event(
            seq=self.seq,
            id=uuid.uuid4().hex[:12],
            kind=kind,
            service=random.choice(SERVICES),
            region=random.choice(REGIONS),
            latency_ms=int(random.lognormvariate(4.5, 0.6)),
            at=datetime.now(timezone.utc),
        )


hub = Hub()


async def produce() -> None:
    """Simulated feed. Replace with your real source (queue consumer, DB changefeed, exchange API...)."""
    while True:
        hub.publish(hub.next_event())
        await asyncio.sleep(1 / RATE)


@asynccontextmanager
async def lifespan(_: FastAPI):
    task = asyncio.create_task(produce())
    yield
    task.cancel()


app = FastAPI(title="Real-time dashboard starter", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "http://localhost:3000").split(","),
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/health")
def health() -> dict:
    return {"ok": True, "seq": hub.seq, "clients": len(hub.clients)}


@app.get("/api/events")
def events(
    before_seq: int | None = None,
    service: str | None = None,
    limit: int = Query(100, ge=1, le=500),
) -> list[Event]:
    """History and search go over REST with pagination, not over the socket."""
    rows = [
        e for e in reversed(hub.history)
        if (before_seq is None or e.seq < before_seq) and (service is None or e.service == service)
    ]
    return rows[:limit]


@app.websocket("/ws")
async def ws(socket: WebSocket) -> None:
    await socket.accept()
    queue: asyncio.Queue = asyncio.Queue(maxsize=CLIENT_QUEUE)
    services: set[str] = set()
    hub.clients[socket] = (queue, services)

    snapshot = [e.model_dump(mode="json") for e in list(hub.history)[-SNAPSHOT:]]
    await socket.send_json({"type": "snapshot", "seq": hub.seq, "events": snapshot})

    async def sender() -> None:
        while True:
            try:
                event = await asyncio.wait_for(queue.get(), timeout=PING_SECONDS)
                await socket.send_json({"type": "event", "event": event.model_dump(mode="json")})
            except asyncio.TimeoutError:
                await socket.send_json({"type": "ping", "seq": hub.seq})

    async def receiver() -> None:
        while True:
            raw = await socket.receive_json()
            try:
                msg = Subscribe.model_validate(raw)
            except ValidationError:
                await socket.send_json({"type": "error", "error": "unknown message"})
                continue
            services.clear()
            services.update(s for s in msg.services if s in SERVICES)
            await socket.send_json({"type": "subscribed", "services": sorted(services)})

    tasks = [asyncio.create_task(sender()), asyncio.create_task(receiver())]
    try:
        await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
    except WebSocketDisconnect:
        pass
    finally:
        for t in tasks:
            t.cancel()
        hub.clients.pop(socket, None)
