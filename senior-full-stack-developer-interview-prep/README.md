# Senior Full Stack Developer Interview Prep
### Role: Knowsy Platform | Focus: Backend, Architecture, Real-Time, AI Workflows

The backend examples use Python with FastAPI, asyncpg, Redis, and Celery. The snippets illustrate individual patterns and assume application services, settings, and test fixtures are wired separately. See the official guides for [FastAPI authentication](https://fastapi.tiangolo.com/tutorial/security/oauth2-jwt/), [FastAPI WebSockets](https://fastapi.tiangolo.com/advanced/websockets/), [Supabase Realtime for Python](https://supabase.com/docs/reference/python/subscribe), and [Celery tasks](https://docs.celeryq.dev/en/stable/userguide/tasks.html).

---

## Table of Contents

1. [Multi-Tenant SaaS Architecture](#1-multi-tenant-saas-architecture)
2. [WebSockets & Real-Time Systems](#2-websockets--real-time-systems)
3. [Supabase Architecture](#3-supabase-architecture)
4. [Role-Based Access Control (RBAC)](#4-role-based-access-control-rbac)
5. [PostgreSQL Optimization & Indexing](#5-postgresql-optimization--indexing)
6. [Event-Driven Architecture](#6-event-driven-architecture)
7. [API Design & Authentication](#7-api-design--authentication)
8. [Scalable Backend Design & FastAPI](#8-scalable-backend-design--fastapi)
9. [System Design – Multiplayer/Real-Time Platform](#9-system-design--multiplayerreal-time-platform)
10. [AI Workflow & Code Review](#10-ai-workflow--code-review)
11. [Distributed Systems Basics](#11-distributed-systems-basics)
12. [Analytics Architecture](#12-analytics-architecture)
13. [GCP Basics](#13-gcp-basics)
14. [Security](#14-security)
15. [Quick Revision Cheat Sheet](#quick-revision-cheat-sheet)

---

## 1. Multi-Tenant SaaS Architecture

### What is Multi-Tenancy?
One application instance serves multiple customers (tenants). Each tenant's data must be isolated, secure, and configurable independently.

---

### Q: What are the three models of multi-tenancy? When do you use each?

**A:**

**1. Shared Database, Shared Schema (Row-Level)**
- Single DB, single table set, each row has a `tenant_id` column.
- Simplest to build and operate.
- Risk: Developer forgets WHERE clause → data leak.
- Used when: Cost matters, tenants have identical structure.

```sql
-- Every table has tenant_id
CREATE TABLE tasks (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  title TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Every query MUST filter by tenant_id
SELECT * FROM tasks WHERE tenant_id = $1 AND id = $2;
```

**2. Shared Database, Separate Schemas**
- One DB, each tenant gets their own Postgres schema.
- Good isolation without per-tenant DB cost.
- Used when: Tenants need custom columns/tables, moderate scale.

```sql
-- Schema per tenant
CREATE SCHEMA tenant_abc;
CREATE TABLE tenant_abc.tasks (...);

-- Set schema at connection time
SET search_path = tenant_abc;
SELECT * FROM tasks;
```

**3. Separate Database per Tenant**
- Full DB isolation. Maximum security, compliance-friendly (HIPAA, SOC2).
- Expensive to operate, complex connection pooling.
- Used when: Enterprise clients with strict data residency needs.

**Decision Matrix:**

| Model | Isolation | Cost | Complexity | Best For |
|---|---|---|---|---|
| Shared Schema | Low | Lowest | Low | Startups, SMB |
| Separate Schema | Medium | Medium | Medium | Mid-market SaaS |
| Separate DB | High | High | High | Enterprise/Compliance |

---

### Q: How do you prevent tenant data leakage in a shared schema model?

**A:** Multiple layers of defense:

**Layer 1 – A FastAPI dependency verifies tenant membership:**
```python
from typing import Annotated
from uuid import UUID

from fastapi import Depends, Header, HTTPException


async def current_tenant(
    actor: Annotated[Actor, Depends(current_actor)],
    x_tenant_id: Annotated[UUID | None, Header()] = None,
) -> UUID:
    tenant_id = x_tenant_id if x_tenant_id is not None else actor.tenant_id
    if not await memberships.is_active(actor.user_id, tenant_id):
        raise HTTPException(status_code=403, detail="Tenant access denied")
    return tenant_id
```

**Layer 2 – Repository/Service layer always scopes queries:**
```python
from uuid import UUID

import asyncpg


async def list_tasks(pool: asyncpg.Pool, tenant_id: UUID) -> list[dict]:
    async with pool.acquire() as conn, conn.transaction():
        await conn.fetchval(
            "SELECT set_config('app.current_tenant', $1, true)", str(tenant_id)
        )
        rows = await conn.fetch(
            "SELECT id, title FROM tasks WHERE tenant_id = $1 ORDER BY id",
            tenant_id,
        )
    return [dict(row) for row in rows]
```

**Layer 3 – PostgreSQL Row Level Security (RLS):**
```sql
ALTER TABLE tasks ENABLE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON tasks
  USING (tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::UUID);

-- Set within each transaction, before accessing tenant rows
SELECT set_config('app.current_tenant', $1, true);
```

**Layer 4 – Integration tests per tenant:**
```python
def test_task_list_is_tenant_scoped(client, tenant_a_token, task_a, task_b):
    response = client.get(
        "/api/tasks", headers={"Authorization": f"Bearer {tenant_a_token}"}
    )
    assert response.status_code == 200
    ids = {task["id"] for task in response.json()}
    assert str(task_a.id) in ids
    assert str(task_b.id) not in ids
```

---

### Q: How do you handle tenant onboarding and schema migrations in a multi-tenant setup?

**A:**

**Onboarding Flow:**
```
1. Admin registers new tenant → create tenants table row
2. Generate tenant slug / subdomain
3. If separate schema: run migration script for new schema
4. Seed default config (roles, settings, plan)
5. Send invite email with setup link
```

**Migration Strategy (Shared Schema):**
- All tenants share schema → standard migration tools (Alembic or Django migrations).
- Add columns carefully – always use nullable or default values.
- Use feature flags to roll out new columns per tenant.

**Migration Strategy (Separate Schema):**
```python
async def migrate_all_tenants(pool):
    async with pool.acquire() as conn:
        tenants = await conn.fetch("SELECT schema_name FROM tenants")
    for tenant in tenants:
        async with pool.acquire() as conn, conn.transaction():
            # Quote the registered schema as an identifier and scope it to this transaction.
            await conn.fetchval(
                "SELECT set_config('search_path', quote_ident($1::text), true)",
                tenant["schema_name"],
            )
            await run_pending_migrations(conn)
```

---

### Q: What is tenant isolation in terms of rate limiting and resource fairness?

**A:**

Without per-tenant limits, one noisy tenant can degrade service for all others.

**Solutions:**
1. **Per-tenant rate limiting** in API gateway (e.g., Redis token bucket keyed by `tenant_id`).
2. **Database connection pooling per tenant** (PgBouncer with per-tenant pools).
3. **Queue priority** – premium tenants get higher priority workers.
4. **Compute quotas** – limit expensive query time per tenant via `statement_timeout`.

```python
import time
from uuid import UUID

from redis.asyncio import Redis


async def within_rate_limit(redis: Redis, tenant_id: UUID, limit: int = 1_000) -> bool:
    minute = int(time.time()) // 60
    key = f"rate:tenant:{tenant_id}:{minute}"
    async with redis.pipeline(transaction=True) as pipe:
        pipe.incr(key)
        pipe.expire(key, 120)
        count, _ = await pipe.execute()
    return count <= limit
```

---

## 2. WebSockets & Real-Time Systems

### What is a WebSocket?
A persistent, full-duplex TCP connection between client and server. Unlike HTTP, both sides can push messages at any time.

---

### Q: How does WebSocket differ from HTTP polling and SSE?

**A:**

| Feature | HTTP Polling | Server-Sent Events (SSE) | WebSocket |
|---|---|---|---|
| Direction | Client pulls | Server pushes only | Bidirectional |
| Protocol | HTTP | HTTP | WS/WSS |
| Connection | New per request | Single persistent | Single persistent |
| Overhead | High | Low | Very low |
| Use Case | Simple refresh | Notifications, feeds | Games, chat, collaboration |

**When to use WebSocket:** Real-time multiplayer, collaborative editing, live dashboards, chat.
**When to use SSE:** One-way notifications, live logs, news feeds.

---

### Q: How would you implement WebSockets in a FastAPI application?

**A:**

```python
from uuid import UUID

from fastapi import APIRouter, Depends, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field, ValidationError

router = APIRouter()


class Answer(BaseModel):
    choice_id: UUID
    round_number: int = Field(ge=1)


@router.websocket("/game/{tenant_id}/{session_id}")
async def game_socket(
    socket: WebSocket, tenant_id: UUID, session_id: UUID,
    actor: Actor = Depends(websocket_actor),
):
    if not await games.can_join(tenant_id, session_id, actor.user_id):
        await socket.close(code=1008)
        return

    room = f"game:tenant:{tenant_id}:session:{session_id}"
    await socket.accept()
    await rooms.add(room, socket)
    try:
        while True:
            try:
                answer = Answer.model_validate_json(await socket.receive_text())
            except ValidationError:
                await socket.send_json({"event": "invalid_answer"})
                continue

            # Recheck permission, round deadline, and duplicate submissions on write.
            score = await games.submit_answer(tenant_id, session_id, actor.user_id, answer)
            await publish_update(redis, room, {"event": "score_update", "score": score})
    except WebSocketDisconnect:
        pass
    finally:
        await rooms.remove(room, socket)
```

---

### Q: How do you scale WebSockets horizontally across multiple servers?

**A:**

The problem: Client A connects to Server 1. Client B connects to Server 2. If Server 1 emits to "room", Client B won't receive it.

**Solution: Redis Pub/Sub**

```python
import json

from redis.asyncio import Redis


async def publish_update(redis: Redis, room: str, event: dict) -> None:
    await redis.publish(f"ws:{room}", json.dumps(event))


async def forward_updates(redis: Redis, rooms) -> None:
    # Run one listener per API worker under the FastAPI lifespan.
    # Configure the Redis client with decode_responses=True.
    async with redis.pubsub() as subscriber:
        await subscriber.psubscribe("ws:game:tenant:*")
        async for message in subscriber.listen():
            if message["type"] == "pmessage":
                room = message["channel"].removeprefix("ws:")
                await rooms.broadcast(room, json.loads(message["data"]))
```

**How it works:**
1. Server 1 emits to Redis channel.
2. Redis broadcasts to all subscribed servers.
3. All servers relay to their connected clients.

**Additional scaling strategies:**
- A native WebSocket stays on the worker that accepted it. On reconnect, reload the latest state and allow any worker to accept the connection.
- **Separate WebSocket cluster** from REST API cluster.
- Consider managed services: **Ably**, **Pusher**, **AWS API Gateway WebSocket**.

---

### Q: How do you handle state synchronization in a real-time multiplayer game?

**A:**

**Key Challenges:**
- Network latency causes state inconsistencies.
- Multiple clients mutating shared state simultaneously.
- Server must be single source of truth.

**Architecture:**
```
Client → sends intent/action → Server validates → Server updates canonical state
                                                  → Server broadcasts to all clients
```

**State Sync Pattern:**
```python
from typing import Any

from pydantic import BaseModel
from redis.exceptions import WatchError


class GameState(BaseModel):
    round_number: int
    version: int
    players: dict[str, int]
    question: dict[str, Any]
    ends_at_ms: int


async def apply_answer(redis, tenant_id, session_id, user_id, answer):
    key = f"game:tenant:{tenant_id}:session:{session_id}"
    for _ in range(5):
        try:
            async with redis.pipeline(transaction=True) as pipe:
                await pipe.watch(key)
                raw = await pipe.get(key)
                if raw is None:
                    raise LookupError("Session not found")
                state = GameState.model_validate_json(raw)
                # Validate the round, deadline, and player before computing the next state.
                next_state = validate_and_score(state, user_id, answer)
                next_state.version = state.version + 1
                pipe.multi()
                pipe.set(key, next_state.model_dump_json(), keepttl=True)
                await pipe.execute()
                return next_state
        except WatchError:
            continue
    raise RuntimeError("Concurrent updates prevented saving the answer")
```

**Conflict resolution:** Last-write-wins for low-stakes; server-authoritative for scores.

---

### Q: How do you implement a countdown timer in a real-time multiplayer session?

**A:**

**Anti-pattern:** Each client runs its own timer → drift, cheating.

**Correct approach: Server-driven timer**

```python
from datetime import datetime, timedelta, timezone


async def start_round(tenant_id, session_id, round_number, duration_seconds):
    ends_at = datetime.now(timezone.utc) + timedelta(seconds=duration_seconds)
    await games.save_round_deadline(tenant_id, session_id, round_number, ends_at)
    await publish_update(
        redis,
        f"game:tenant:{tenant_id}:session:{session_id}",
        {"event": "round_started", "ends_at": ends_at.isoformat()},
    )
    # Celery task closes the round only if it is still active and the deadline passed.
    close_round.apply_async(
        args=[str(tenant_id), str(session_id), round_number], eta=ends_at
    )
```

---

## 3. Supabase Architecture

### Q: What is Supabase and how does it work under the hood?

**A:**

Supabase is an open-source Firebase alternative built on top of:
- **PostgreSQL** – primary data store
- **PostgREST** – auto-generates REST API from Postgres schema
- **Realtime** – broadcasts DB changes via WebSockets
- **GoTrue** – auth service
- **Storage** – file storage built on S3-compatible backends
- **Edge Functions** – Deno-based serverless functions

**Key insight:** Supabase is just PostgreSQL + tooling. Everything you know about Postgres applies.

---

### Q: How does Supabase Realtime work?

**A:**

Supabase Realtime uses **PostgreSQL Logical Replication** to stream DB changes.

```
DB Write → WAL (Write-Ahead Log) → Logical Replication Slot 
        → Realtime Server → WebSocket → Client
```

**Python subscription using the Supabase client:**
```python
from supabase import acreate_client


async def watch_scores(url, publishable_key, access_token, refresh_token, on_score):
    client = await acreate_client(url, publishable_key)
    await client.auth.set_session(access_token, refresh_token)
    channel = client.channel("game-scores")
    await channel.on_postgres_changes(
        "INSERT", schema="public", table="player_scores", callback=on_score
    ).subscribe()
    return client, channel  # Keep the client alive and remove the channel on shutdown.
```

**Multi-tenant Realtime:**
```python
async def watch_tenant_tasks(client, tenant_id, on_task):
    # The authenticated client's RLS policies enforce access.
    channel = client.channel(f"tenant:{tenant_id}:tasks")
    for event in ("INSERT", "UPDATE"):
        channel.on_postgres_changes(
            event, schema="public", table="tasks",
            filter=f"tenant_id=eq.{tenant_id}", callback=on_task,
        )
    await channel.subscribe()
    return channel
```

---

### Q: How do you implement Row Level Security in Supabase?

**A:**

```sql
-- Enable RLS on the table
ALTER TABLE tasks ENABLE ROW LEVEL SECURITY;

-- Policy: users can only see their tenant's tasks
CREATE POLICY "tenant_isolation" ON tasks
  FOR ALL
  USING (
    tenant_id = (
      SELECT tenant_id FROM users 
      WHERE id = auth.uid()
    )
  );

-- Policy: only task owner can delete
CREATE POLICY "owner_delete" ON tasks
  FOR DELETE
  USING (created_by = auth.uid());
```

**Important:** RLS only fires when using `anon` or `authenticated` roles. When using service role key (backend), RLS is bypassed by default. Use `SET ROLE authenticated` or explicitly add tenant filtering in backend queries.

---

### Q: How do you use Supabase in a FastAPI backend vs using it directly from the client?

**A:**

**Direct client use (frontend):**
- Anon key exposed in browser.
- RLS enforces access control.
- Good for reads and user-specific writes.

**Backend use (FastAPI with service key):**
- Service key bypasses RLS → must manually enforce tenant isolation.
- Required for: admin operations, bulk processing, cross-tenant aggregation.

```python
import os
from uuid import UUID

from supabase import create_client

admin_client = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])


def list_tasks_for_admin_job(tenant_id: UUID) -> list[dict]:
    # Call only from an authorized backend job that already verified its tenant scope.
    response = (
        admin_client.table("tasks").select("id,title").eq("tenant_id", str(tenant_id))
        .execute()
    )
    return response.data
```

---

## 4. Role-Based Access Control (RBAC)

### Q: What is RBAC and how do you implement it in a multi-tenant SaaS?

**A:**

RBAC assigns permissions to roles, and roles to users. A user's access is derived from their role.

**Schema design:**
```sql
-- Core tables
CREATE TABLE roles (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID REFERENCES tenants(id),
  name TEXT NOT NULL, -- 'admin', 'moderator', 'player'
  UNIQUE(tenant_id, name)
);

CREATE TABLE permissions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  resource TEXT NOT NULL,  -- 'session', 'leaderboard', 'user'
  action TEXT NOT NULL,    -- 'create', 'read', 'update', 'delete'
  UNIQUE(resource, action)
);

CREATE TABLE role_permissions (
  role_id UUID REFERENCES roles(id),
  permission_id UUID REFERENCES permissions(id),
  PRIMARY KEY (role_id, permission_id)
);

CREATE TABLE user_roles (
  user_id UUID REFERENCES users(id),
  role_id UUID REFERENCES roles(id),
  tenant_id UUID REFERENCES tenants(id),
  PRIMARY KEY (user_id, role_id, tenant_id)
);
```

**FastAPI permission dependency:**
```python
from fastapi import APIRouter, Depends, HTTPException

router = APIRouter()


def require_permission(permission: str):
    async def check(actor: Actor = Depends(current_actor), tenant_id=Depends(current_tenant)):
        permissions = await rbac.permissions_for(actor.user_id, tenant_id)
        if permission not in permissions:
            raise HTTPException(status_code=403, detail="Permission denied")
    return check


@router.get("/sessions", dependencies=[Depends(require_permission("session:read"))])
async def get_sessions(tenant_id=Depends(current_tenant)):
    return await sessions.list_for_tenant(tenant_id)


@router.post("/sessions", dependencies=[Depends(require_permission("session:create"))])
async def create_session(data: SessionInput, tenant_id=Depends(current_tenant)):
    return await sessions.create(tenant_id, data)
```

---

### Q: What is the difference between RBAC and ABAC?

**A:**

**RBAC (Role-Based):** Access based on *who you are* (your role).
**ABAC (Attribute-Based):** Access based on *attributes* of user, resource, and environment.

| Aspect | RBAC | ABAC |
|---|---|---|
| Complexity | Low | High |
| Flexibility | Medium | High |
| Policy example | "Admins can delete" | "Managers can delete tasks they own, in their dept, during business hours" |
| Use case | Standard SaaS | Healthcare, finance, fine-grained access |

For Knowsy: RBAC is sufficient. Tenants have roles (admin, host, player). Fine-grained rules can be layered as needed.

---

## 5. PostgreSQL Optimization & Indexing

### Q: What types of indexes does PostgreSQL support and when do you use each?

**A:**

**B-Tree (default):** For equality and range queries. Most common.
```sql
CREATE INDEX idx_tasks_tenant_id ON tasks(tenant_id);
CREATE INDEX idx_tasks_created_at ON tasks(created_at DESC);

-- Composite index – order matters
-- Good for: WHERE tenant_id = ? AND status = ?
CREATE INDEX idx_tasks_tenant_status ON tasks(tenant_id, status);
```

**GIN (Generalized Inverted Index):** For JSONB, arrays, full-text search.
```sql
-- JSONB search
CREATE INDEX idx_metadata ON tasks USING GIN(metadata);
SELECT * FROM tasks WHERE metadata @> '{"priority": "high"}';

-- Full-text search
CREATE INDEX idx_fts ON tasks USING GIN(to_tsvector('english', title));
SELECT * FROM tasks WHERE to_tsvector('english', title) @@ to_tsquery('meeting');
```

**Partial Index:** Index only a subset of rows.
```sql
-- Only index active tasks – smaller, faster
CREATE INDEX idx_active_tasks ON tasks(tenant_id)
WHERE status = 'active';
```

**Covering Index (INCLUDE):** Avoid table heap fetch.
```sql
-- Query can be answered from index alone
CREATE INDEX idx_tasks_covering ON tasks(tenant_id)
INCLUDE (title, status, created_at);
```

---

### Q: How do you diagnose a slow query?

**A:**

```sql
-- Step 1: EXPLAIN ANALYZE
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT * FROM tasks 
WHERE tenant_id = 'abc' AND status = 'active'
ORDER BY created_at DESC
LIMIT 20;

-- Look for:
-- Seq Scan → missing index
-- Nested Loop on large sets → missing join index
-- High "actual rows" vs "estimated rows" → stale statistics → run ANALYZE
-- Buffers hit vs read → cache miss → hot data not in shared_buffers
```

**Step 2: Check pg_stat_statements:**
```sql
-- Top slow queries
SELECT query, mean_exec_time, calls, total_exec_time
FROM pg_stat_statements
ORDER BY mean_exec_time DESC
LIMIT 10;
```

**Step 3: Update statistics if needed:**
```sql
ANALYZE tasks;
-- Or configure autovacuum properly
```

---

### Q: How do you design a schema for a leaderboard that updates in real-time for a multi-tenant game?

**A:**

```sql
-- Sessions table
CREATE TABLE game_sessions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  status TEXT DEFAULT 'waiting', -- waiting, active, completed
  started_at TIMESTAMPTZ,
  ended_at TIMESTAMPTZ
);

-- Scores table
CREATE TABLE player_scores (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  session_id UUID NOT NULL REFERENCES game_sessions(id),
  tenant_id UUID NOT NULL,
  user_id UUID NOT NULL,
  score INT DEFAULT 0,
  rank INT,
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for leaderboard queries
CREATE INDEX idx_scores_session ON player_scores(session_id, score DESC);
CREATE INDEX idx_scores_tenant ON player_scores(tenant_id, score DESC);

-- Leaderboard query
SELECT 
  u.display_name,
  ps.score,
  RANK() OVER (PARTITION BY ps.session_id ORDER BY ps.score DESC) as rank
FROM player_scores ps
JOIN users u ON u.id = ps.user_id
WHERE ps.session_id = $1
ORDER BY ps.score DESC;
```

**For real-time updates:** Combine this with Supabase Realtime subscription on `player_scores` filtered by `session_id`.

---

### Q: What is connection pooling and why is it critical in multi-tenant SaaS?

**A:**

PostgreSQL spawns a process per connection. Thousands of connections → memory exhaustion.

**Solution: PgBouncer**
- Maintains a fixed pool of DB connections.
- Application connects to PgBouncer (cheap).
- PgBouncer multiplexes onto real DB connections.

**Modes:**
- **Session pooling:** One DB connection per client session. Safe but less efficient.
- **Transaction pooling:** DB connection returned after each transaction. Most efficient. Incompatible with session-level features like `SET` statements.
- **Statement pooling:** Most aggressive. Rarely used.

```
FastAPI (100 instances) → PgBouncer (20 real PG connections) → PostgreSQL
```

**Configuration:**
```ini
; pgbouncer.ini
[databases]
knowsy = host=localhost dbname=knowsy

[pgbouncer]
pool_mode = transaction
max_client_conn = 1000
default_pool_size = 20
```

---

## 6. Event-Driven Architecture

### Q: What is event-driven architecture and when should you use it?

**A:**

Instead of services calling each other directly (synchronous), services emit events and others react asynchronously.

**When to use:**
- Decoupling services (game service shouldn't know about analytics service)
- Background processing (score calculation, email sending)
- Fan-out (one event → multiple consumers)
- Audit trails

**When NOT to use:**
- When you need an immediate response (REST/RPC is simpler)
- Simple CRUD with no downstream effects

---

### Q: How would you implement event-driven communication in FastAPI?

**A:**

**Option 1: In-process event handlers (no broker)**
```python
from dataclasses import dataclass
from uuid import UUID


@dataclass(frozen=True)
class RoundCompleted:
    tenant_id: UUID
    session_id: UUID
    round_number: int


async def notify_round_completed(event: RoundCompleted) -> None:
    # In-process handlers run in the same worker and have no durable delivery.
    await analytics.record_round(event)
    await notifications.record_round(event)
```

**Option 2: Celery with a broker (cross-process)**
```python
from uuid import UUID

from app.celery_app import celery_app


@celery_app.task(autoretry_for=(TimeoutError,), retry_backoff=True, max_retries=3)
def record_round_analytics(tenant_id: str, session_id: str, round_number: int):
    # Upsert against a unique tenant/session/round key so retries are safe.
    analytics.upsert_round(UUID(tenant_id), UUID(session_id), round_number)


def enqueue_round_analytics(event: RoundCompleted) -> None:
    record_round_analytics.apply_async(
        args=[str(event.tenant_id), str(event.session_id), event.round_number],
        queue="analytics",
    )
```

**Option 3: Kafka/RabbitMQ for true distributed event streaming at scale.**

---

### Q: What is the outbox pattern and why is it important?

**A:**

**Problem:** You save to DB and then publish an event. If the publish fails, the event is lost. If you publish first and the DB save fails, you have a ghost event.

**Outbox Pattern – Guarantee at-least-once delivery:**

```sql
-- Events stored in DB in same transaction as business data
CREATE TABLE outbox_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  event_type TEXT NOT NULL,
  payload JSONB NOT NULL,
  published BOOLEAN DEFAULT FALSE,
  created_at TIMESTAMPTZ DEFAULT NOW()
);
```

```python
import json


async def end_session(pool, tenant_id, session_id):
    async with pool.acquire() as conn, conn.transaction():
        updated = await conn.fetchval(
            '''
            UPDATE game_sessions SET status = 'completed', ended_at = now()
            WHERE tenant_id = $1 AND id = $2 AND status = 'active'
            RETURNING id
            ''',
            tenant_id, session_id,
        )
        if updated is not None:
            await conn.execute(
                "INSERT INTO outbox_events (event_type, payload) VALUES ($1, $2::jsonb)",
                "session.completed",
                json.dumps({"tenantId": str(tenant_id), "sessionId": str(session_id)}),
            )


async def publish_outbox_batch(pool, broker):
    async with pool.acquire() as conn, conn.transaction():
        events = await conn.fetch(
            '''
            SELECT id, event_type, payload FROM outbox_events WHERE published = FALSE
            ORDER BY created_at, id LIMIT 100 FOR UPDATE SKIP LOCKED
            '''
        )
        for event in events:
            # Wait for the broker's acknowledgement; consumers deduplicate by event ID.
            await broker.publish(event["event_type"], event["payload"], event_id=str(event["id"]))
            await conn.execute(
                "UPDATE outbox_events SET published = TRUE WHERE id = $1", event["id"]
            )
```

---

## 7. API Design & Authentication

### Q: How do you design a secure, tenant-aware REST API?

**A:**

**URL design:**
```
# Tenant identified via subdomain (preferred) or header
GET https://acme.knowsy.com/api/sessions
GET https://api.knowsy.com/api/sessions  (with header: X-Tenant-ID: acme)
```

**JWT structure for multi-tenant:**
```json
{
  "sub": "user-uuid",
  "tenantId": "tenant-uuid",
  "roles": ["admin"],
  "plan": "pro",
  "iat": 1700000000,
  "exp": 1700003600
}
```

**Auth flow:**
```
1. Client sends credentials → POST /auth/login
2. Server validates → issues Access Token (15 min) + Refresh Token (7 days)
3. Client stores Refresh Token in httpOnly cookie (not localStorage)
4. Access Token in memory only
5. On expiry → POST /auth/refresh using cookie
6. On logout → invalidate Refresh Token in DB/Redis
```

**FastAPI JWT authentication dependency:**
```python
from dataclasses import dataclass
from typing import Annotated
from uuid import UUID

import jwt
from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

bearer = HTTPBearer()


@dataclass(frozen=True)
class Actor:
    user_id: UUID
    tenant_id: UUID


async def current_actor(
    credentials: Annotated[HTTPAuthorizationCredentials, Depends(bearer)],
) -> Actor:
    try:
        claims = jwt.decode(
            credentials.credentials, settings.jwt_public_key, algorithms=["RS256"],
            audience=settings.jwt_audience, issuer=settings.jwt_issuer,
            options={"require": ["exp", "sub", "tenantId"]},
        )
        return Actor(UUID(claims["sub"]), UUID(claims["tenantId"]))
    except (jwt.InvalidTokenError, KeyError, ValueError, TypeError) as exc:
        raise HTTPException(
            status_code=401, detail="Invalid access token",
            headers={"WWW-Authenticate": "Bearer"},
        ) from exc
```

---

### Q: How do you handle API versioning?

**A:**

**URL versioning (most common, most visible):**
```
/api/v1/sessions
/api/v2/sessions
```

**Header versioning:**
```
GET /api/sessions
Accept: application/vnd.knowsy.v2+json
```

**FastAPI URL versioning:**
```python
from fastapi import APIRouter, FastAPI

from app.game.api_v1 import router as sessions_v1
from app.game.api_v2 import router as sessions_v2

app = FastAPI()
for version, sessions_router in (("v1", sessions_v1), ("v2", sessions_v2)):
    api = APIRouter(prefix=f"/api/{version}")
    api.include_router(sessions_router, prefix="/sessions")
    app.include_router(api)
```

**Strategy:** Never break v1. Deprecate with Sunset header. Maintain 2 versions max.

---

## 8. Scalable Backend Design & FastAPI

### Q: How do you structure a FastAPI application for a large-scale SaaS product?

**A:**

```text
app/
  main.py                # FastAPI application and router registration
  config.py
  database.py
  celery_app.py
  auth/
    dependencies.py      # Verified identity and permission checks
    service.py
  tenants/
    dependencies.py      # Active tenant membership
    repository.py
  game/
    router.py
    websocket.py
    service.py
    repository.py
    schemas.py           # Pydantic request and response models
  analytics/
    tasks.py             # Celery worker tasks
tests/
  integration/
```

**Key patterns:**
- **CQRS** (Command Query Responsibility Segregation) for complex game logic.
- **Repository pattern** to decouple data access.
- **DTO validation** with `class-validator` on every endpoint.
- **Interceptors** for response transformation and logging.

---

### Q: How do you handle background jobs and scheduled tasks in FastAPI?

**A:**

```python
import os

from celery import Celery
from celery.schedules import crontab

celery_app = Celery("knowsy", broker=os.environ["CELERY_BROKER_URL"])
celery_app.conf.timezone = "UTC"
celery_app.conf.beat_schedule = {
    "clean-expired-sessions": {
        "task": "maintenance.clean_expired_sessions",
        "schedule": crontab(hour=2, minute=0),
    },
    "sync-leaderboards": {
        "task": "maintenance.sync_leaderboards",
        "schedule": 60.0,
    },
}


@celery_app.task(name="maintenance.clean_expired_sessions")
def clean_expired_sessions():
    # A dedicated maintenance job iterates tenants and deletes in bounded batches.
    maintenance.delete_old_sessions(retention_days=90)


@celery_app.task(name="maintenance.sync_leaderboards")
def sync_leaderboards():
    maintenance.refresh_tenant_leaderboards()
```

---

## 9. System Design – Multiplayer/Real-Time Platform

### Q: Design the architecture for Knowsy – a real-time multiplayer quiz platform with multi-tenant SaaS.

**A:**

**Requirements (derived from JD):**
- Multiple tenants (companies), each running independent sessions
- Real-time ranking, scoring, VIP selection
- Supports hundreds of concurrent players per session
- Analytics dashboard for hosts

**High-Level Architecture:**

```
                    ┌─────────────────────────────┐
                    │         Client (React)       │
                    └────────────┬────────────────┘
                                 │ HTTPS / WSS
                    ┌────────────▼────────────────┐
                    │    API Gateway / Load Balancer │
                    └───────────┬─────────────────┘
               ┌────────────────┼──────────────────┐
               │                │                  │
     ┌─────────▼──────┐ ┌───────▼──────┐ ┌────────▼────────┐
     │  REST API      │ │  WebSocket   │ │  Auth Service   │
     │  (FastAPI)      │ │  Gateway     │ │  (GoTrue/Custom)│
     └─────────┬──────┘ └───────┬──────┘ └────────┬────────┘
               │                │                  │
     ┌─────────▼────────────────▼──────────────────▼────────┐
     │                    Redis                              │
     │  - Session state, leaderboards, rate limiting        │
     │  - Pub/Sub for WS horizontal scaling                 │
     │  - Celery job queues                                 │
     └───────────────────────────┬───────────────────────────┘
                                 │
     ┌───────────────────────────▼───────────────────────────┐
     │                  PostgreSQL (Supabase)                 │
     │  - Tenants, Users, Sessions, Scores, Analytics        │
     │  - Row Level Security for tenant isolation            │
     │  - Logical Replication → Supabase Realtime            │
     └───────────────────────────────────────────────────────┘
```

**Session Lifecycle:**
```
Host creates session → Generate session code → Players join via code
→ Host starts round → Server broadcasts question + timer (server-side)
→ Players submit answers → Server validates + updates scores atomically
→ Server broadcasts leaderboard update
→ Round ends → Server saves results to DB → triggers analytics events
```

**Leaderboard Update Strategy:**
1. Player submits answer → REST POST.
2. Server atomically updates Redis sorted set: `ZADD leaderboard:{sessionId} score userId`.
3. Server reads top 10: `ZREVRANGE leaderboard:{sessionId} 0 9 WITHSCORES`.
4. Server emits `leaderboardUpdate` to WebSocket room.
5. At session end: persist Redis scores → PostgreSQL.

**VIP Selection Logic:**
- VIP = highest scorer or randomly selected from top 3.
- Determined server-side only (never trust client).
- Broadcast VIP announcement via WebSocket.

**Scaling:**
- Horizontal scaling of REST + WS servers behind load balancer.
- Redis Pub/Sub across WebSocket workers.
- Read replicas for analytics queries.
- CDN for static assets.

---

### Q: How would you handle 10,000 concurrent users on this platform?

**A:**

**Bottlenecks to address:**

1. **WebSocket connections:**
   - Load-test connection capacity per ASGI worker using representative message rates and payload sizes.
   - Scale horizontally; use Redis Pub/Sub for cross-instance messaging.
   - Group players by session → targeted broadcasts (not broadcast-all).

2. **Database:**
   - PgBouncer for connection pooling.
   - Separate read replicas for analytics/reporting.
   - Redis for hot data (leaderboards, session state).
   - Batch writes: buffer score updates every 500ms instead of per-answer.

3. **API layer:**
   - Rate limiting per tenant.
   - Cache session metadata in Redis (TTL 60s).
   - Use queue for non-critical operations (analytics, emails).

4. **Infrastructure:**
   - Auto-scaling groups (GCP Cloud Run or GKE).
   - Health checks + graceful shutdown.
   - Circuit breakers for downstream services.

---

## 10. AI Workflow & Code Review

### Q: How do you validate AI-generated code before merging to production?

**A:**

This is a high-value skill for this role. AI tools generate code fast but introduce subtle bugs.

**Validation Framework:**

**1. Logic correctness**
- Does the code do what was asked?
- Edge cases: empty arrays, null values, off-by-one errors.
- Ask: "What happens when the input is empty/null/huge?"

**2. Security review**
```python
async def get_user(conn, tenant_id, user_id):
    # Pass values separately from the SQL statement.
    return await conn.fetchrow(
        "SELECT id, display_name FROM users WHERE tenant_id = $1 AND id = $2",
        tenant_id, user_id,
    )
```

Common AI security mistakes:
- SQL injection via string concatenation.
- Missing input validation.
- Exposed secrets in code (hardcoded API keys).
- Missing auth checks on routes.
- JWT verified but `tenantId` not checked.

**3. Performance issues**
```python
async def list_sessions_with_scores(conn, tenant_id):
    return await conn.fetch(
        '''
        SELECT s.id,
               COALESCE(
                   jsonb_agg(jsonb_build_object('user_id', p.user_id, 'score', p.score))
                       FILTER (WHERE p.id IS NOT NULL),
                   '[]'::jsonb
               ) AS scores
        FROM game_sessions AS s
        LEFT JOIN player_scores AS p ON p.session_id = s.id AND p.tenant_id = s.tenant_id
        WHERE s.tenant_id = $1
        GROUP BY s.id
        ''',
        tenant_id,
    )
```

**4. Architecture violations**
- Business logic leaking into controllers.
- Direct DB access from route handlers (should go through service layer).
- Missing error handling / unhandled exceptions in async tasks.

**5. Type safety**
```python
from uuid import UUID

from pydantic import BaseModel, Field


class PlayerScore(BaseModel):
    player_id: UUID
    score: int = Field(ge=0)


def parse_score(raw: str) -> PlayerScore:
    return PlayerScore.model_validate_json(raw)
```

**Your review checklist:**
```text
□ Does it pass the Python type checker?
□ Are all inputs validated (Pydantic)?
□ Are all DB queries parameterized?
□ Is auth/authorization checked?
□ Are errors handled?
□ Is there a test for this logic?
□ Does it respect the service layer architecture?
□ Any hardcoded values that should be config?
□ Performance: any N+1 queries or missing indexes?
```

---

### Q: What are the risks of using AI-generated code in production systems?

**A:**

1. **Hallucinated APIs:** AI uses methods that don't exist or have wrong signatures.
2. **Outdated patterns:** Training data may reflect older library versions.
3. **Security vulnerabilities:** AI doesn't always apply OWASP principles.
4. **Missing business context:** AI doesn't know your domain rules (e.g., only one active session per tenant).
5. **Over-confidence:** AI code looks clean and confident but may be subtly wrong.
6. **Test coverage gaps:** AI may generate code without proper tests.

**Best practice:**
- Use AI for boilerplate/scaffolding, review everything.
- Never use AI for security-critical paths without deep review.
- Treat AI code as "written by a junior who doesn't know the codebase."

---

### Q: How do you review AI-generated database migrations?

**A:**

Database migrations are high-risk. Bad migrations can corrupt data or cause downtime.

**Review checklist:**
```
□ Is it reversible? (DOWN migration defined)
□ Does it lock the table? (ALTER TABLE on large tables is dangerous)
□ Missing DEFAULT on new NOT NULL column → will fail on existing rows
□ Dropping a column in use by application code
□ Index created without CONCURRENTLY → locks table
□ Foreign key without index on referencing column
```

**Examples:**
```sql
-- AI might generate (DANGEROUS on large table – locks):
ALTER TABLE tasks ADD COLUMN priority INT NOT NULL;

-- Correct approach:
ALTER TABLE tasks ADD COLUMN priority INT; -- nullable first
UPDATE tasks SET priority = 0;            -- backfill
ALTER TABLE tasks ALTER COLUMN priority SET NOT NULL; -- then constrain
ALTER TABLE tasks ALTER COLUMN priority SET DEFAULT 0;

-- AI might generate (locks table):
CREATE INDEX idx_tasks_status ON tasks(status);

-- Correct (no lock):
CREATE INDEX CONCURRENTLY idx_tasks_status ON tasks(status);
```

---

## 11. Distributed Systems Basics

### Q: What is CAP theorem and how does it apply to a real-time game platform?

**A:**

**CAP Theorem:** A distributed system can only guarantee two of three:
- **Consistency (C):** All nodes see same data at same time.
- **Availability (A):** System always responds.
- **Partition Tolerance (P):** System works despite network failures.

Since network partitions always happen in practice, you choose C or A.

**For Knowsy:**
- **Leaderboard:** Prefer Availability. Slightly stale score display is acceptable. (AP)
- **Score submission:** Prefer Consistency. A submitted answer must be counted. (CP)
- **Session state:** Prefer Consistency. Everyone must see same round state. (CP)

---

### Q: What is eventual consistency? Where would you apply it in this platform?

**A:**

Data will become consistent *eventually* (not immediately). Acceptable when slight delay doesn't break user experience.

**Apply eventual consistency in Knowsy:**
- **Analytics:** It's okay if dashboards are 30 seconds behind.
- **Notification delivery:** Push notification may arrive a second late.
- **Leaderboard cache:** Redis leaderboard refreshed every 500ms from DB.

**Do NOT use eventual consistency for:**
- Score submission (must be accurate).
- Auth/session validation.
- Payment processing.

---

### Q: What is a circuit breaker pattern?

**A:**

Prevents cascading failures when a downstream service is slow or down.

```
States: Closed (normal) → Open (failing, reject calls) → Half-Open (test recovery)
```

```python
import httpx
from pybreaker import CircuitBreaker, CircuitBreakerError

breaker = CircuitBreaker(fail_max=5, reset_timeout=30)


@breaker
def fetch_standings(http: httpx.Client, url: str) -> dict:
    response = http.get(url, timeout=3.0)
    response.raise_for_status()
    return response.json()


def standings(http: httpx.Client, url: str, cached: dict) -> dict:
    try:
        return fetch_standings(http, url)
    except (CircuitBreakerError, httpx.HTTPError):
        return cached
```

---

## 12. Analytics Architecture

### Q: How would you design an analytics system for a multiplayer game platform?

**A:**

**Requirements:** Track player behavior, session performance, engagement metrics. Support historical reports and real-time dashboards.

**Architecture:**

```
Game Events → Celery Queue → Analytics Worker → Write to analytics DB
                                              → Supabase Realtime → Dashboard
```

**Event Schema (append-only, never update):**
```sql
CREATE TABLE analytics_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID NOT NULL,
  session_id UUID,
  user_id UUID,
  event_type TEXT NOT NULL,  -- 'answer_submitted', 'session_joined', 'vip_selected'
  properties JSONB,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Partition by month for performance
CREATE TABLE analytics_events_2024_11 PARTITION OF analytics_events
FOR VALUES FROM ('2024-11-01') TO ('2024-12-01');
```

**Key metrics:**
```sql
-- Average score per session
SELECT session_id, AVG(score) FROM player_scores GROUP BY session_id;

-- Player retention (came back within 7 days)
SELECT COUNT(DISTINCT user_id) FROM analytics_events
WHERE event_type = 'session_joined'
  AND user_id IN (SELECT user_id FROM analytics_events WHERE occurred_at < NOW() - INTERVAL '7 days')
  AND occurred_at > NOW() - INTERVAL '7 days';

-- Tenant engagement
SELECT tenant_id, COUNT(*) as sessions, COUNT(DISTINCT user_id) as players
FROM analytics_events
WHERE event_type = 'session_joined'
  AND occurred_at > NOW() - INTERVAL '30 days'
GROUP BY tenant_id;
```

**For large scale:** Move analytics to ClickHouse or BigQuery. Keep PostgreSQL for operational data.

---

## 13. GCP Basics

### Q: What GCP services would you use to deploy and scale this platform?

**A:**

| Service | Use |
|---|---|
| **Cloud Run** | Deploy stateless REST API (auto-scaling, pay per use) |
| **GKE (Kubernetes)** | Deploy WebSocket workers and scale them based on measured load |
| **Cloud SQL / Supabase** | PostgreSQL managed database |
| **Memorystore (Redis)** | Managed Redis for sessions, queues, pub/sub |
| **Cloud Pub/Sub** | Durable event streaming between services |
| **Cloud Storage** | Static assets, file uploads |
| **Cloud CDN** | Cache static assets globally |
| **Cloud Armor** | WAF (Web Application Firewall), DDoS protection |
| **Secret Manager** | Store API keys, DB passwords |
| **Cloud Monitoring** | Metrics, alerting, dashboards |

**Deployment pipeline:**
```
GitHub Push → Cloud Build (CI/CD) → Docker Image → Artifact Registry
                                  → Deploy to Cloud Run / GKE
```

---

## 14. Security

### Q: What are the top security concerns for a multi-tenant SaaS platform?

**A:**

**1. Tenant isolation failure (highest risk)**
- Missing WHERE tenant_id clause.
- Fix: RLS + repository pattern + integration tests.

**2. Broken authentication**
- Weak JWT secret.
- Not validating token expiry.
- Fix: Strong secrets, short-lived tokens, httpOnly cookies for refresh.

**3. Injection attacks**
- SQL injection via string interpolation.
- Fix: Always use parameterized queries.

**4. IDOR (Insecure Direct Object Reference)**
```python
from fastapi import HTTPException


async def get_player_score(conn, tenant_id, session_id, user_id):
    score = await conn.fetchrow(
        '''
        SELECT id, score FROM player_scores
        WHERE tenant_id = $1 AND session_id = $2 AND user_id = $3
        ''',
        tenant_id, session_id, user_id,
    )
    if score is None:
        raise HTTPException(status_code=404, detail="Score not found")
    return dict(score)
```

**5. Rate limiting abuse**
- Without rate limits, one tenant hammers the API.
- Fix: Per-tenant rate limits, IP-based limits.

**6. Secrets in code**
- AI tools sometimes embed API keys in generated code.
- Fix: Environment variables, GCP Secret Manager, pre-commit hooks to detect secrets.

**7. WebSocket authentication**
```python
from fastapi import WebSocket, WebSocketException, status


async def websocket_actor(socket: WebSocket) -> Actor:
    if socket.headers.get("origin") not in settings.allowed_web_origins:
        raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION)
    # This short-lived session is set by the login flow in an HttpOnly, Secure cookie.
    session_id = socket.cookies.get("session")
    actor = await auth.actor_for_session(session_id) if session_id else None
    if actor is None:
        raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION)
    return actor
```

---

## Quick Revision Cheat Sheet

| Topic | Key Points |
|---|---|
| Multi-tenancy | Row-level (tenant_id), Schema-level, DB-level. Use RLS in Postgres. |
| WebSockets | FastAPI WebSockets + Redis Pub/Sub for horizontal scaling. Server-authoritative state. |
| Supabase Realtime | PostgreSQL WAL → logical replication → WebSocket. RLS applies to Postgres Changes inserts and updates. |
| RBAC | Roles → Permissions → Users. FastAPI dependencies check tenant permissions. |
| PostgreSQL Indexing | B-Tree (default), GIN (JSONB/FTS), Partial, Covering. EXPLAIN ANALYZE. |
| Event-Driven | Python event handlers (in-process), Celery (cross-process), Outbox pattern for reliability. |
| Leaderboard | Redis sorted sets (ZADD, ZREVRANGE). Persist to DB on session end. |
| AI Code Review | Security, N+1 queries, missing validation, type safety, architecture violations. |
| CAP Theorem | Partition always happens. Choose C (accuracy) vs A (availability) per feature. |
| Circuit Breaker | Closed → Open → Half-Open. Prevent cascading failures. |
| Scaling WS | Redis Pub/Sub across workers. Reload state on reconnect. |
| Connection Pooling | PgBouncer. Transaction mode = most efficient. Max ~20 real connections. |

---

*Good luck with the interview. You've got this.*
