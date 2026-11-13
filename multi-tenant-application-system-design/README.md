# Multi-Tenant Application — Complete System Design Guide

> A comprehensive reference for system design interviews and real-world implementation.

The backend examples use Python with FastAPI, asyncpg, Redis, and Celery. The snippets assume your app provides verified identity, membership lookups, database connections, and worker configuration. See the official guides for [FastAPI authentication](https://fastapi.tiangolo.com/tutorial/security/oauth2-jwt/) and [Celery tasks](https://docs.celeryq.dev/en/stable/userguide/tasks.html).

---

## Table of Contents

1. [What is Multi-Tenancy?](#what-is-multi-tenancy)
2. [Real-World Examples](#real-world-examples)
3. [Architecture Types](#architecture-types)
4. [Core Components](#core-components)
5. [Database Design](#database-design)
6. [Scaling Challenges](#scaling-challenges)
7. [Security Challenges](#security-challenges)
8. [Deployment Challenges](#deployment-challenges)
9. [Advanced Topics](#advanced-topics)
10. [Tech Stack](#tech-stack)
11. [Interview Guide](#interview-guide)
12. [Common Interview Mistakes](#common-interview-mistakes)
13. [Best Production Practices](#best-production-practices)
14. [Quick-Reference Cheat Sheet](#quick-reference-cheat-sheet)

---

## What is Multi-Tenancy?

A **multi-tenant application** is a software architecture where a **single application instance** serves multiple customers (tenants).

Each tenant:
- Has **isolated data**
- May have **custom configuration**
- Uses the **same backend infrastructure**

**Analogy:** Think of it like an apartment building — one building (infrastructure), many tenants (customers), each with their own locked unit (data isolation).

---

## Real-World Examples

| Product | What it does |
|---|---|
| Salesforce | CRM SaaS for thousands of companies |
| Shopify | E-commerce platform per merchant |
| Slack | Messaging per workspace/organization |
| Notion | Productivity tool per team |

### Example Scenario: Task Management SaaS

Customers: **Company A**, **Company B**, **Company C**

All use:
- Same backend
- Same frontend
- Same database server

But:
- Company A **cannot** access Company B's data
- Each company has separate users, roles, and settings

---

## Architecture Types

### 1. Shared Database + Shared Schema ⭐ (Startup Default)

All tenants share the same DB and the same tables. Every table has a `tenant_id` column.

```sql
-- Tasks table
id | tenant_id | title
1  | 101       | Design homepage
2  | 202       | Write unit tests
```

**Advantages:**
- Cheapest to run
- Easy to scale horizontally
- Simple deployment
- Best for startups and early-stage SaaS

**Disadvantages:**

| Problem | Description |
|---|---|
| Data leakage risk | A missing `WHERE tenant_id = ?` exposes all tenant data |
| Noisy neighbor | One tenant's heavy load slows down all others |
| Hard customization | Custom fields/workflows make schema messy |

---

### 2. Shared Database + Separate Schema

Same database server, but each tenant gets their **own schema (namespace)**.

```
postgres (DB)
├── tenant_a.users
├── tenant_b.users
└── tenant_c.users
```

**Advantages:**
- Better data isolation than shared schema
- Easier per-tenant backup and restore
- Easier per-tenant migration
- Reduced accidental leakage risk

**Disadvantages:**

| Problem | Description |
|---|---|
| Schema explosion | 10,000 tenants = 10,000 schemas |
| Migration complexity | Must run migrations across all schemas |
| Tooling challenges | Most ORMs don't natively support schema-per-tenant |

---

### 3. Separate Database Per Tenant

Each tenant gets their **own dedicated database**.

```
tenant_a_db
tenant_b_db
tenant_c_db
```

**Advantages:**
- Strongest isolation (enterprise-grade)
- Easy compliance (GDPR, HIPAA, SOC 2)
- Independent scaling per tenant
- Simplest to delete a tenant (drop their DB)

**Disadvantages:**

| Problem | Description |
|---|---|
| Expensive | More DB servers, connections, monitoring |
| Operational overhead | Backup, migrations, and monitoring per tenant |
| Connection pool limits | Thousands of DBs = connection management nightmare |

---

### Which Architecture Should You Choose?

| Company Stage | Recommended Approach |
|---|---|
| Startup / MVP | Shared DB + Shared Schema |
| Mid-scale SaaS | Shared DB + Separate Schema |
| Enterprise SaaS | Separate DB per Tenant |
| Hybrid SaaS | Shared for SMB + Dedicated for Enterprise |

> **Interview Tip:** Always explain the tradeoffs before recommending. A junior answer picks one; a senior answer explains the decision based on tenant count, compliance needs, and budget.

---

## Core Components

### 1. Tenant Resolver

Determines **which tenant is making the request** before anything else runs.

**Resolution Methods:**

| Method | Example |
|---|---|
| Subdomain | `company-a.app.com`, `company-b.app.com` |
| JWT Claims | `{ "tenantId": 101 }` in the token |
| API Key | Mapped to a tenant in the DB |
| Custom Header | `X-Tenant-ID: 101` |
| URL Path | `/tenant/101/tasks` |

---

### 2. Authentication & Authorization

**Tenant-aware JWT structure:**

```json
{
  "userId": 10,
  "tenantId": 101,
  "role": "admin",
  "permissions": ["read:tasks", "write:tasks"]
}
```

**RBAC (Role-Based Access Control)** must be tenant-scoped:
- A user who is `admin` in Tenant A must have **no elevated access** in Tenant B.

---

### 3. Row-Level Security (RLS) — Critical

PostgreSQL RLS prevents accidental data leakage at the database level.

```sql
-- Create a policy on the tasks table
CREATE POLICY tenant_policy
ON tasks
USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::int);

-- Enable RLS
ALTER TABLE tasks ENABLE ROW LEVEL SECURITY;
```

Set the tenant context within the transaction and run requests under a database role subject to RLS. PostgreSQL then enforces the policy even if a query omits the tenant filter.

---

### 4. Tenant Context

A route dependency resolves the requested tenant and checks the signed-in user’s active membership before passing the tenant ID to the handler.

**FastAPI dependency:**

```python
from typing import Annotated

from fastapi import Depends, Header, HTTPException


async def current_tenant(
    actor: Annotated[Actor, Depends(current_actor)],
    x_tenant_id: Annotated[int | None, Header(gt=0)] = None,
) -> int:
    tenant_id = x_tenant_id if x_tenant_id is not None else actor.tenant_id
    if not await memberships.is_active(actor.user_id, tenant_id):
        raise HTTPException(status_code=403, detail="Tenant access denied")
    return tenant_id
```

**FastAPI route using verified tenant context:**

```python
from typing import Annotated

import asyncpg
from fastapi import APIRouter, Depends

router = APIRouter()


@router.get("/tasks")
async def list_tasks(
    tenant_id: Annotated[int, Depends(current_tenant)],
    pool: Annotated[asyncpg.Pool, Depends(get_pool)],
) -> list[dict]:
    return await TaskRepository(pool, tenant_id).list_all()
```

---

### 5. Scoped Repository Pattern

Every DB query is wrapped in a class that automatically applies tenant filtering.

```python
import asyncpg


class TaskRepository:
    def __init__(self, pool: asyncpg.Pool, tenant_id: int):
        self.pool = pool
        self.tenant_id = tenant_id

    async def list_all(self) -> list[dict]:
        async with self.pool.acquire() as conn, conn.transaction():
            await conn.fetchval(
                "SELECT set_config('app.tenant_id', $1, true)", str(self.tenant_id)
            )
            rows = await conn.fetch(
                "SELECT id, title FROM tasks WHERE tenant_id = $1 ORDER BY id",
                self.tenant_id,
            )
        return [dict(row) for row in rows]
```

---

## Database Design

### Users Table

```
users
----------------------------
id          (PK)
tenant_id   (FK, indexed)
name
email
role
created_at
```

### Projects Table

```
projects
----------------------------
id          (PK)
tenant_id   (FK, indexed)
name
settings    (JSONB)
created_at
```

### Tasks Table

```
tasks
----------------------------
id          (PK)
tenant_id   (FK, indexed)
project_id  (FK)
title
status
assigned_to (FK → users.id)
created_at
```

> **Critical:** Index `tenant_id` on every table.
> ```sql
> CREATE INDEX idx_tasks_tenant ON tasks(tenant_id);
> ```

---

## Scaling Challenges

### 1. Database Scaling

**Problem:** Millions of rows across shared tables.

**Solutions:**

| Technique | When to use |
|---|---|
| Partitioning by `tenant_id` | Large tables with many tenants |
| Read replicas | High read traffic |
| Sharding | Very large scale, separate DB per shard |
| Connection pooling (PgBouncer) | Too many simultaneous DB connections |

---

### 2. Caching — Namespace by Tenant

**Wrong (cross-tenant leakage risk):**
```
cache:user:10
```

**Correct (tenant-scoped):**
```
tenant:101:user:10
```

Redis example:
```python
import json

from redis.asyncio import Redis


async def cache_task(redis: Redis, tenant_id: int, task_id: int, task: dict) -> None:
    key = f"tenant:{tenant_id}:tasks:{task_id}"
    await redis.set(key, json.dumps(task), ex=3_600)
```

---

### 3. Search Isolation (Elasticsearch)

Every search query must include `tenant_id` as a filter:

```json
{
  "query": {
    "bool": {
      "must": [
        { "match": { "title": "deploy" } }
      ],
      "filter": [
        { "term": { "tenant_id": 101 } }
      ]
    }
  }
}
```

---

### 4. Tenant-Aware Logging

Every log entry must carry the tenant context:

```json
{
  "timestamp": "2025-01-01T12:00:00Z",
  "tenantId": 101,
  "userId": 88,
  "action": "CREATE_TASK",
  "resourceId": 55,
  "ip": "192.168.1.1"
}
```

---

### 5. Analytics Challenges

| Type | Challenge |
|---|---|
| Per-tenant analytics | Simple but must be tenant-scoped |
| Global/aggregated analytics | Expensive, needs dedicated pipeline |
| Real-time analytics | High cost, consider ClickHouse or BigQuery |

---

## Security Challenges

### 1. Cross-Tenant Access (Most Dangerous)

**Prevention layers (Defense in Depth):**

```
Request
  → Authentication dependency (verify access token)
  → Tenant dependency (verify active membership for selected tenant)
  → Scoped Repository (inject tenantId in all queries)
  → Row-Level Security (DB enforces at storage layer)
```

---

### 2. Rate Limiting Per Tenant

Without per-tenant rate limiting, one tenant can DDoS the entire platform.

```python
import time

from redis.asyncio import Redis


async def within_rate_limit(redis: Redis, tenant_id: int, limit: int = 1_000) -> bool:
    minute = int(time.time()) // 60
    key = f"rate:tenant:{tenant_id}:{minute}"
    async with redis.pipeline(transaction=True) as pipe:
        pipe.incr(key)
        pipe.expire(key, 120)
        count, _ = await pipe.execute()
    return count <= limit


from fastapi import HTTPException


async def enforce_rate_limit(redis: Redis, tenant_id: int) -> None:
    if not await within_rate_limit(redis, tenant_id):
        raise HTTPException(status_code=429, detail="Tenant rate limit exceeded")
```

---

### 3. File Storage Isolation

**Wrong:**
```
/uploads/profile.png
```

**Correct:**
```
/uploads/tenant-101/user-88/profile.png
```

AWS S3 example:
```
s3://my-app-bucket/tenants/101/files/report.pdf
```

---

### 4. Audit Logs

Every sensitive action must be logged with full context:

```json
{
  "tenantId": 101,
  "userId": 88,
  "action": "DELETE_USER",
  "targetUserId": 55,
  "timestamp": "2025-01-01T12:00:00Z",
  "ip": "192.168.1.1",
  "userAgent": "Mozilla/5.0..."
}
```

---

## Deployment Challenges

### Blue-Green Deployment

With thousands of tenants sharing a DB, migrations must be **backward-compatible**:

1. Add new column as nullable (old code still works)
2. Backfill existing data
3. Deploy new code that writes to new column
4. Make column non-nullable
5. Remove old column in later release

---

### Feature Flags Per Tenant

```sql
-- Tenant feature flags table
tenant_feature_flags
----------------------------
tenant_id
feature_name
enabled
```

```python
async def is_feature_enabled(conn, tenant_id: int, feature: str) -> bool:
    value = await conn.fetchval(
        "SELECT enabled FROM tenant_feature_flags WHERE tenant_id = $1 AND feature_name = $2",
        tenant_id, feature,
    )
    return value is True
```

---

## Advanced Topics

### Hybrid Tenancy

Very common in real-world SaaS products:

```
Small/Mid tenants  → Shared DB + Shared Schema
Large enterprise   → Dedicated DB
```

The Tenant Resolver routes requests to the appropriate database based on tenant configuration.

---

### Tenant-Aware Microservices

Tenant context must propagate through every service call:

```
API Gateway
  → User Service      (tenantId: 101)
  → Billing Service   (tenantId: 101)
  → Email Service     (tenantId: 101)
  → Notification      (tenantId: 101)
```

Pass tenant context via:
- HTTP headers: `X-Tenant-ID: 101`
- gRPC metadata
- Message queue payloads

---

### Event-Driven Architecture (Kafka/RabbitMQ)

Every event must carry tenant context:

```json
{
  "eventType": "TASK_CREATED",
  "tenantId": 101,
  "userId": 88,
  "payload": {
    "taskId": 55,
    "title": "Deploy to prod"
  },
  "timestamp": "2025-01-01T12:00:00Z"
}
```

**Kafka topic strategy:**

| Strategy | Pros | Cons |
|---|---|---|
| One topic per tenant | Full isolation | Thousands of topics |
| Shared topic + filter | Simple | Consumer must filter by tenantId |
| Partition by tenantId | Balanced | Rebalancing on new tenants |

---

### Background Job Isolation

```python
from app.celery_app import celery_app


@celery_app.task
def send_weekly_report(tenant_id: int, user_id: int, week: str) -> None:
    if memberships.is_active_sync(user_id, tenant_id):
        # Queries, storage paths, and the job's deduplication key include tenant_id.
        reports.generate_for_user(tenant_id=tenant_id, user_id=user_id, week=week)


def enqueue_weekly_report(tenant_id: int, user_id: int, week: str) -> None:
    send_weekly_report.delay(tenant_id, user_id, week)
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Frontend | React, Angular |
| Backend | Python, FastAPI, asyncpg |
| Database | PostgreSQL (with RLS) |
| Cache | Redis |
| Search | Elasticsearch |
| Message Queue | Apache Kafka, RabbitMQ |
| File Storage | AWS S3 |
| Cloud | AWS, Google Cloud |
| Monitoring | Datadog, Grafana (per-tenant dashboards) |

---

## Interview Guide

### Step-by-Step Answer Framework

When an interviewer says **"Design a multi-tenant SaaS"**, answer in this order:

---

#### Step 1 — Clarify Requirements

Ask these questions before designing:

- How many tenants? (100 vs 100,000 changes everything)
- What level of data isolation is required?
- Are there compliance requirements? (HIPAA, GDPR, SOC 2)
- Are there enterprise customers needing dedicated infrastructure?
- Do tenants need custom workflows, fields, or permissions?
- What is the expected traffic per tenant?

---

#### Step 2 — Choose Tenancy Model

Present the tradeoffs clearly:

> "For a startup with ~100 tenants and no strict compliance needs, I'd start with shared DB + shared schema — it's the cheapest and fastest to ship. As we grow past 1,000 tenants or if enterprise clients require data isolation, I'd migrate to separate schemas. For highly regulated enterprise clients, we'd offer a dedicated DB tier."

---

#### Step 3 — Authentication Design

Explain:
- How tenant is identified (subdomain, JWT, API key)
- JWT structure (include `tenantId` and `role`)
- RBAC scoped to tenant

---

#### Step 4 — Data Isolation

Cover all layers:
1. `tenant_id` column on every table
2. A FastAPI dependency that validates tenant membership
3. Scoped repositories that inject `tenant_id` automatically
4. PostgreSQL RLS as the last line of defense

---

#### Step 5 — Scaling

Discuss:
- Caching with tenant-namespaced keys
- DB partitioning on `tenant_id`
- Read replicas for heavy tenants
- Queue isolation for background jobs

---

#### Step 6 — Security

Cover:
- Per-tenant rate limiting
- Audit logs for every sensitive action
- File storage isolation (e.g., S3 paths by tenant)
- Encryption at rest and in transit
- Cross-tenant access prevention (Defense in Depth)

---

### Sample Interview Q&A

**Q: How would you prevent one tenant from seeing another tenant's data?**

> Use multiple layers: the FastAPI dependency verifies the signed-in user’s membership in the selected tenant on every request; all repository queries automatically include `WHERE tenant_id = ?`; PostgreSQL Row-Level Security enforces it at the DB level. This defense-in-depth means a single missed check doesn't cause a breach.

---

**Q: What is the noisy neighbor problem and how do you solve it?**

> When one tenant's heavy workload degrades performance for all others. Solutions: per-tenant rate limiting, database query timeouts per tenant, query prioritization, and for very high-volume tenants, migrating them to a dedicated DB tier.

---

**Q: How do you handle database migrations in a multi-tenant system?**

> Migrations must be backward-compatible. Use an expand-contract pattern: add new columns as nullable first (old code still works), backfill data, deploy new code, then make columns required. For separate-schema tenancy, run migrations in batches with rollback capability.

---

**Q: How do you handle tenant-specific customizations?**

> Store tenant configuration in a `tenant_settings` or `tenant_feature_flags` table. Use a feature flag service to enable custom fields, workflows, or UI elements per tenant. For complex customizations (different schemas), consider separate-schema or separate-DB tenancy.

---

**Q: How would you scale this system to 1 million tenants?**

> At that scale: shared schema with heavy DB partitioning on `tenant_id`; Redis for tenant-namespaced caching; Elasticsearch with tenant filtering for search; Kafka with `tenantId` in every event; CDN for static assets; auto-scaling compute; and a hybrid model where high-value enterprise tenants get dedicated infrastructure.

---

## Common Interview Mistakes

| Mistake | Why It's Wrong | Correct Approach |
|---|---|---|
| Ignoring tenant isolation | Core requirement of multi-tenancy | Always discuss all isolation layers |
| No migration strategy | DB changes break thousands of tenants | Always use expand-contract pattern |
| Forgetting noisy neighbor | One tenant kills the platform | Per-tenant rate limiting and quotas |
| No RBAC discussion | Authorization is tenant-scoped | Explain tenant-aware RBAC |
| Treating tenantId as optional | Queries can run outside the intended tenant | Verify tenant access in the route dependency, repository, and database |
| One-size-fits-all architecture | Startups ≠ enterprises | Always explain hybrid/tiered models |
| Ignoring compliance | Deal-breaker for enterprise sales | Mention GDPR, HIPAA, SOC 2 where relevant |

---

## Best Production Practices

- Use **PostgreSQL RLS** as a database-level safety net
- **Namespace all cache keys** by tenant: `tenant:{id}:resource:{id}`
- Set up **per-tenant monitoring and dashboards**
- Use **feature flags** for beta and premium features per tenant
- Maintain **audit logs** for all sensitive actions
- Isolate **background jobs** by tenant with separate queues
- Apply **per-tenant rate limiting** to prevent abuse
- Create **strong indexes** on `tenant_id` in every table
- Isolate **file storage** with tenant-prefixed paths
- Use **connection pooling** (PgBouncer) to handle connection limits
- Propagate **tenant context** in all microservice calls and events

---

## Quick-Reference Cheat Sheet

```
MULTI-TENANT DESIGN CHECKLIST
==============================

TENANCY MODEL
 [ ] Startup     → Shared DB + Shared Schema
 [ ] Mid-scale   → Shared DB + Separate Schema
 [ ] Enterprise  → Separate DB
 [ ] At scale    → Hybrid (shared + dedicated)

DATA ISOLATION
 [ ] tenant_id on every table
 [ ] Index on tenant_id
 [ ] Verified tenant context on every tenant-scoped request
 [ ] Scoped repositories
 [ ] PostgreSQL Row-Level Security

AUTHENTICATION
 [ ] JWT includes tenantId + role
 [ ] Tenant-scoped RBAC
 [ ] Tenant resolution strategy (subdomain/header/JWT)

CACHING
 [ ] Keys namespaced: tenant:{id}:resource:{id}
 [ ] Cache invalidation scoped to tenant

SECURITY
 [ ] Per-tenant rate limiting
 [ ] File storage isolated by tenant
 [ ] Audit logs with tenantId on every action
 [ ] Cross-tenant access tested

SCALING
 [ ] DB partitioning on tenant_id
 [ ] Read replicas
 [ ] Search filtered by tenant_id
 [ ] Event payloads include tenantId
 [ ] Background jobs scoped to tenant

DEPLOYMENT
 [ ] Backward-compatible migrations
 [ ] Feature flags per tenant
 [ ] Per-tenant monitoring/alerting
```

---

*Last updated: 2025 | For system design interviews and production SaaS architecture*
