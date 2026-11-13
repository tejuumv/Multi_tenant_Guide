# Senior Full Stack Developer Interview Prep
### Role: Knowsy Platform | Focus: Backend, Architecture, Real-Time, AI Workflows

---

## Table of Contents

1. [Multi-Tenant SaaS Architecture](#1-multi-tenant-saas-architecture)
2. [WebSockets & Real-Time Systems](#2-websockets--real-time-systems)
3. [Supabase Architecture](#3-supabase-architecture)
4. [Role-Based Access Control (RBAC)](#4-role-based-access-control-rbac)
5. [PostgreSQL Optimization & Indexing](#5-postgresql-optimization--indexing)
6. [Event-Driven Architecture](#6-event-driven-architecture)
7. [API Design & Authentication](#7-api-design--authentication)
8. [Scalable Backend Design & NestJS](#8-scalable-backend-design--nestjs)
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

**Layer 1 – Middleware injects tenant context:**
```typescript
// NestJS middleware
@Injectable()
export class TenantMiddleware implements NestMiddleware {
  use(req: Request, res: Response, next: NextFunction) {
    const tenantId = req.headers['x-tenant-id'] || this.extractFromJWT(req);
    if (!tenantId) throw new UnauthorizedException('No tenant context');
    req['tenantId'] = tenantId;
    next();
  }
}
```

**Layer 2 – Repository/Service layer always scopes queries:**
```typescript
@Injectable()
export class TaskService {
  constructor(private readonly db: DatabaseService) {}

  findAll(tenantId: string) {
    // tenantId is ALWAYS passed; never derived from body/params alone
    return this.db.query(
      'SELECT * FROM tasks WHERE tenant_id = $1',
      [tenantId]
    );
  }
}
```

**Layer 3 – PostgreSQL Row Level Security (RLS):**
```sql
ALTER TABLE tasks ENABLE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON tasks
  USING (tenant_id = current_setting('app.current_tenant')::UUID);

-- Set at the start of each DB session
SELECT set_config('app.current_tenant', $1, true);
```

**Layer 4 – Integration tests per tenant:**
```typescript
it('should not return Task of tenant B when querying as tenant A', async () => {
  const result = await taskService.findAll(tenantA.id);
  expect(result.every(t => t.tenantId === tenantA.id)).toBe(true);
});
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
- All tenants share schema → standard migration tools (Flyway, Prisma Migrate, TypeORM).
- Add columns carefully – always use nullable or default values.
- Use feature flags to roll out new columns per tenant.

**Migration Strategy (Separate Schema):**
```typescript
async function runMigrationsForAllTenants() {
  const tenants = await db.query('SELECT schema_name FROM tenants');
  for (const tenant of tenants) {
    await db.query(`SET search_path = ${tenant.schema_name}`);
    await runMigrations(); // run Flyway/Prisma against this schema
  }
}
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

```typescript
// Redis rate limiter per tenant
async function checkRateLimit(tenantId: string): Promise<boolean> {
  const key = `rate:${tenantId}`;
  const current = await redis.incr(key);
  if (current === 1) await redis.expire(key, 60); // 1 minute window
  return current <= TENANT_RATE_LIMIT; // e.g., 1000 req/min
}
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

### Q: How would you implement WebSockets in a NestJS application?

**A:**

```typescript
// Install: npm install @nestjs/websockets @nestjs/platform-socket.io socket.io

@WebSocketGateway({
  cors: { origin: '*' },
  namespace: '/game',
})
export class GameGateway implements OnGatewayInit, OnGatewayConnection, OnGatewayDisconnect {
  @WebSocketServer() server: Server;

  private logger = new Logger('GameGateway');

  afterInit(server: Server) {
    this.logger.log('WebSocket Gateway initialized');
  }

  handleConnection(client: Socket) {
    const tenantId = client.handshake.auth.tenantId;
    client.join(`tenant:${tenantId}`); // join tenant-specific room
    this.logger.log(`Client connected: ${client.id}`);
  }

  handleDisconnect(client: Socket) {
    this.logger.log(`Client disconnected: ${client.id}`);
  }

  @SubscribeMessage('submitAnswer')
  handleSubmitAnswer(
    @MessageBody() data: { sessionId: string; answer: string },
    @ConnectedSocket() client: Socket,
  ) {
    const tenantId = client.handshake.auth.tenantId;
    // process answer, update leaderboard
    this.server.to(`session:${data.sessionId}`).emit('scoreUpdate', {
      playerId: client.id,
      newScore: 100,
    });
  }

  // Broadcast to all clients in a session
  broadcastToSession(sessionId: string, event: string, payload: any) {
    this.server.to(`session:${sessionId}`).emit(event, payload);
  }
}
```

---

### Q: How do you scale WebSockets horizontally across multiple servers?

**A:**

The problem: Client A connects to Server 1. Client B connects to Server 2. If Server 1 emits to "room", Client B won't receive it.

**Solution: Redis Pub/Sub Adapter**

```typescript
// main.ts
import { IoAdapter } from '@nestjs/platform-socket.io';
import { createAdapter } from '@socket.io/redis-adapter';
import { createClient } from 'redis';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  const pubClient = createClient({ url: 'redis://localhost:6379' });
  const subClient = pubClient.duplicate();
  await Promise.all([pubClient.connect(), subClient.connect()]);

  const io = app.get(Server);
  io.adapter(createAdapter(pubClient, subClient));

  await app.listen(3000);
}
```

**How it works:**
1. Server 1 emits to Redis channel.
2. Redis broadcasts to all subscribed servers.
3. All servers relay to their connected clients.

**Additional scaling strategies:**
- **Sticky sessions** at load balancer so each client consistently hits same server.
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
```typescript
interface GameState {
  sessionId: string;
  round: number;
  players: Record<string, PlayerState>;
  currentQuestion: Question;
  timeRemaining: number;
}

// Server holds canonical state in Redis
class GameStateManager {
  async getState(sessionId: string): Promise<GameState> {
    const raw = await redis.get(`game:${sessionId}`);
    return JSON.parse(raw);
  }

  async updateState(sessionId: string, update: Partial<GameState>) {
    const current = await this.getState(sessionId);
    const next = { ...current, ...update };
    await redis.set(`game:${sessionId}`, JSON.stringify(next));
    return next;
  }

  async submitAnswer(sessionId: string, playerId: string, answer: string) {
    // Atomic update using Redis transaction
    const multi = redis.multi();
    // validate, score, update
    await multi.exec();
  }
}
```

**Conflict resolution:** Last-write-wins for low-stakes; server-authoritative for scores.

---

### Q: How do you implement a countdown timer in a real-time multiplayer session?

**A:**

**Anti-pattern:** Each client runs its own timer → drift, cheating.

**Correct approach: Server-driven timer**

```typescript
class SessionTimerService {
  async startRound(sessionId: string, durationSeconds: number) {
    const endsAt = Date.now() + durationSeconds * 1000;

    // Store end time, not countdown
    await redis.set(`timer:${sessionId}`, endsAt);

    // Broadcast start with absolute end time
    this.gateway.broadcastToSession(sessionId, 'roundStarted', {
      endsAt, // ISO timestamp
      durationSeconds,
    });

    // Server-side timeout to end round
    setTimeout(() => this.endRound(sessionId), durationSeconds * 1000);
  }

  // Client computes remaining time: endsAt - Date.now()
}
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

**Client subscription:**
```typescript
import { createClient } from '@supabase/supabase-js';

const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);

// Listen to all inserts on a table
const channel = supabase
  .channel('game-scores')
  .on(
    'postgres_changes',
    { event: 'INSERT', schema: 'public', table: 'scores' },
    (payload) => {
      console.log('New score:', payload.new);
      updateLeaderboard(payload.new);
    }
  )
  .subscribe();

// Broadcast custom events (client-to-client, no DB needed)
channel.send({
  type: 'broadcast',
  event: 'cursor-move',
  payload: { x: 100, y: 200 },
});
```

**Multi-tenant Realtime:**
```typescript
// Filter by tenant using RLS – Supabase respects RLS on realtime too
supabase.channel(`tenant:${tenantId}`)
  .on('postgres_changes', {
    event: '*',
    schema: 'public',
    table: 'tasks',
    filter: `tenant_id=eq.${tenantId}` // server-side filter
  }, handler)
  .subscribe();
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

### Q: How do you use Supabase in a NestJS backend vs using it directly from the client?

**A:**

**Direct client use (frontend):**
- Anon key exposed in browser.
- RLS enforces access control.
- Good for reads and user-specific writes.

**Backend use (NestJS with service key):**
- Service key bypasses RLS → must manually enforce tenant isolation.
- Required for: admin operations, bulk processing, cross-tenant aggregation.

```typescript
// supabase.service.ts
@Injectable()
export class SupabaseService {
  private client: SupabaseClient;

  constructor() {
    this.client = createClient(
      process.env.SUPABASE_URL,
      process.env.SUPABASE_SERVICE_KEY // Never expose this to client
    );
  }

  // Always scope by tenant in service layer
  async getTasksForTenant(tenantId: string) {
    return this.client
      .from('tasks')
      .select('*')
      .eq('tenant_id', tenantId); // Explicit filtering since RLS bypassed
  }
}
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

**NestJS Guard implementation:**
```typescript
// permission.guard.ts
@Injectable()
export class PermissionGuard implements CanActivate {
  constructor(
    private reflector: Reflector,
    private authService: AuthService,
  ) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const required = this.reflector.get<string[]>('permissions', context.getHandler());
    if (!required) return true;

    const request = context.switchToHttp().getRequest();
    const { userId, tenantId } = request.user;

    const userPermissions = await this.authService.getPermissions(userId, tenantId);
    return required.every(p => userPermissions.includes(p));
  }
}

// Decorator
export const RequirePermission = (...permissions: string[]) =>
  SetMetadata('permissions', permissions);

// Usage on controller
@Get('sessions')
@RequirePermission('session:read')
getSessions() { ... }

@Post('sessions')
@RequirePermission('session:create')
createSession() { ... }
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
NestJS (100 instances) → PgBouncer (20 real PG connections) → PostgreSQL
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

### Q: How would you implement event-driven communication in NestJS?

**A:**

**Option 1: NestJS built-in EventEmitter (in-process, no broker)**
```typescript
import { EventEmitter2 } from '@nestjs/event-emitter';

// Emit event
@Injectable()
export class GameService {
  constructor(private events: EventEmitter2) {}

  async endRound(sessionId: string) {
    const results = await this.calculateScores(sessionId);
    this.events.emit('round.completed', { sessionId, results });
  }
}

// Listen to event
@Injectable()
export class AnalyticsService {
  @OnEvent('round.completed')
  handleRoundCompleted(payload: { sessionId: string; results: any }) {
    // persist analytics data
  }
}

@Injectable()
export class NotificationService {
  @OnEvent('round.completed')
  handleRoundCompleted(payload: { sessionId: string; results: any }) {
    // send push notifications
  }
}
```

**Option 2: Redis Streams / BullMQ (cross-process, durable)**
```typescript
import { InjectQueue } from '@nestjs/bull';
import { Queue } from 'bull';

@Injectable()
export class GameService {
  constructor(@InjectQueue('analytics') private analyticsQueue: Queue) {}

  async endRound(sessionId: string) {
    await this.analyticsQueue.add('round-completed', { sessionId }, {
      attempts: 3,
      backoff: { type: 'exponential', delay: 1000 },
    });
  }
}

@Processor('analytics')
export class AnalyticsProcessor {
  @Process('round-completed')
  async handle(job: Job<{ sessionId: string }>) {
    await this.processRoundAnalytics(job.data.sessionId);
  }
}
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

```typescript
// Business operation + event in one transaction
async function endRound(sessionId: string) {
  await db.transaction(async (trx) => {
    await trx.query('UPDATE game_sessions SET status = $1 WHERE id = $2', ['completed', sessionId]);
    await trx.query(
      'INSERT INTO outbox_events (event_type, payload) VALUES ($1, $2)',
      ['round.completed', JSON.stringify({ sessionId })]
    );
  });
}

// Separate poller publishes outbox events
async function pollOutbox() {
  const events = await db.query('SELECT * FROM outbox_events WHERE published = FALSE LIMIT 100');
  for (const event of events.rows) {
    await messageBroker.publish(event.event_type, event.payload);
    await db.query('UPDATE outbox_events SET published = TRUE WHERE id = $1', [event.id]);
  }
}
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

**NestJS JWT guard:**
```typescript
@Injectable()
export class JwtAuthGuard extends AuthGuard('jwt') {
  handleRequest(err: any, user: any, info: any) {
    if (err || !user) throw err || new UnauthorizedException('Invalid token');
    return user;
  }
}

// JWT strategy extracts tenantId
@Injectable()
export class JwtStrategy extends PassportStrategy(Strategy) {
  validate(payload: JwtPayload) {
    return { userId: payload.sub, tenantId: payload.tenantId, roles: payload.roles };
  }
}
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

**NestJS URL versioning:**
```typescript
// main.ts
app.enableVersioning({ type: VersioningType.URI });

// Controller
@Controller({ path: 'sessions', version: '1' })
export class SessionControllerV1 { ... }

@Controller({ path: 'sessions', version: '2' })
export class SessionControllerV2 { ... }
```

**Strategy:** Never break v1. Deprecate with Sunset header. Maintain 2 versions max.

---

## 8. Scalable Backend Design & NestJS

### Q: How do you structure a NestJS application for a large-scale SaaS product?

**A:**

```
src/
  modules/
    auth/
      auth.module.ts
      auth.service.ts
      auth.controller.ts
      strategies/jwt.strategy.ts
      guards/jwt.guard.ts
    tenant/
      tenant.module.ts
      tenant.service.ts
      tenant.middleware.ts
    game/
      game.module.ts
      game.service.ts
      game.gateway.ts      ← WebSocket
      game.controller.ts
      dto/
      entities/
    analytics/
      analytics.module.ts
      analytics.processor.ts  ← BullMQ consumer
  common/
    filters/http-exception.filter.ts
    interceptors/logging.interceptor.ts
    decorators/current-tenant.decorator.ts
    guards/permission.guard.ts
  config/
    database.config.ts
    redis.config.ts
  app.module.ts
  main.ts
```

**Key patterns:**
- **CQRS** (Command Query Responsibility Segregation) for complex game logic.
- **Repository pattern** to decouple data access.
- **DTO validation** with `class-validator` on every endpoint.
- **Interceptors** for response transformation and logging.

---

### Q: How do you handle background jobs and scheduled tasks in NestJS?

**A:**

```typescript
// BullMQ for background jobs
@Module({
  imports: [
    BullModule.registerQueue({ name: 'email' }),
    BullModule.registerQueue({ name: 'analytics' }),
  ],
})

// Schedule with @nestjs/schedule
@Injectable()
export class CleanupService {
  @Cron('0 2 * * *') // Every day at 2am
  async cleanExpiredSessions() {
    await this.db.query(
      "DELETE FROM game_sessions WHERE ended_at < NOW() - INTERVAL '90 days'"
    );
  }

  @Interval(60000) // Every 60 seconds
  async syncLeaderboardCache() {
    // Refresh Redis leaderboard cache from DB
  }
}
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
     │  (NestJS)      │ │  Gateway     │ │  (GoTrue/Custom)│
     └─────────┬──────┘ └───────┬──────┘ └────────┬────────┘
               │                │                  │
     ┌─────────▼────────────────▼──────────────────▼────────┐
     │                    Redis                              │
     │  - Session state, leaderboards, rate limiting        │
     │  - Pub/Sub for WS horizontal scaling                 │
     │  - BullMQ job queues                                 │
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
- Redis adapter for WS cluster.
- Read replicas for analytics queries.
- CDN for static assets.

---

### Q: How would you handle 10,000 concurrent users on this platform?

**A:**

**Bottlenecks to address:**

1. **WebSocket connections:**
   - Node.js can handle ~50k WS connections per instance.
   - Scale horizontally; use Redis adapter for cross-instance messaging.
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
```typescript
// AI might generate this – DANGEROUS
const user = await db.query(`SELECT * FROM users WHERE id = '${userId}'`); // SQL injection

// Correct
const user = await db.query('SELECT * FROM users WHERE id = $1', [userId]);
```

Common AI security mistakes:
- SQL injection via string concatenation.
- Missing input validation.
- Exposed secrets in code (hardcoded API keys).
- Missing auth checks on routes.
- JWT verified but `tenantId` not checked.

**3. Performance issues**
```typescript
// AI might generate – N+1 query
for (const session of sessions) {
  session.scores = await db.query('SELECT * FROM scores WHERE session_id = $1', [session.id]);
}

// Correct – single query with join
const sessions = await db.query(`
  SELECT s.*, json_agg(sc.*) as scores
  FROM sessions s
  LEFT JOIN scores sc ON sc.session_id = s.id
  GROUP BY s.id
`);
```

**4. Architecture violations**
- Business logic leaking into controllers.
- Direct DB access from route handlers (should go through service layer).
- Missing error handling / unhandled promise rejections.

**5. Type safety**
```typescript
// AI might use 'any'
function process(data: any) { ... }

// Should be typed
function process(data: PlayerScore): void { ... }
```

**Your review checklist:**
```
□ Does it compile without TypeScript errors?
□ Are all inputs validated (class-validator, zod)?
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

```typescript
// Using opossum library
import CircuitBreaker from 'opossum';

const breaker = new CircuitBreaker(callExternalService, {
  timeout: 3000,         // Fail if takes > 3s
  errorThresholdPercentage: 50, // Open if 50% of calls fail
  resetTimeout: 30000,   // Try again after 30s
});

breaker.fallback(() => getCachedData()); // Return cached data when open

const result = await breaker.fire(payload);
```

---

## 12. Analytics Architecture

### Q: How would you design an analytics system for a multiplayer game platform?

**A:**

**Requirements:** Track player behavior, session performance, engagement metrics. Support historical reports and real-time dashboards.

**Architecture:**

```
Game Events → BullMQ Queue → Analytics Worker → Write to analytics DB
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
| **GKE (Kubernetes)** | Deploy stateful WebSocket servers with sticky sessions |
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
```typescript
// DANGEROUS – attacker can access any user's data
GET /api/users/456/sessions

// Always verify ownership
const session = await db.query(
  'SELECT * FROM sessions WHERE id = $1 AND tenant_id = $2',
  [sessionId, req.user.tenantId]
);
if (!session) throw new ForbiddenException();
```

**5. Rate limiting abuse**
- Without rate limits, one tenant hammers the API.
- Fix: Per-tenant rate limits, IP-based limits.

**6. Secrets in code**
- AI tools sometimes embed API keys in generated code.
- Fix: Environment variables, GCP Secret Manager, pre-commit hooks to detect secrets.

**7. WebSocket authentication**
```typescript
// Always authenticate on WS connection
handleConnection(client: Socket) {
  const token = client.handshake.auth.token;
  try {
    const payload = this.jwt.verify(token);
    client.data.user = payload;
    client.join(`tenant:${payload.tenantId}`);
  } catch {
    client.disconnect();
  }
}
```

---

## Quick Revision Cheat Sheet

| Topic | Key Points |
|---|---|
| Multi-tenancy | Row-level (tenant_id), Schema-level, DB-level. Use RLS in Postgres. |
| WebSockets | NestJS Gateway + Redis adapter for horizontal scaling. Server-authoritative state. |
| Supabase Realtime | Logical replication → WAL → WebSocket. RLS respected in realtime. |
| RBAC | Roles → Permissions → Users. Guards + decorators in NestJS. |
| PostgreSQL Indexing | B-Tree (default), GIN (JSONB/FTS), Partial, Covering. EXPLAIN ANALYZE. |
| Event-Driven | EventEmitter2 (in-process), BullMQ (cross-process), Outbox pattern for reliability. |
| Leaderboard | Redis sorted sets (ZADD, ZREVRANGE). Persist to DB on session end. |
| AI Code Review | Security, N+1 queries, missing validation, type safety, architecture violations. |
| CAP Theorem | Partition always happens. Choose C (accuracy) vs A (availability) per feature. |
| Circuit Breaker | Closed → Open → Half-Open. Prevent cascading failures. |
| Scaling WS | Redis Pub/Sub adapter. Sticky sessions at load balancer. |
| Connection Pooling | PgBouncer. Transaction mode = most efficient. Max ~20 real connections. |

---

*Good luck with the interview. You've got this.*
