# Flux Shared DB

Flux Shared DB is a solution for persistent, shared database storage on the [Flux network](https://www.runonflux.io). It handles replication between MySQL instances.

Operator nodes discover each other using the FluxOS API and immediately form a cluster. Each Operator node connects to a DB engine through a connection pool. Read queries are proxied directly to the DB engine, while write queries are sent to the master node. The master node timestamps and sequences incoming write queries, then immediately forwards them to slave nodes.

![FLUX DB Cluster](https://user-images.githubusercontent.com/1296210/184499730-722801f7-e827-4857-902e-fe9a61f36e5f.jpg)

## Operator Interfaces

The Operator exposes three interfaces:

1. DB Interface (proxy interface for the DB engine)
2. Internal API (used for internal communication)
3. UI API (used for cluster management)

## Running on the Flux Network

To use Flux Shared DB in your project, link it to a DB engine and the Operator handles the rest. A common setup is to run it alongside a DB engine. You can also add your application to the same compose stack and connect directly to the Operator DB port.

- Log in to [home.runonflux.io](https://home.runonflux.io) and navigate to Applications > Register New App.
- Add a component for MySQL.
- Name it `mysql`.
- Use the official Docker image: `mysql:latest`.
- Add this command to the `Commands` field: `--disable-log-bin`.
- Use the following sample to set the environment variables for the mysql component:
   ```json
   [
      "MYSQL_ROOT_PASSWORD=PASSWORD",
      "MYSQL_ROOT_HOST=172.0.0.0/255.0.0.0"
   ]
   ```
- Add another component for Operator.
- Name it `operator`.
- Use shared-db latest Docker image: `runonflux/shared-db:latest`.
- Set the Container Data for the component to `s:/app/dumps`.
- Add these ports to the `Cont. Ports` field: `[3307,7071,8008]`.
- Using the `Ports` field, map those ports to new ones, for example: `[13307,17071,18008]`.
- For the `Domains` field, add this: `["","",""]`.
- Use the following sample to set the environment variables for the Operator component:
   ```json
   [
      "DB_COMPONENT_NAME=mysql",
      "INIT_DB_NAME=my-db",
      "DB_INIT_PASS=PASSWORD",
      "OPERATOR_API_SECRET=REPLACE_WITH_RANDOM_64_HEX_SECRET",
      "DB_USER=root",
      "DB_PORT=13307",
      "API_PORT=17071"
   ]
   ```
- Add your Application component (optional).
- In your app you can use this connection string to connect to the DB:
  ```bash
   "server=operator:3307;uid=DB_USER;pwd=DB_INIT_PASS;database=INIT_DB_NAME"
   ```

## Image Variants

Three image variants are published to Docker Hub:

| Image | Contents | Tags | DB component needed? |
|-------|----------|------|----------------------|
| `runonflux/shared-db:latest` / `:dev` | Distroless operator only (DB-less) | `latest` (master), `dev` (development) | Yes - a separate DB component |
| `runonflux/shared-db:latest-mysql` / `:dev-mysql` | Operator + MySQL 8 bundled | `latest-mysql`, `dev-mysql` | No - DB runs inside the container |
| `runonflux/shared-db:latest-mariadb` / `:dev-mariadb` | Operator + MariaDB bundled | `latest-mariadb`, `dev-mariadb` | No - DB runs inside the container |

`dev*` tags are built from the `development` branch, `latest*` from `master`.

Pick the DB-less image if you want to run and manage the DB engine as a separate component (setup above). Pick a bundled image for a single self-contained component (setup below). Do not use both approaches for the same deployment.

## Running a Bundled Image (no separate DB component)

The bundled images run the DB engine (MySQL 8 or MariaDB) inside the same container as the Operator, so you only register a single component.

- Log in to [home.runonflux.io](https://home.runonflux.io) and navigate to Applications > Register New App.
- Add a single component for the Operator.
- Name it `operator`.
- Use a bundled image, for example: `runonflux/shared-db:latest-mysql` (or `runonflux/shared-db:latest-mariadb`).
- Do NOT add a separate MySQL component, and do NOT set `DB_COMPONENT_NAME` (it defaults to `localhost`, connecting to the in-container DB).
- Set the Container Data for the component to `s:/app/dumps|/app/db`.
  - `s:/app/dumps` holds SQL backups and is synced across instances (the `s:` flag).
  - `/app/db` is the DB engine data directory. It must be persistent but node-local (no `s:` flag) - each node keeps its own copy and replication is handled by the Operator.
- Add these ports to the `Cont. Ports` field: `[3307,7071,8008]`.
- Using the `Ports` field, map those ports to new ones, for example: `[13307,17071,18008]`.
- For the `Domains` field, add this: `["","",""]`.
- Use the following sample to set the environment variables:
   ```json
   [
      "INIT_DB_NAME=my-db",
      "DB_INIT_PASS=PASSWORD",
      "OPERATOR_API_SECRET=REPLACE_WITH_RANDOM_64_HEX_SECRET",
      "DB_USER=root",
      "DB_PORT=13307",
      "API_PORT=17071"
   ]
   ```
- Add your Application component (optional) and connect to the Operator DB port as with the DB-less setup.

Note: `DB_INIT_PASS` sets the DB root password on first boot only. Changing it later will not re-key an already-initialized `/app/db` datadir.

### MySQL interface authentication

Missing or empty `DB_USER` and `DB_INIT_PASS` retain the historical defaults `root` and `secret`; existing deployments using those credentials continue working without new environment variables. Blank client passwords are still rejected.

The MySQL emulator requires the configured `DB_USER` and the effective DB password (initialized from `DB_INIT_PASS`), in addition to its existing IP and cluster-readiness checks. Whitelisted and application IPs must authenticate too. It verifies the `mysql_native_password` challenge response against the fresh per-connection salt before allocating a pooled DB connection. Clients that initially select a different plugin (including MySQL 8 clients starting with `caching_sha2_password`) receive a native-password auth-switch request with a fresh challenge. The switched response is verified before username authorization or pool allocation. Wrong or omitted passwords, incorrect usernames, malformed handshakes, and unsupported authentication protocols receive error 1045 / SQLSTATE 28000 and the connection closes. A client that closes during authorization or pool allocation cannot retain a pooled connection.

Automatic local-network access is limited to valid RFC1918 IPv4 addresses: `10.0.0.0/8`, `172.16.0.0/12`, and `192.168.0.0/16`. Public addresses (including other `172.x` ranges) require an explicit `WHITELIST` entry or a discovered application IP match. Local access still requires valid credentials and follows `AUTH_MASTER_ONLY`; loopback access uses the default `127.0.0.1` whitelist entry.

The master requires an admitted replication peer before accepting DB clients or treating its incoming peer connection as healthy. Raw Engine.IO sessions, rejected proofs, and short-lived discovery sockets do not satisfy this check. New persistent peers identify their replica role; older peers identify themselves by requesting backlog, and unauthenticated legacy peers count only during the grace period.

This interface exposes a single configured account backed by the existing privileged connection pool; it does not implement separate MySQL user accounts or per-user grants. The operator API secret is separate and is not a MySQL login password. The emulator continues to use its existing native-password wire protocol without MySQL TLS support.

Regression coverage includes malformed and replayed handshakes, commands pipelined after failed authentication, and username rejection on trusted IPs. The [three-node integration test](test/integration/README.md) also checks correct, incorrect, and missing credentials with real mysql2 and MariaDB clients, then verifies authenticated replication.

### Operator API authentication

Updated operator clients always send a session-bound HMAC proof. This release accepts legacy connections without proofs from allowed operator IPs until **2026-10-25 at 17:48:17 UTC**, then requires authentication for discovery, election, replication, key exchange, and session synchronization. This is a fixed release cutoff, not 48 hours after each restart. Invalid proofs and unauthorized IPs are rejected even during the grace period. The previous shared-IP exposure remains during that period.

At the cutoff, already-connected unauthenticated peers are disconnected, including passive broadcast subscribers; incoming legacy commands are also blocked at dispatch. Authenticated connections remain open. Nodes starting after the cutoff enforce authentication immediately. Once enforcement is observed, a running process never reopens legacy access because of a backwards clock adjustment. Startup logs the cutoff, and `getStatus` reports `operatorAuth.version`, `operatorAuth.required`, `operatorAuth.enforceAt`, and `operatorAuth.secondaryKeyConfigured`.

To override the release cutoff, set the same `OPERATOR_AUTH_ENFORCE_AT` on every replica using an ISO UTC timestamp such as `2026-10-25T17:48:17Z`. A past timestamp enforces immediately; invalid or empty values prevent startup. Keep node clocks synchronized. Old binaries cannot enforce the cutoff themselves, so complete their replacement before it. TLS remains a separate setting; keep the transport consistent during the mixed-version rollout.

Set the same randomly generated `OPERATOR_API_SECRET` on every replica (at least 32 bytes; for example, generate one with `openssl rand -hex 32`). If it is unset, the operator derives its authentication key from the effective DB password. For backward compatibility, the historical default `secret` and existing short DB passwords remain accepted, and startup logs a warning for passwords shorter than 32 bytes. The public default produces a predictable API key and does not protect against another app on an allowed node; configure a private password or dedicated API secret for that protection. An explicitly empty `OPERATOR_API_SECRET` is rejected rather than falling back to the DB password.

The key is derived with HKDF-SHA256 using `DB_APPNAME` and a versioned operator authentication purpose. `DB_APPNAME` is resolved from Flux hostinfo when unset; set it explicitly for local deployments. All replicas must use the same app name. Neither the password nor the derived key is transmitted: the proof authenticates the fresh Engine.IO session ID, so a captured proof cannot be reused on a later connection.

For encryption and protection against active network interception, configure `OPERATOR_TLS_CERT` and `OPERATOR_TLS_KEY` with PEM file paths on every replica. Clients then use HTTPS and verify server certificates. For a private CA, also set `OPERATOR_TLS_CA` to its PEM file. Certificates must cover the peer IPs, or set `OPERATOR_TLS_SERVERNAME` to a shared DNS identity covered by the certificates. The existing `SSL` option controls the UI separately. Key derivation, certificate/CA reads, and client HTTPS agents are cached lazily after the cluster identity is resolved. Restart instances to load changed TLS files. Without operator TLS, traffic remains readable and susceptible to active relaying/tampering despite replay-resistant connection authentication.

Complete the rolling upgrade before the cutoff: old clients can communicate with updated servers during the grace period, and updated clients can communicate with old servers. After the cutoff, updated servers reject old clients. This grace period does not relax the separate MySQL username/password or private-network checks. Rotating a secret directly during a rolling update is not safe: the grace period accepts missing legacy proofs, never incorrect proofs. Use the staged rotation below for an independent API secret. When using the DB password fallback, changing `DB_INIT_PASS` also changes API authentication, but does not change an existing DB engine password; coordinate the actual DB password change separately. A password-derived API key also gives DB password holders operator privileges; use the dedicated secret when those permissions should differ.

### Rolling API key rotation

`OPERATOR_API_SECRET_SECONDARY` is an optional acceptance-only key. Clients always send proofs using the primary `OPERATOR_API_SECRET` (or the DB password fallback); servers accept either explicitly configured key. No secrets or additional proofs are sent to older peers.

1. **Prepare:** keep the current primary key A and set `OPERATOR_API_SECRET_SECONDARY=B` on every replica. Wait until every instance is updated and healthy. All peers still send A, so peers that have not yet updated can communicate.
2. **Switch:** set the primary to B and the secondary to A on every replica. During this rolling update, prepared and switched peers accept both keys, so either direction continues working. Wait until all instances use B as primary.
3. **Retire:** remove the secondary A on every replica. All peers now send B; removed keys cannot authenticate new connections after each restart.

When migrating from the DB-password fallback, keep `DB_INIT_PASS` unchanged and use its current value as A in the switch phase. A retiring legacy DB password may be shorter than 32 bytes; an explicitly empty secondary is rejected. A new dedicated primary still requires at least 32 bytes. Old keys remain valid until retired. Check `getStatus` for `secondaryKeyConfigured` and rollout health before switching; do not skip preparation or silently fall back after failed authentication.

### Environment Variables
| Variable | Description | Default |
|----------|-------------|---------|
|`DB_COMPONENT_NAME` | Hostname for the DB engine component. Required for the DB-less image; leave unset for bundled images (defaults to `localhost`). | `localhost` |
|`INIT_DB_NAME` | Initial database name created immediately after initialization | Required |
|`DB_INIT_PASS` | Root password for the DB engine | `secret` |
|`OPERATOR_AUTH_ENFORCE_AT` | Shared UTC cutoff for requiring operator authentication; ISO datetime ending in `Z` | `2026-10-25T17:48:17Z` |
|`OPERATOR_API_SECRET_SECONDARY` | Optional acceptance-only key for staged rotation; nonempty, may contain the retiring DB password | Unset |
|`OPERATOR_API_SECRET` | Shared random operator authentication secret, minimum 32 bytes | Falls back to `DB_INIT_PASS` |
|`DB_APPNAME` | Cluster identity for operator key derivation | Resolved from Flux hostinfo |
|`OPERATOR_TLS_CERT` / `OPERATOR_TLS_KEY` | PEM certificate and private key file paths for the operator API; set both | Unset (HTTP) |
|`OPERATOR_TLS_CA` | PEM CA file for validating operator peer certificates | System trust store |
|`OPERATOR_TLS_SERVERNAME` | Certificate DNS identity when dialing peer IPs | Peer IP |
|`DB_USER` | Username that can authenticate with the Operator | `root` |
|`DB_PORT` | External DB port for the DB Interface, first item in the Ports array | Required |
|`API_PORT` | External API port for cluster communication, second item in the Ports array | Required |
|`WHITELIST` | Comma-separated list of IPs that can connect remotely to `DB_PORT` | Optional |
|`AUTH_MASTER_ONLY` | If set to `"true"`, only the master node authenticates DB access from the app. This is useful if you want to keep a single reachable master node and return an error page to FDM so only the master node is reachable to end users. | `false` |

## Related Projects

- Flux Postgres Cluster: [https://github.com/RunOnFlux/flux-pg-cluster](https://github.com/RunOnFlux/flux-pg-cluster)
- Flux MongoDB Cluster: [https://github.com/RunOnFlux/flux-mongodb-cluster](https://github.com/RunOnFlux/flux-mongodb-cluster)
- Flux Redis Cluster: [https://github.com/RunOnFlux/flux-redis-cluster](https://github.com/RunOnFlux/flux-redis-cluster)
