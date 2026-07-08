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
      "DB_USER=root",
      "DB_PORT=13307",
      "API_PORT=17071"
   ]
   ```
- Add your Application component (optional) and connect to the Operator DB port as with the DB-less setup.

Note: `DB_INIT_PASS` sets the DB root password on first boot only. Changing it later will not re-key an already-initialized `/app/db` datadir.

### Environment Variables
| Variable | Description | Default |
|----------|-------------|---------|
|`DB_COMPONENT_NAME` | Hostname for the DB engine component. Required for the DB-less image; leave unset for bundled images (defaults to `localhost`). | `localhost` |
|`INIT_DB_NAME` | Initial database name created immediately after initialization | Required |
|`DB_INIT_PASS` | Root password for the DB engine | `secret` |
|`DB_USER` | Username that can authenticate with the Operator | `root` |
|`DB_PORT` | External DB port for the DB Interface, first item in the Ports array | Required |
|`API_PORT` | External API port for cluster communication, second item in the Ports array | Required |
|`WHITELIST` | Comma-separated list of IPs that can connect remotely to `DB_PORT` | Optional |
|`AUTH_MASTER_ONLY` | If set to `"true"`, only the master node authenticates DB access from the app. This is useful if you want to keep a single reachable master node and return an error page to FDM so only the master node is reachable to end users. | `false` |

## Related Projects

- Flux Postgres Cluster: [https://github.com/RunOnFlux/flux-pg-cluster](https://github.com/RunOnFlux/flux-pg-cluster)
- Flux MongoDB Cluster: [https://github.com/RunOnFlux/flux-mongodb-cluster](https://github.com/RunOnFlux/flux-mongodb-cluster)
- Flux Redis Cluster: [https://github.com/RunOnFlux/flux-redis-cluster](https://github.com/RunOnFlux/flux-redis-cluster)
