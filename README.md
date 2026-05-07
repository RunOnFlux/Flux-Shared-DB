# Flux Shared DB

Flux Shared DB is a solution for persistent, shared database storage on the [Flux network](https://www.runonflux.io). It handles replication between MySQL instances.

Operator nodes discover each other using the FluxOS API and immediately form a cluster. Each Operator node connects to a DB engine through a connection pool. Read queries are proxied directly to the DB engine, while write queries are sent to the master node. The master node timestamps and sequences incoming write queries, then immediately forwards them to slave nodes.

![FLUX DB Cluster](https://user-images.githubusercontent.com/1296210/184499730-722801f7-e827-4857-902e-fe9a61f36e5f.jpg)

## Operator Interfaces

The Operator exposes three interfaces:

1. DB Interface (proxy interface for the DB engine)
2. Internal API (used for internal communication)
3. UI API (used for cluster management)

The DB Interface listens on port `3307` by default and acts as a proxy. If you are using MySQL as the DB engine, it behaves like a MySQL server.

## Running on the Flux Network

To use Flux Shared DB in your project, link it to a DB engine and the Operator handles the rest. A common setup is to run it alongside a DB engine. You can also add your application to the same compose stack and connect directly to the Operator DB port.

To deploy on Flux, go to [Register Flux App](https://cloud.runonflux.io/apps/registerapp), complete your app details, and include these components:

1. DB engine (example: [mysql:latest](https://hub.docker.com/_/mysql))
2. Operator: [runonflux/shared-db](https://hub.docker.com/repository/docker/runonflux/shared-db)
3. Your application (optional)

## Operator Options (Environment Variables)

- `DB_COMPONENT_NAME` (required): Hostname for the DB engine component. It should be provided in this format: `flux[db engine component name]_[application name]`
- `INIT_DB_NAME`: Initial database name created immediately after initialization.
- `DB_INIT_PASS`: Root password for the DB engine.
- `DB_USER`: Username that can authenticate with the Operator. Default: `root`.
- `DB_PORT`: External DB port for the DB Interface. This port can be used to connect to the cluster remotely and manage the database.
- `API_PORT`: External API port for cluster communication.
- `DB_APPNAME` (required): Name of the application on the Flux network.
- `CLIENT_APPNAME` (required): Name of the application on the Flux network. If you want to give access to an application outside the local compose network, provide the name of the application running on Flux.
- `WHITELIST`: Comma-separated list of IPs that can connect remotely to `DB_PORT`.
- `authMasterOnly`: If set to `"true"`, only the master node authenticates DB access from the app. This is useful if you want to keep a single reachable master node and return an error page to FDM so only the master node is reachable to end users.

## Related Projects

- Flux Postgres Cluster: [https://github.com/RunOnFlux/flux-pg-cluster](https://github.com/RunOnFlux/flux-pg-cluster)
- Flux MongoDB Cluster: [https://github.com/RunOnFlux/flux-mongodb-cluster](https://github.com/RunOnFlux/flux-mongodb-cluster)

