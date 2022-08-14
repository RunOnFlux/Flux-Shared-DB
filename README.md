# Flux Shared DB

Flux Shared DB is a solution for persistent shared DB storage on [Flux network](https://www.runonflux.io), It handles replication between various DB engine instances (MySQL, ~~MongoDB~~ and ~~PostgreSQL~~). The operator nodes discover each other using FluxOS API and immediately form a cluster. Each Operator node is connected to a DB engine using a connection pool, received read queries are proxied directly to the DB engine, and write queries are sent to the master node. master node timestamps and sequences received write queries and immediately forwards them to the slaves.

![FLUX DB Cluster](https://user-images.githubusercontent.com/1296210/184499730-722801f7-e827-4857-902e-fe9a61f36e5f.jpg)

The Operator has 3 interfaces:
1. DB Interface (proxy interface for DB engine)
2. Internal API (used for internal communication)
3. UI API (used for managing cluster)

DB Interface is listening to port 3307 by default and acts as a proxy, so if you're using MySql as DB engine it will behave like a MySql server.

## Running it on Flux network

Using Flux Shared DB in your project is easy, you just need to link it to a DB engine and it handles the rest. One setup could be using docker compose to run it alongside a DB engine, you can also add your application to the compose and connect it directly to the Operator's DB port, to do that goto [Register Flux App](https://home.runonflux.io/apps/registerapp), fill in your app details, and add these components to it:  
1. DB engine (ex: [mysql:latest](https://hub.docker.com/_/mysql))
2. Operator: [alihmahdavi/fluxdb](https://hub.docker.com/r/alihmahdavi/fluxdb)
3. Your Application (Optional)

### Operator Options (environment variables):
* DB_COMPONENT_NAME (required) - hostname for the DB engine component, it should be provided with this format: `flux[db engine component name]_[application name]`
* INIT_DB_NAME - initial database name that will be created immediately after initialization.
* DB_INIT_PASS - root password for DB engine.
* DB_PORT - external DB port for DB interface, this port can be used to connect to the cluster remotely and manage the database.
* API_PORT - external API port for cluster communication.
* DB_APPNAME (required) - the name of the application on the Flux network.
* CLIENT_APPNAME - in case you want to give access to an application outside the local compose network, give the name of the application running on Flux
* WHITELIST - comma separated list of IPs that can connect to the DB_PORT remotely


TODO:

-- implement MongoDB support

-- implement PostgreSQL support

