# Flux Shared DB

Flux Shared DB is a solution for persistent shared DB storage on the [Flux network](https://www.runonflux.io), It handles replication between various DB engine instances (MySQL, ~~MongoDB~~ and ~~PostgreSQL~~). The operator nodes discover each other using FluxOS API and immediately form a cluster. Each Operator node is connected to a DB engine using a connection pool, received read queries are proxied directly to the DB engine, and write queries are sent to the master node. master nodes timestamp and sequence received write queries and immediately forward them to the slaves.

![FLUX DB Cluster](https://user-images.githubusercontent.com/1296210/184499730-722801f7-e827-4857-902e-fe9a61f36e5f.jpg)

The Operator has 3 interfaces:
1. DB Interface (proxy interface for DB engine)
2. Internal API (used for internal communication)
3. UI API (used for managing cluster)

DB Interface is listening to port 3307 by default and acts as a proxy, so if you're using MySql as a DB engine it will behave as a MySql server.

## Running it on Flux network

In order to use Flux Shared DB you need to run it as a composed docker app with at least these 2 components:
1. a DB engine (ex: [MySql:latest](https://hub.docker.com/_/mysql))
2. [Operator](https://hub.docker.com/r/alihmahdavi/fluxdb)
3. Your Application

### Options/Enviroment Parameters:
* DB_COMPONENT_NAME (required) - hostname for the DB engine component, it should be provided with this format: `flux[db engine component name]_[application name]`
* INIT_DB_NAME - this is the initial database name that will be created immediately after initialization.
* DB_INIT_PASS - root password for DB engine.
* DB_PORT - external DB port for DB interface, this port can be used to connect to the cluster remotely and manage the database.
* API_PORT - external API port for cluster communication.
* DB_APPNAME (required) - the name of the application on the Flux network.
* CLIENT_APPNAME - in case you want to give access to another application to connect, give the name of the application running on Flux
* WHITELIST - comma separated list of IPs that can connect to the DB_PORT remotely


TODO:

-- implement MongoDB support

-- implement PostgreSQL support

