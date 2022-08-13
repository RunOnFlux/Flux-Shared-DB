# Flux Shared DB

Flux Shared DB is a cluster operator that handles replication between DB engine instances (MySQL, ~~MongoDB~~ and ~~PostgreSQL~~). Each Operator node is connected to a DB engine. The operator nodes discover each other using FluxOS api and immediatley form a cluster.

![FLUX DB Cluster](https://user-images.githubusercontent.com/1296210/184499730-722801f7-e827-4857-902e-fe9a61f36e5f.jpg)

The Operator has 3 interfaces:
1. DB Interface (proxy interface for DB engine)
2. Internal API (used for internal communication)
3. UI API (used for managing cluster)

DB Interface is listening to port 3307 by default and acts as a proxy, so if you're using MySql as DB engine it will behave as a MySql server.

TODO:

-- implement mongoDB support

-- implement postgreSql support

