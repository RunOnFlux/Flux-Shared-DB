# MariaDB Follower Regression

This opt-in test uses three disposable MariaDB 11.4 containers, real mysql2
and MariaDB clients, and Socket.IO over TCP. Each node loads the production Operator,
Backlog, DBClient, SQL analyzer, and MySQL emulator. The master transport mirrors
the echo-before-acknowledgement ordering in `ClusterOperator/server.js`.
Discovery and initial snapshot sync are bypassed. Socket.IO connections use the production session-bound operator authentication middleware and client proofs.

The test verifies that wrong/missing passwords and wrong usernames are rejected
by the production password and username checks before pool allocation, using
both mysql2 and MariaDB clients. It also verifies successful authentication using
the historical root/secret credentials and operator API key derivation from the
default DB password.

The test also accepts a legacy writer during the authentication grace period,
advances the cutoff clock, disconnects/rejects that legacy peer, and verifies
that authenticated follower connections remain open and continue replicating.

The test delays follower database requests and checks database setup, a
nine-statement import, connection reuse, prepared writes, duplicate-key errors,
and replicated data on all three nodes. Unit tests separately cover buffered
echoes, local apply failures, and disconnects during application.

Run from the repository root:

```sh
docker compose -p flux-mariadb-echo-test -f test/integration/mariadb-compose.yml up -d --wait
MARIADB_INTEGRATION=1 node test/integration/follower.test.js
docker compose -p flux-mariadb-echo-test -f test/integration/mariadb-compose.yml down -v
```

Ports 17306-17308 must be free. The test creates and resets its databases inside
these disposable containers. Normal `npm test` skips this integration test.
