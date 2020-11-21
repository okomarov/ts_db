initdb /usr/local/var/postgres
createuser -s postgres
pg_ctl -D /usr/local/var/postgres start
