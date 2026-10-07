#!/bin/bash
set -e

mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" <<SQL
GRANT ALL PRIVILEGES ON etl_control.*   TO '${MYSQL_USER}'@'%';
GRANT ALL PRIVILEGES ON stg_extract.*   TO '${MYSQL_USER}'@'%';
GRANT ALL PRIVILEGES ON stg_transform.* TO '${MYSQL_USER}'@'%';
GRANT ALL PRIVILEGES ON stg_load.*      TO '${MYSQL_USER}'@'%';
FLUSH PRIVILEGES;
SQL