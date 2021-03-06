#!/bin/bash

MYSQL_OPTS="-N --batch"
MYSQL_HOST="10.1.116.42"
MYSQL_USER="ntopng"
MYSQL_PASS="qwerty1"
MYSQL_DB_NAME=ntopng

MYSQL="/usr/bin/mysql $MYSQL_OPTS -h $MYSQL_HOST -u $MYSQL_USER -p$MYSQL_PASS $MYSQL_DB_NAME"

HISTORY_DEPTH=3
TABLE_PATTERN=$(date -d "-${HISTORY_DEPTH} day" +flows_%Y%m%d)

request()
{
  cat <<EOF
SHOW TABLES LIKE 'flows\_%'
EOF
}

drop()
{
  cat <<EOF
DROP TABLE $1
EOF
}

create()
{
  cat <<EOF
CREATE TABLE IF NOT EXISTS $1 (
  IP_SRC_ADDR int(10) unsigned DEFAULT NULL,
  L4_SRC_PORT smallint(5) unsigned DEFAULT NULL,
  IP_DST_ADDR int(10) unsigned DEFAULT NULL,
  L4_DST_PORT smallint(5) unsigned DEFAULT NULL,
  IN_PACKETS int(10) unsigned DEFAULT NULL,
  IN_BYTES int(10) unsigned DEFAULT NULL,
  OUT_PACKETS int(10) unsigned DEFAULT NULL,
  OUT_BYTES int(10) unsigned DEFAULT NULL,
  LAST_SWITCHED int(10) unsigned DEFAULT NULL,
  TURN tinyint(1) default 0,
  INDEX IP_SRC_ADDR (IP_SRC_ADDR),
  INDEX L4_SRC_PORT (L4_SRC_PORT),
  INDEX IP_DST_ADDR (IP_DST_ADDR),
  INDEX L4_DST_PORT (L4_DST_PORT),
  INDEX LAST_SWITCHED (LAST_SWITCHED))
EOF
}

while read TABLE 
do
  if [[ $TABLE < $TABLE_PATTERN ]]; then
    echo "delete $TABLE"
    $MYSQL -e "$(drop $TABLE)"
  fi
done < <($MYSQL -e "$(request)")

TODAY=$(date +flows_%Y%m%d)
$MYSQL -e "$(create $TODAY)"
echo "create $TODAY"

TOMORROW=$(date -d '1 day' +flows_%Y%m%d)
$MYSQL -e "$(create $TOMORROW)"
echo "create $TOMORROW"

