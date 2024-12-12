#!/usr/bin/env bash

set -eux
cd $(dirname "$0")

MYSQL="mysql"
if [ "${ENV:-}" == "local-dev" ]; then
	MYSQL="docker exec -i isuride-mysql mysql"
fi

if test -f /home/isucon/env.sh; then
	. /home/isucon/env.sh
fi

ISUCON_DB_HOST=${ISUCON_DB_HOST:-127.0.0.1}
ISUCON_DB_PORT=${ISUCON_DB_PORT:-3306}
ISUCON_DB_USER=${ISUCON_DB_USER:-isucon}
ISUCON_DB_PASSWORD=${ISUCON_DB_PASSWORD:-isucon}
ISUCON_DB_NAME=${ISUCON_DB_NAME:-isuride}

# MySQLを初期化
echo 'drop database if exists isuride; create database isuride;' | $MYSQL -u"$ISUCON_DB_USER" \
	-p"$ISUCON_DB_PASSWORD" \
	--host "$ISUCON_DB_HOST" \
	--port "$ISUCON_DB_PORT" \
	"$ISUCON_DB_NAME"

$MYSQL -u"$ISUCON_DB_USER" \
	-p"$ISUCON_DB_PASSWORD" \
	--host "$ISUCON_DB_HOST" \
	--port "$ISUCON_DB_PORT" \
	"$ISUCON_DB_NAME" <1-schema.sql

$MYSQL -u"$ISUCON_DB_USER" \
	-p"$ISUCON_DB_PASSWORD" \
	--host "$ISUCON_DB_HOST" \
	--port "$ISUCON_DB_PORT" \
	"$ISUCON_DB_NAME" <2-master-data.sql

gzip -dkc 3-initial-data.sql.gz | $MYSQL -u"$ISUCON_DB_USER" \
	-p"$ISUCON_DB_PASSWORD" \
	--host "$ISUCON_DB_HOST" \
	--port "$ISUCON_DB_PORT" \
	"$ISUCON_DB_NAME"

$MYSQL -u"$ISUCON_DB_USER" \
	-p"$ISUCON_DB_PASSWORD" \
	--host "$ISUCON_DB_HOST" \
	--port "$ISUCON_DB_PORT" \
	"$ISUCON_DB_NAME" <4-index.sql
