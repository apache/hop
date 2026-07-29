#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Wraps the stock Oracle Free entrypoint to make the container usable for the Hop TLS tests.
#
# The image does ship configTcps.sh, but its runOracle.sh no longer acts on ENABLE_TCPS (only the
# unused runOracle.sh.orig still does) and the /opt/oracle/scripts/setup hook is not wired in
# either, so nothing runs it for us. We therefore start the stock entrypoint, wait for the
# database, and do the setup ourselves.
#
# What this produces:
#   - a TCPS listener endpoint on ${TCPS_PORT} next to the plain TCP one on 1521
#   - a client wallet in ${ORACLE_BASE}/oradata/clientWallet/${ORACLE_SID}, holding cwallet.sso
#     (auto-login), ewallet.p12, sqlnet.ora and a tnsnames.ora with a TCPS alias
#   - an application user for the tests to connect as
#   - a marker file the compose healthcheck waits for, so the tests never start against a
#     database whose wallet does not exist yet

set -u

READY_MARKER="${ORACLE_BASE}/oradata/.hop-it-ready"
WALLET_DIR="${ORACLE_BASE}/oradata/clientWallet/${ORACLE_SID}"
TCPS_PORT="${TCPS_PORT:-2484}"
# Must match the hostname the tests connect to: it becomes the certificate CN, and the tests
# deliberately leave server DN matching on.
TCPS_HOSTNAME="${TCPS_HOSTNAME:-oracle}"
APP_USER="${APP_USER:-hop}"
APP_USER_PASSWORD="${APP_USER_PASSWORD:-hop_password}"
APP_PDB="${APP_PDB:-FREEPDB1}"

# A restart must not report ready while the wallet is being rebuilt.
rm -f "${READY_MARKER}"

"${ORACLE_BASE}/${RUN_FILE}" &
ORACLE_PID=$!

(
  # Wait for the database itself. checkDBStatus.sh is what the image's own healthcheck uses.
  until "${ORACLE_BASE}/${CHECK_DB_FILE}" >/dev/null 2>&1; do
    if ! kill -0 "${ORACLE_PID}" 2>/dev/null; then
      echo "hop-it: oracle exited before it became available" >&2
      exit 1
    fi
    sleep 5
  done

  # Only generate certificates when there are none. configTcps.sh issues a fresh server
  # certificate and a fresh client wallet every time it runs, so doing this on every start would
  # invalidate any wallet already copied out of the container -- the connection then fails with
  # "signature check failed", which looks nothing like the cause.
  #
  if [ -f "${WALLET_DIR}/cwallet.sso" ]; then
    echo "hop-it: TCPS already configured, keeping the existing wallet in ${WALLET_DIR}"
  else
    echo "hop-it: database is up, configuring TCPS on port ${TCPS_PORT} for host ${TCPS_HOSTNAME}"
    if ! "${ORACLE_BASE}/configTcps.sh" "${TCPS_PORT}" "${TCPS_HOSTNAME}" >/tmp/configTcps.log 2>&1; then
      echo "hop-it: configTcps.sh failed" >&2
      tail -30 /tmp/configTcps.log >&2
      exit 1
    fi
  fi

  # orapki creates these 0600 and owned by oracle. The test container runs as a different user,
  # so without this it cannot read the wallet it is supposed to connect with.
  chmod 644 "${WALLET_DIR}"/* 2>/dev/null

  echo "hop-it: creating application user ${APP_USER} in ${APP_PDB}"
  sqlplus -s / as sysdba <<SQL >/tmp/createUser.log 2>&1
whenever sqlerror exit 1
alter session set container=${APP_PDB};
declare
  already_exists exception;
  pragma exception_init(already_exists, -1920);
begin
  execute immediate 'create user ${APP_USER} identified by "${APP_USER_PASSWORD}"';
exception
  when already_exists then null;
end;
/
grant connect, resource, unlimited tablespace to ${APP_USER};
exit
SQL
  if [ $? -ne 0 ]; then
    echo "hop-it: creating the application user failed" >&2
    tail -30 /tmp/createUser.log >&2
    exit 1
  fi

  touch "${READY_MARKER}"
  echo "hop-it: ready - wallet in ${WALLET_DIR}"
) &

wait "${ORACLE_PID}"
