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
# Brings up the Oracle database whose character set is not Unicode.
#
# The image ships a built database and opens it as it is, and a database that is merely opened
# keeps the character set it was built with. ORACLE_CHARACTERSET is only read when a database is
# created, and one is only created when oradata is empty. So the baked database is removed on the
# first start, which is what makes the image build a WE8MSWIN1252 one in its place.
#
# Emptying the volume rather than mounting an empty one is deliberate: mounting the named volume
# copies the baked database in with its oracle ownership, and removing the contents afterwards
# keeps that ownership. An empty volume would arrive owned by root, which the database cannot
# write to.

set -u

MARKER="${ORACLE_BASE}/oradata/.hop-nchar-built"

if [ ! -f "${MARKER}" ]; then
  echo "hop-it: removing the database the image ships, so that one is created with ORACLE_CHARACTERSET=${ORACLE_CHARACTERSET:-unset}"
  shopt -s dotglob
  rm -rf "${ORACLE_BASE}"/oradata/* 2>/dev/null || true
  shopt -u dotglob
  # Read back by the next start, and by a restart after the false failure handled below. It sits
  # beside the database rather than inside it, and the image looks for a directory named after
  # the SID, so it is not mistaken for one.
  touch "${MARKER}"
fi

APP_USER="${APP_USER:-hop}"
APP_USER_PASSWORD="${APP_USER_PASSWORD:-hop_password}"
APP_PDB="${APP_PDB:-FREEPDB1}"

"${ORACLE_BASE}/${RUN_FILE}" &
ORACLE_PID=$!

(
  # checkDBStatus.sh is what the image's own healthcheck uses.
  until "${ORACLE_BASE}/${CHECK_DB_FILE}" >/dev/null 2>&1; do
    if ! kill -0 "${ORACLE_PID}" 2>/dev/null; then
      echo "hop-it: oracle exited before it became available" >&2
      exit 1
    fi
    sleep 5
  done

  # The image creates APP_USER as one of the last steps of building a database, and building this
  # one ends early because the character set is not the one it expects. So the user the tests
  # connect as is never created, and it is created here instead. Guarded by a lookup rather than
  # by the marker, because a restart has to find it already there and leave it alone.
  if ! sqlplus -s / as sysdba <<SQL | grep -q "^${APP_USER}$"
set heading off feedback off pagesize 0
alter session set container=${APP_PDB};
select username from dba_users where username = upper('${APP_USER}');
exit
SQL
  then
    echo "hop-it: creating ${APP_USER} in ${APP_PDB}, which building the database did not get to"
    sqlplus -s / as sysdba <<SQL
alter session set container=${APP_PDB};
create user ${APP_USER} identified by ${APP_USER_PASSWORD} quota unlimited on users;
grant connect, resource, create view to ${APP_USER};
exit
SQL
  fi
) &

wait "${ORACLE_PID}"

# Creating a database with a character set the image does not expect ends with it reporting
# failure even though the database is built and opens. Exiting here would take the whole compose
# run down with it (the suite runs with --abort-on-container-exit), so the start is simply
# repeated: the second one finds the database on the volume and opens it, and the block above
# runs again to add the user.
if [ "${HOP_NCHAR_RETRIED:-0}" = "1" ]; then
  echo "hop-it: the database returned twice, giving up rather than looping" >&2
  exit 1
fi
echo "hop-it: first start returned, opening the database that is now on the volume"
export HOP_NCHAR_RETRIED=1
exec "${ORACLE_BASE}/hop-nchar-entrypoint.sh"
