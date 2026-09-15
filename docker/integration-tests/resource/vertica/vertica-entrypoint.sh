#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Boots a single node Vertica database inside opentext/vertica-k8s.
#
# That image is built for the VerticaDB Kubernetes operator: it has no entry point and
# no admintools, and it does not create a database on its own. The operator normally
# generates the TLS material, starts the node management agent (NMA) and then calls
# "vcluster create_db" over its REST API. This script does the same three things so the
# image can be used from plain docker compose.

set -euo pipefail

DB_NAME=${VERTICA_DB_NAME:-vmart}
DB_USER=${VERTICA_DB_USER:-dbadmin}
DB_PASSWORD=${VERTICA_DB_PASSWORD:-}
DATA_PATH=${VERTICA_DATA_PATH:-/data}
CERT_DIR=/opt/vertica/config/https_certs

# The image ships the Vertica files owned by uid 997 but carries no matching passwd entry,
# because the operator supplies one through the pod security context.
if ! getent passwd "${DB_USER}" > /dev/null; then
  groupadd -g 995 verticadba 2> /dev/null || true
  useradd -u 997 -g 995 -m -d "/home/${DB_USER}" -s /bin/bash "${DB_USER}" 2> /dev/null || true
fi

mkdir -p "${DATA_PATH}" /opt/vertica/log /vertica/tmp
chown -R 997:995 "${DATA_PATH}" /opt/vertica/log /opt/vertica/config /vertica "/home/${DB_USER}"

as_dbadmin() {
  su "${DB_USER}" -c "$1"
}

# The NMA refuses to start without TLS material. The image ships the generator the
# operator uses; it writes into the current directory under its own names, which then
# have to be linked to the names the NMA and vcluster look for.
if [ ! -f "${CERT_DIR}/rootca.pem" ]; then
  echo "Generating TLS certificates for the node management agent"
  as_dbadmin "cd ${CERT_DIR} && /opt/vertica/bin/gen_httpstls_json.sh" > /tmp/gen_certs.log 2>&1 ||
    { echo "Certificate generation failed:"; cat /tmp/gen_certs.log; exit 1; }
  cd "${CERT_DIR}"
  cp -f rootca_cert.pem rootca.pem            # trusted CA, used by the NMA and by vcluster
  cp -f nma_cert.pem vertica_https.pem        # NMA server certificate
  cp -f nma_key.pem vertica_https.key
  cp -f "${DB_USER}_cert.pem" "${DB_USER}.pem" # vcluster client certificate
  cp -f "${DB_USER}_key.pem" "${DB_USER}.key"
  chown 997:995 rootca.pem vertica_https.pem vertica_https.key "${DB_USER}.pem" "${DB_USER}.key"
  cd /
fi

echo "Starting the node management agent"
rm -f /opt/vertica/config/node_management_agent.pid
as_dbadmin "nohup /opt/vertica/bin/node_management_agent > /opt/vertica/log/nma.log 2>&1 & disown"

for _ in $(seq 1 60); do
  if (exec 3<> /dev/tcp/127.0.0.1/5554) 2> /dev/null; then
    break
  fi
  sleep 1
done
if ! (exec 3<> /dev/tcp/127.0.0.1/5554) 2> /dev/null; then
  echo "The node management agent did not come up:"
  cat /opt/vertica/log/nma.log
  exit 1
fi

# vcluster addresses the node by IP, and only IPv4 is supported.
HOST_IP=$(grep -oE '^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+' /etc/hosts | grep -v '^127\.' | head -1)

if [ ! -d "${DATA_PATH}/${DB_NAME}" ]; then
  echo "Creating database ${DB_NAME} on ${HOST_IP}"
  as_dbadmin "/opt/vertica/bin/vcluster create_db \
      --db-name ${DB_NAME} \
      --hosts ${HOST_IP} \
      --catalog-path ${DATA_PATH} \
      --data-path ${DATA_PATH} \
      --password '${DB_PASSWORD}' \
      --skip-package-install \
      --force-cleanup-on-failure \
      --config-param HttpServerConf=${CERT_DIR}/httpstls.json"
else
  echo "Restarting existing database ${DB_NAME} on ${HOST_IP}"
  as_dbadmin "/opt/vertica/bin/vcluster start_db --db-name ${DB_NAME} --hosts ${HOST_IP} --password '${DB_PASSWORD}'"
fi

echo "Vertica is ready on port 5433, database ${DB_NAME}"

# create_db leaves the server running in the background, so hold the container open and
# forward a stop signal to the database rather than letting the node be killed outright.
trap "as_dbadmin \"/opt/vertica/bin/vcluster stop_db --db-name ${DB_NAME} --password '${DB_PASSWORD}'\" || true; exit 0" TERM INT
tail -f /opt/vertica/log/nma.log &
wait $!
