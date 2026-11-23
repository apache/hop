#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Re-issues the certificates used by the http mTLS integration tests (0018-mts-*).
#
# The existing private keys are reused, only the certificates and the key and trust stores built
# from them are written again. The certificates are valid for DAYS days, so this only needs to run
# again when they expire.
#
# client-expired.crt is not touched: it expired on purpose and is still signed by the same CA key.
#
# Requires openssl and keytool (JDK).

set -euo pipefail

DAYS=7300 # 20 years
PASS=hop123

HERE="$(cd "$(dirname "$0")" && pwd)"
SERVER_DIR="${HERE}/test-certs"
CLIENT_DIR="${HERE}/../../../../../integration-tests/http/test-certs"
CLIENT_DIR="$(cd "${CLIENT_DIR}" && pwd)"

WORK="$(mktemp -d)"
trap 'rm -rf "${WORK}"' EXIT

cat > "${WORK}/leaf.ext" <<EOF
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid
EOF

# The CA that the mTLS server trusts
openssl req -x509 -new -sha256 -days "${DAYS}" \
  -key "${SERVER_DIR}/ca.key" \
  -subj "/C=BE/ST=Flanders/L=Brussels/O=Hop Test CA/CN=Hop Test Root CA" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -out "${SERVER_DIR}/ca.crt"

# A CA the server does not trust
openssl req -x509 -new -sha256 -days "${DAYS}" \
  -key "${SERVER_DIR}/wrong-ca.key" \
  -subj "/C=US/O=Wrong CA/CN=Evil CA" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -out "${WORK}/wrong-ca.crt"

# sign <key> <subject> <ca.crt> <ca.key> <out.crt>
sign() {
  openssl req -new -sha256 -key "$1" -subj "$2" -out "${WORK}/req.csr"
  openssl x509 -req -sha256 -days "${DAYS}" -in "${WORK}/req.csr" \
    -CA "$3" -CAkey "$4" -set_serial "0x$(openssl rand -hex 16)" \
    -extfile "${WORK}/leaf.ext" -out "$5"
}

sign "${SERVER_DIR}/server.key" "/C=BE/ST=Flanders/L=Brussels/O=Hop Test/CN=localhost" \
  "${SERVER_DIR}/ca.crt" "${SERVER_DIR}/ca.key" "${SERVER_DIR}/server.crt"

sign "${CLIENT_DIR}/client-valid.key" \
  "/C=BE/ST=Flanders/L=Brussels/O=Hop Development/CN=Hop Test User" \
  "${SERVER_DIR}/ca.crt" "${SERVER_DIR}/ca.key" "${CLIENT_DIR}/client-valid.crt"

sign "${CLIENT_DIR}/client-wrong.key" "/C=US/O=Evil Corp/CN=Wrong User" \
  "${WORK}/wrong-ca.crt" "${SERVER_DIR}/wrong-ca.key" "${CLIENT_DIR}/client-wrong.crt"

# Client key stores
openssl pkcs12 -export -name "Hop Test User" -passout "pass:${PASS}" \
  -inkey "${CLIENT_DIR}/client-valid.key" -in "${CLIENT_DIR}/client-valid.crt" \
  -certfile "${SERVER_DIR}/ca.crt" -out "${CLIENT_DIR}/client-valid.p12"

openssl pkcs12 -export -name "Wrong User" -passout "pass:${PASS}" \
  -inkey "${CLIENT_DIR}/client-wrong.key" -in "${CLIENT_DIR}/client-wrong.crt" \
  -certfile "${WORK}/wrong-ca.crt" -out "${CLIENT_DIR}/client-wrong.p12"

openssl pkcs12 -export -name "Hop Expired User" -passout "pass:${PASS}" \
  -inkey "${CLIENT_DIR}/client-expired.key" -in "${CLIENT_DIR}/client-expired.crt" \
  -certfile "${SERVER_DIR}/ca.crt" -out "${CLIENT_DIR}/client-expired.p12"

rm -f "${CLIENT_DIR}/client-valid.jks"
keytool -importkeystore -noprompt \
  -srckeystore "${CLIENT_DIR}/client-valid.p12" -srcstoretype PKCS12 -srcstorepass "${PASS}" \
  -destkeystore "${CLIENT_DIR}/client-valid.jks" -deststoretype JKS -deststorepass "${PASS}" \
  -destkeypass "${PASS}"

# Trust stores with the test CA
for store in truststore.jks:JKS truststore.p12:PKCS12; do
  file="${CLIENT_DIR}/${store%%:*}"
  rm -f "${file}"
  keytool -importcert -noprompt -alias hop-test-ca -file "${SERVER_DIR}/ca.crt" \
    -keystore "${file}" -storetype "${store##*:}" -storepass "${PASS}"
done

echo "Certificates valid until $(openssl x509 -in "${SERVER_DIR}/ca.crt" -noout -enddate)"
