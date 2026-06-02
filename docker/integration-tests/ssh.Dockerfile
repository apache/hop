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
# Minimal SSH server for integration tests: allows password auth and TCP forwarding
# so Hop can tunnel to Postgres through this container. Public-key auth is also
# enabled for the SSH transform private-key integration test; the matching key
# pair lives in integration-tests/ssh/keys (test-only, never used outside the IT).
#
FROM alpine:3.19

RUN apk add --no-cache openssh && \
    ssh-keygen -A && \
    adduser -D -s /bin/sh hop && \
    echo "hop:hop_ssh_password" | chpasswd && \
    sed -i 's/^#\?PermitRootLogin.*/PermitRootLogin no/' /etc/ssh/sshd_config && \
    sed -i 's/^#\?PasswordAuthentication.*/PasswordAuthentication yes/' /etc/ssh/sshd_config && \
    sed -i 's/^#\?PubkeyAuthentication.*/PubkeyAuthentication yes/' /etc/ssh/sshd_config && \
    sed -i 's/^#\?AllowTcpForwarding.*/AllowTcpForwarding yes/' /etc/ssh/sshd_config && \
    sed -i 's/^#\?GatewayPorts.*/GatewayPorts no/' /etc/ssh/sshd_config

# Authorize the integration-test public key for the hop user. sshd enforces
# StrictModes, so ownership and permissions must be exact or pubkey auth fails.
COPY integration-tests/ssh/keys/it_rsa.pub /tmp/it_rsa.pub
RUN mkdir -p /home/hop/.ssh && \
    cat /tmp/it_rsa.pub > /home/hop/.ssh/authorized_keys && \
    rm -f /tmp/it_rsa.pub && \
    chown -R hop:hop /home/hop/.ssh && \
    chmod 700 /home/hop/.ssh && \
    chmod 600 /home/hop/.ssh/authorized_keys

EXPOSE 22

CMD ["/usr/sbin/sshd", "-D"]
