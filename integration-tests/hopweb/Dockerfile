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

FROM selenium/standalone-chrome

COPY integration-tests/hopweb/src/ /home/hop/src/
COPY integration-tests/hopweb/pom.xml /home/hop/

RUN sudo useradd -s /bin/bash -m hop \
  && sudo chown -R hop:hop /home/hop/ \
  && sudo mkdir /surefire-reports/ && sudo chown -R hop:hop /surefire-reports


RUN sudo apt-get -qqy update \
  && sudo apt-get -qqy remove docker docker.io containerd runc \
  && sudo apt-get -qqy --no-install-recommends install \
    ca-certificates \
    curl \
    gnupg \
    lsb-release \
    uidmap \
    apt-utils \
  && sudo mkdir -p /etc/apt/keyrings \
  && curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg \
  && echo \
    "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
    $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null \
  && sudo apt-get -qqy update \
  && sudo apt-get -qqy install docker-ce docker-ce-cli containerd.io docker-compose-plugin \
  && sudo rm -rf /var/lib/apt/lists/* /var/cache/apt/*


USER hop

RUN wget https://dlcdn.apache.org/maven/maven-3/3.8.6/binaries/apache-maven-3.8.6-bin.tar.gz -P /tmp \
  && cd /tmp/ && tar -xvzf apache-maven-3.8.6-bin.tar.gz \
  && cp /home/hop/src/test/resources/hopwebtest-docker.properties /home/hop/src/test/resources/hopwebtest.properties \
  && cp /home/hop/src/test/resources/run-tests.sh /home/hop/ \
  && chmod +x /home/hop/run-tests.sh


ENV PATH=$PATH:/tmp/apache-maven-3.8.6/bin/

WORKDIR /home/hop
ENTRYPOINT ["/bin/bash", "/home/hop/run-tests.sh"]

