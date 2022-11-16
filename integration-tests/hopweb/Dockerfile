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

RUN sudo useradd -s /bin/bash -m hop

COPY integration-tests/hopweb/src/ /home/hop/src/
COPY integration-tests/hopweb/pom.xml /home/hop/

RUN sudo chown -R hop:hop /home/hop/
RUN sudo mkdir /surefire-reports/ && sudo chown -R hop:hop /surefire-reports

USER hop

RUN wget https://dlcdn.apache.org/maven/maven-3/3.8.6/binaries/apache-maven-3.8.6-bin.tar.gz -P /tmp
RUN cd /tmp/ && tar -xvzf apache-maven-3.8.6-bin.tar.gz

ENV PATH=$PATH:/tmp/apache-maven-3.8.6/bin/

RUN cp /home/hop/src/test/resources/hopwebtest-docker.properties /home/hop/src/test/resources/hopwebtest.properties
RUN cp /home/hop/src/test/resources/run-tests.sh /home/hop/

RUN chmod +x /home/hop/run-tests.sh

WORKDIR /home/hop
ENTRYPOINT ["/bin/bash", "/home/hop/run-tests.sh"]

