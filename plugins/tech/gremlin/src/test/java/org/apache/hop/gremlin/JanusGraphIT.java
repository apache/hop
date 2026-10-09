/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.gremlin;

import java.time.Duration;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/** The Gremlin tests on JanusGraph, which sends its own id types. */
class JanusGraphIT extends GremlinTestBase {

  @BeforeAll
  static void setUp() throws Exception {
    start(
        new GenericContainer<>(DockerImageName.parse("janusgraph/janusgraph:1.1.0"))
            .withExposedPorts(8182)
            .waitingFor(Wait.forLogMessage(".*Channel started at port 8182.*", 1))
            .withStartupTimeout(Duration.ofMinutes(5)));
  }
}
