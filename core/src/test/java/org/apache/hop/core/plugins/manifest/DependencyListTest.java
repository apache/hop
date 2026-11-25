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
package org.apache.hop.core.plugins.manifest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class DependencyListTest {

  @Test
  void parsesPlainAndDecoratedLines() {
    String content =
        String.join(
            "\n",
            "",
            "The following files have been resolved:",
            "   io.crate:crate-jdbc:jar:2.7.0:provided:/Users/x/.m2/repository/io/crate/crate-jdbc/2.7.0/crate-jdbc-2.7.0.jar\u001B[36m -- module crate.jdbc\u001B[0;1;33m (auto)\u001B[m",
            "   com.jayway.jsonpath:json-path:jar:2.10.0:compile:/Users/x/.m2/repository/com/jayway/jsonpath/json-path/2.10.0/json-path-2.10.0.jar",
            "   io.netty:netty-transport-native-epoll:jar:linux-x86_64:4.1.0:runtime:C:\\Users\\x\\.m2\\netty-transport-native-epoll-4.1.0-linux-x86_64.jar -- module io.netty.transport.epoll.linux.x86_64 (auto)",
            "   org.apache.hop:hop-core:jar:2.20.0-SNAPSHOT:provided",
            "   junit:junit:jar:4.13:test:/x/junit-4.13.jar",
            "   none");
    DependencyList list = DependencyList.parse(content);

    assertEquals(5, list.getArtifacts().size());

    DependencyList.Artifact crate = list.findByFileName("crate-jdbc-2.7.0.jar");
    assertNotNull(crate);
    assertEquals("io.crate", crate.groupId());
    assertEquals("2.7.0", crate.version());
    assertEquals("provided", crate.scope());
    assertNull(crate.classifier());

    DependencyList.Artifact epoll =
        list.findByFileName("netty-transport-native-epoll-4.1.0-linux-x86_64.jar");
    assertNotNull(epoll, "windows path and classifier");
    assertEquals("linux-x86_64", epoll.classifier());
    assertEquals("runtime", epoll.scope());

    DependencyList.Artifact hopCore = list.findByGa("org.apache.hop", "hop-core");
    assertNotNull(hopCore, "line without a file still resolves by coordinates");
    assertEquals("hop-core-2.20.0-SNAPSHOT.jar", hopCore.fileName());
    assertNotNull(list.findByFileName("hop-core-2.20.0-SNAPSHOT.jar"));

    assertNull(list.findByFileName("does-not-exist.jar"));
  }

  @Test
  void emptyListResolvesNothing() {
    DependencyList list = DependencyList.empty();
    assertEquals(0, list.getArtifacts().size());
    assertNull(list.findByFileName("x.jar"));
  }
}
