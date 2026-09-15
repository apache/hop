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

package org.apache.hop.ui.hopgui;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Hop Web serves many people from one JVM on a pool of request threads, so the same thread serves
 * one session and then another. What a session sees has to be its own, and only a thread with no
 * session at all - an execution thread - may fall back to what it inherited.
 */
class RapSessionScopeTest {

  private RapSessionScope<String> scope() {
    return new RapSessionScope<>(RapSessionScope.VfsNamespaceHolder.class);
  }

  @Test
  @DisplayName("A session with nothing bound gets nothing, not the previous session's value")
  void aSessionNeverInheritsFromThePooledThread() {
    RapSessionScope<String> scope = scope();
    RapSessionScope.Holder sessionA = new RapSessionScope.VfsNamespaceHolder();
    RapSessionScope.Holder sessionB = new RapSessionScope.VfsNamespaceHolder();

    // Session A is served on this thread and binds something. set() also writes the thread local,
    // so that executions it starts inherit it.
    sessionA.value = "namespace of A";
    scope.set("namespace of A");

    assertEquals("namespace of A", scope.valueOf(sessionA));
    assertNull(
        scope.valueOf(sessionB),
        "Session B has opened nothing and must not be handed what session A left on this thread");
  }

  @Test
  @DisplayName("A thread with no session at all falls back to what it inherited")
  void anExecutionThreadFallsBackToTheThread() {
    RapSessionScope<String> scope = scope();
    scope.set("namespace of the session that started this execution");

    assertEquals(
        "namespace of the session that started this execution",
        scope.valueOf(null),
        "An execution runs on threads with no UISession and has to keep resolving as its session does");
  }
}
