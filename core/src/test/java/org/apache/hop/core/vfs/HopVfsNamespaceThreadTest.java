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

package org.apache.hop.core.vfs;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.scope.IHopScope;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Most call sites resolve a file without any variables in hand, so the namespace of the running
 * execution has to reach them through the thread. These are the properties that makes that safe:
 * executions nest, and they hand work to threads they start themselves.
 */
class HopVfsNamespaceThreadTest {

  @AfterEach
  void tearDown() {
    HopVfsNamespaces.reset();
  }

  private HopVfsNamespace namespace(String description) throws HopException {
    // Bypass the registry: these tests are about the thread binding, not about the metadata key.
    return new HopVfsNamespace(description);
  }

  @Test
  @DisplayName("A nested execution puts its parent's namespace back on the way out")
  void nestedBindingRestoresTheParent() throws Exception {
    HopVfsNamespace outer = namespace("outer");
    HopVfsNamespace inner = namespace("inner");

    HopVfsNamespace beforeOuter = HopVfsNamespaces.bindThread(outer);
    try {
      assertSame(outer, HopVfsNamespaces.getCurrent());

      HopVfsNamespace beforeInner = HopVfsNamespaces.bindThread(inner);
      try {
        assertSame(inner, HopVfsNamespaces.getCurrent());
      } finally {
        HopVfsNamespaces.restoreThread(beforeInner);
      }

      assertSame(
          outer,
          HopVfsNamespaces.getCurrent(),
          "The nested execution left its parent resolving files in the wrong namespace");
    } finally {
      HopVfsNamespaces.restoreThread(beforeOuter);
    }
    assertNull(HopVfsNamespaces.getCurrent());
  }

  @Test
  @DisplayName("An execution without a namespace of its own does not drop its parent's")
  void bindingNothingKeepsTheParentBinding() throws Exception {
    HopVfsNamespace outer = namespace("outer");

    HopVfsNamespace beforeOuter = HopVfsNamespaces.bindThread(outer);
    try {
      // An execution running against the metadata of the process gets null from acquire().
      HopVfsNamespace beforeInner = HopVfsNamespaces.bindThread(null);
      HopVfsNamespaces.restoreThread(beforeInner);

      assertSame(outer, HopVfsNamespaces.getCurrent());
    } finally {
      HopVfsNamespaces.restoreThread(beforeOuter);
    }
  }

  @Test
  @DisplayName("Threads an execution starts inherit its namespace")
  void startedThreadsInheritTheNamespace() throws Exception {
    HopVfsNamespace ns = namespace("pipeline");

    HopVfsNamespace before = HopVfsNamespaces.bindThread(ns);
    HopVfsNamespace[] seenByChild = new HopVfsNamespace[1];
    try {
      // This is how Pipeline and Workflow hand work to their transforms and actions.
      Thread child = new Thread(() -> seenByChild[0] = HopVfsNamespaces.getCurrent());
      child.start();
      child.join();
    } finally {
      HopVfsNamespaces.restoreThread(before);
    }

    assertSame(ns, seenByChild[0], "A transform thread cannot see the namespace of its pipeline");
  }

  @Test
  @DisplayName("Unbinding the starter does not disturb threads already running")
  void alreadyStartedThreadsKeepTheirNamespace() throws Exception {
    HopVfsNamespace ns = namespace("pipeline");

    HopVfsNamespace before = HopVfsNamespaces.bindThread(ns);
    HopVfsNamespace[] seenByChild = new HopVfsNamespace[1];
    Thread child = new Thread(() -> seenByChild[0] = HopVfsNamespaces.getCurrent());
    child.start();
    // The server prepares and starts a pipeline on two different request threads, and each hands
    // the thread back to its pool afterwards.
    HopVfsNamespaces.restoreThread(before);
    child.join();

    assertSame(ns, seenByChild[0]);
    assertNull(HopVfsNamespaces.getCurrent());
  }

  @Test
  @DisplayName("Resolving with variables that know nothing falls back to the thread")
  void variablesWithoutMetadataFallBackToTheThread() throws Exception {
    HopVfsNamespace ns = namespace("pipeline");
    IHopMetadataProvider unrelated = mock(IHopMetadataProvider.class);
    assertNull(unrelated.getDescription());

    HopVfsNamespace before = HopVfsNamespaces.bindThread(ns);
    try {
      assertSame(ns, HopVfsNamespaces.resolve(new Variables()));
      assertSame(ns, HopVfsNamespaces.resolve(null));
    } finally {
      HopVfsNamespaces.restoreThread(before);
    }
  }

  @Test
  @DisplayName("A replacement scope decides what current means, thread or not")
  void aReplacementScopeTakesOver() throws Exception {
    // Hop Web serves many people from one JVM, and the thread handling a request says nothing
    // about whose project it belongs to. It binds a per session scope here instead.
    HopVfsNamespace sessionNamespace = namespace("session");
    HopVfsNamespace[] held = new HopVfsNamespace[1];
    try {
      HopVfsNamespaces.setScope(
          new IHopScope<HopVfsNamespace>() {
            @Override
            public HopVfsNamespace get() {
              return held[0];
            }

            @Override
            public void set(HopVfsNamespace namespace) {
              held[0] = namespace;
            }

            @Override
            public void remove() {
              held[0] = null;
            }
          });

      HopVfsNamespaces.bindThread(sessionNamespace);
      assertSame(sessionNamespace, HopVfsNamespaces.getCurrent());

      // Not the thread's: a brand new thread that inherited nothing still sees it.
      HopVfsNamespace[] seenElsewhere = new HopVfsNamespace[1];
      Thread other = new Thread(() -> seenElsewhere[0] = HopVfsNamespaces.getCurrent());
      other.start();
      other.join();
      assertSame(sessionNamespace, seenElsewhere[0]);
    } finally {
      HopVfsNamespaces.setScope(null);
    }

    assertNull(HopVfsNamespaces.getCurrent(), "The per thread default should be back");
  }
}
