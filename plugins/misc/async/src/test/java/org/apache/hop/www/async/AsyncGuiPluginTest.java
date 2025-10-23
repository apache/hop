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

package org.apache.hop.www.async;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Test class for AsyncGuiPlugin */
public class AsyncGuiPluginTest {

  @Test
  public void testGetInstance() {
    AsyncGuiPlugin instance1 = AsyncGuiPlugin.getInstance();
    AsyncGuiPlugin instance2 = AsyncGuiPlugin.getInstance();

    assertNotNull(instance1);
    assertSame(instance1, instance2);
  }

  @Test
  public void testConstructor() {
    AsyncGuiPlugin plugin = new AsyncGuiPlugin();
    assertNotNull(plugin);
  }

  @Test
  public void testActionIds() {
    assertNotNull(AsyncGuiPlugin.ACTION_ID_WORKFLOW_GRAPH_ENABLE_ASYNC_LOGGING);
    assertNotNull(AsyncGuiPlugin.ACTION_ID_WORKFLOW_GRAPH_DISABLE_ASYNC_LOGGING);

    assertTrue(AsyncGuiPlugin.ACTION_ID_WORKFLOW_GRAPH_ENABLE_ASYNC_LOGGING.contains("enable"));
    assertTrue(AsyncGuiPlugin.ACTION_ID_WORKFLOW_GRAPH_DISABLE_ASYNC_LOGGING.contains("disable"));

    assertTrue(AsyncGuiPlugin.ACTION_ID_WORKFLOW_GRAPH_ENABLE_ASYNC_LOGGING.contains("async"));
    assertTrue(AsyncGuiPlugin.ACTION_ID_WORKFLOW_GRAPH_DISABLE_ASYNC_LOGGING.contains("async"));
  }

  @Test
  public void testActionIdUniqueness() {
    assertTrue(
        !AsyncGuiPlugin.ACTION_ID_WORKFLOW_GRAPH_ENABLE_ASYNC_LOGGING.equals(
            AsyncGuiPlugin.ACTION_ID_WORKFLOW_GRAPH_DISABLE_ASYNC_LOGGING));
  }

  @Test
  public void testActionIdFormat() {
    String enableId = AsyncGuiPlugin.ACTION_ID_WORKFLOW_GRAPH_ENABLE_ASYNC_LOGGING;
    String disableId = AsyncGuiPlugin.ACTION_ID_WORKFLOW_GRAPH_DISABLE_ASYNC_LOGGING;

    assertTrue(enableId.startsWith("workflow-graph-action"));
    assertTrue(disableId.startsWith("workflow-graph-action"));

    assertTrue(enableId.matches(".*\\d+.*"));
    assertTrue(disableId.matches(".*\\d+.*"));
  }

  @Test
  public void testSingletonBehavior() {
    AsyncGuiPlugin instance1 = AsyncGuiPlugin.getInstance();
    AsyncGuiPlugin instance2 = AsyncGuiPlugin.getInstance();
    AsyncGuiPlugin instance3 = AsyncGuiPlugin.getInstance();

    assertSame(instance1, instance2);
    assertSame(instance2, instance3);
    assertSame(instance1, instance3);
  }

  @Test
  public void testInstanceNotNull() {
    AsyncGuiPlugin instance = AsyncGuiPlugin.getInstance();
    assertNotNull(instance);
    assertTrue(instance instanceof AsyncGuiPlugin);
  }
}
