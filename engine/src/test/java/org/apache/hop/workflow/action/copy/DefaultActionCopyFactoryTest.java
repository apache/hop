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

package org.apache.hop.workflow.action.copy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.pipeline.transform.copy.CopyContext;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.actions.start.ActionStart;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Comprehensive test suite for DefaultActionCopyFactory.
 *
 * <p>Tests cover all aspects of action copying including:
 *
 * <ul>
 *   <li>Basic copying functionality
 *   <li>Changed state preservation and clearing
 *   <li>Different copy contexts (DEFAULT, LIGHTWEIGHT, SAME_PIPELINE)
 *   <li>Null handling
 *   <li>ActionMeta copying
 *   <li>Integration with replaceMeta() and clone() methods
 *   <li>Error handling and fallback mechanisms
 * </ul>
 */
class DefaultActionCopyFactoryTest {

  private DefaultActionCopyFactory factory;
  private ActionStart sourceAction;
  private ActionMeta sourceActionMeta;
  private WorkflowMeta workflowMeta;

  @BeforeEach
  void setUp() {
    factory = DefaultActionCopyFactory.getInstance();

    // Create a test action
    sourceAction = new ActionStart("TestAction");
    sourceAction.setDescription("Test Description");
    sourceAction.setChanged(false); // Start with unchanged state

    // Create a test action meta
    sourceActionMeta = new ActionMeta(sourceAction);
    sourceActionMeta.setLaunchingInParallel(true);
    sourceActionMeta.setAttribute("testGroup", "testKey", "testValue");

    // Create a test workflow meta
    workflowMeta = new WorkflowMeta();
    sourceActionMeta.setParentWorkflowMeta(workflowMeta);
  }

  @Test
  void testBasicActionCopy() {
    // Test basic copying functionality
    ActionStart copy = (ActionStart) factory.copy(sourceAction);

    assertNotNull(copy, "Copy should not be null");
    assertNotSame(sourceAction, copy, "Copy should be a different instance");
    assertEquals(sourceAction.getName(), copy.getName(), "Name should be copied");
    assertEquals(
        sourceAction.getDescription(), copy.getDescription(), "Description should be copied");
  }

  @Test
  void testChangedStatePreservation() {
    // Set changed state on the source action
    sourceAction.setChanged(true);
    assertTrue(sourceAction.hasChanged(), "Source action should be marked as changed");

    // Copy with default context (should preserve changed state)
    ActionStart copy = (ActionStart) factory.copy(sourceAction, CopyContext.DEFAULT);

    assertNotNull(copy, "Copy should not be null");
    assertTrue(copy.hasChanged(), "Copy should preserve changed state");
    assertTrue(sourceAction.hasChanged(), "Source should still be changed");
  }

  @Test
  void testLightweightCopy() {
    // Set changed state on source
    sourceAction.setChanged(true);
    assertTrue(sourceAction.hasChanged(), "Source action should be marked as changed");

    // Copy with lightweight context (should NOT preserve changed state)
    ActionStart copy = (ActionStart) factory.copy(sourceAction, CopyContext.LIGHTWEIGHT);

    assertNotNull(copy, "Copy should not be null");
    assertFalse(copy.hasChanged(), "Lightweight copy should not preserve changed state");
    assertTrue(sourceAction.hasChanged(), "Source should still be changed");
  }

  @Test
  void testNullActionHandling() {
    // Test that null inputs are handled gracefully
    ActionStart nullCopy = (ActionStart) factory.copy((ActionStart) null);
    assertNull(nullCopy, "Copying null action should return null");

    ActionStart nullContextCopy = (ActionStart) factory.copy(sourceAction, null);
    assertNotNull(nullContextCopy, "Copy with null context should work (fallback to DEFAULT)");
  }

  @Test
  void testSamePipelineContext() {
    // Test copying within the same pipeline context
    ActionStart copy = (ActionStart) factory.copy(sourceAction, CopyContext.SAME_PIPELINE);

    assertNotNull(copy, "Copy should not be null");
    assertNotSame(sourceAction, copy, "Copy should be a different instance");

    // For actions, SAME_PIPELINE context should work the same as DEFAULT
    // since actions don't have as complex parent relationships as transforms
    assertEquals(sourceAction.getName(), copy.getName(), "Name should be copied");
  }

  @Test
  void testCopyFactoryReusability() {
    // Test that the same factory instance can be used multiple times
    ActionStart copy1 = (ActionStart) factory.copy(sourceAction);
    ActionStart copy2 = (ActionStart) factory.copy(sourceAction);

    assertNotNull(copy1, "First copy should not be null");
    assertNotNull(copy2, "Second copy should not be null");
    assertNotSame(copy1, copy2, "Multiple copies should be different instances");
    assertEquals(copy1.getName(), copy2.getName(), "Both copies should have same name");
  }

  @Test
  void testActionMetaCopy() {
    // Test copying ActionMeta with default context
    ActionMeta copy = factory.copy(sourceActionMeta);

    assertNotNull(copy, "ActionMeta copy should not be null");
    assertNotSame(sourceActionMeta, copy, "Copy should be a different instance");
    assertNotSame(
        sourceActionMeta.getAction(),
        copy.getAction(),
        "Underlying actions should be different instances");

    assertEquals(sourceActionMeta.getName(), copy.getName(), "Action name should be copied");
    assertEquals(
        sourceActionMeta.isLaunchingInParallel(),
        copy.isLaunchingInParallel(),
        "Parallel flag should be copied");
    assertEquals(
        sourceActionMeta.getAttribute("testGroup", "testKey"),
        copy.getAttribute("testGroup", "testKey"),
        "Attributes should be copied");
  }

  @Test
  void testActionMetaWithSamePipelineContext() {
    // Test ActionMeta copying with SAME_PIPELINE context
    ActionMeta copy = factory.copy(sourceActionMeta, CopyContext.SAME_PIPELINE);

    assertNotNull(copy, "ActionMeta copy should not be null");
    assertSame(
        sourceActionMeta.getParentWorkflowMeta(),
        copy.getParentWorkflowMeta(),
        "Parent WorkflowMeta should be preserved with SAME_PIPELINE context");
  }

  @Test
  void testActionMetaChangedStateHandling() {
    // Test that ActionMeta copying properly handles changed state
    sourceActionMeta.setChanged(); // This should mark the underlying action as changed
    assertTrue(sourceActionMeta.hasChanged(), "Source ActionMeta should be marked as changed");

    // Copy with default context
    ActionMeta copy = factory.copy(sourceActionMeta, CopyContext.DEFAULT);

    assertNotNull(copy, "Copy should not be null");
    assertTrue(copy.hasChanged(), "Copy should preserve changed state");
  }

  @Test
  void testIntegrationWithReplaceMeta() {
    // Test that replaceMeta now uses the copy factory
    ActionMeta target = new ActionMeta();

    // Set changed state on source action
    sourceActionMeta.getAction().setChanged(true);
    assertTrue(sourceActionMeta.getAction().hasChanged(), "Source action should be changed");

    // Call replaceMeta (which should use the copy factory)
    target.replaceMeta(sourceActionMeta);

    assertNotNull(target.getAction(), "Target should have an action after replaceMeta");
    assertNotSame(
        sourceActionMeta.getAction(), target.getAction(), "Actions should be different instances");
    assertTrue(target.getAction().hasChanged(), "Target action should preserve changed state");
  }

  @Test
  void testIntegrationWithCloneMethod() {
    // Test that the clone method now uses the copy factory
    sourceAction.setChanged(true);
    assertTrue(sourceAction.hasChanged(), "Source action should be changed");

    ActionStart cloned = (ActionStart) sourceAction.clone();

    assertNotNull(cloned, "Clone should not be null");
    assertNotSame(sourceAction, cloned, "Clone should be a different instance");
    assertTrue(cloned.hasChanged(), "Clone should preserve changed state");
    assertEquals(sourceAction.getName(), cloned.getName(), "Clone should have same name");
  }

  @Test
  void testSingletonBehavior() {
    // Test that getInstance() returns the same instance
    DefaultActionCopyFactory factory1 = DefaultActionCopyFactory.getInstance();
    DefaultActionCopyFactory factory2 = DefaultActionCopyFactory.getInstance();

    assertSame(factory1, factory2, "getInstance() should return the same singleton instance");
  }
}
