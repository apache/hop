/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transform.copy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test for the DefaultTransformMetaCopyFactory to ensure proper state preservation and correct
 * copying behavior in different scenarios.
 */
class DefaultTransformMetaCopyFactoryTest {

  private DefaultTransformMetaCopyFactory factory;
  private TransformMeta sourceTransformMeta;
  private DummyMeta dummyMeta;

  @BeforeEach
  void setUp() {
    factory = DefaultTransformMetaCopyFactory.getInstance();

    // Create a test transform metadata
    dummyMeta = new DummyMeta();
    sourceTransformMeta = new TransformMeta("test-transform", dummyMeta);
    sourceTransformMeta.setDescription("Test transform for copy factory");
    sourceTransformMeta.setSelected(true);
  }

  @Test
  void testBasicCopy() {
    // Test basic copying functionality
    TransformMeta copy = factory.copy(sourceTransformMeta);

    assertNotNull(copy, "Copy should not be null");
    assertNotSame(sourceTransformMeta, copy, "Copy should be a different instance");
    assertEquals(sourceTransformMeta.getName(), copy.getName(), "Name should be copied");
    assertEquals(
        sourceTransformMeta.getDescription(),
        copy.getDescription(),
        "Description should be copied");
    assertEquals(
        sourceTransformMeta.isSelected(), copy.isSelected(), "Selected state should be copied");
    assertNotSame(
        sourceTransformMeta.getTransform(),
        copy.getTransform(),
        "Inner transform should be different instance");
  }

  @Test
  void testChangedStatePreservation() {
    // Set changed state on both the transform meta and inner transform
    sourceTransformMeta.setChanged();
    dummyMeta.setChanged();

    assertTrue(sourceTransformMeta.hasChanged(), "Source should be marked as changed");
    assertTrue(dummyMeta.hasChanged(), "Source inner transform should be marked as changed");

    // Copy with default context (should preserve changed state)
    TransformMeta copy = factory.copy(sourceTransformMeta, CopyContext.DEFAULT);

    assertTrue(copy.hasChanged(), "Copy should preserve changed state");
    assertTrue(
        copy.getTransform().hasChanged(), "Copy inner transform should preserve changed state");
  }

  @Test
  void testLightweightCopy() {
    // Set changed state
    sourceTransformMeta.setChanged();
    dummyMeta.setChanged();

    // Copy with lightweight context (should not preserve changed state)
    TransformMeta copy = factory.copy(sourceTransformMeta, CopyContext.LIGHTWEIGHT);

    assertFalse(copy.hasChanged(), "Lightweight copy should not preserve changed state");
    assertFalse(
        copy.getTransform().hasChanged(),
        "Lightweight copy inner transform should not preserve changed state");
  }

  @Test
  void testNullHandling() {
    // Test that null inputs are handled gracefully
    TransformMeta nullCopy = factory.copy((TransformMeta) null);
    assertNull(nullCopy, "Null input should return null");

    // Test with null inner transform
    TransformMeta emptyTransformMeta = new TransformMeta();
    TransformMeta emptyCopy = factory.copy(emptyTransformMeta);
    assertNotNull(emptyCopy, "Empty transform meta should be copied");
    assertNull(emptyCopy.getTransform(), "Empty copy should have null inner transform");
  }

  @Test
  void testSamePipelineContext() {
    // Test copying within the same pipeline context
    TransformMeta copy = factory.copy(sourceTransformMeta, CopyContext.SAME_PIPELINE);

    assertNotNull(copy, "Copy should not be null");
    assertEquals(sourceTransformMeta.getName(), copy.getName(), "Name should be copied");

    // In same pipeline context, parent references should be preserved
    // (though in this test case we don't set parent references)
  }

  @Test
  void testCopyFactoryReusability() {
    // Test that the same factory instance can be used multiple times
    TransformMeta copy1 = factory.copy(sourceTransformMeta);
    TransformMeta copy2 = factory.copy(sourceTransformMeta);

    assertNotNull(copy1, "First copy should not be null");
    assertNotNull(copy2, "Second copy should not be null");
    assertNotSame(copy1, copy2, "Copies should be different instances");
    assertEquals(copy1.getName(), copy2.getName(), "Both copies should have same name");
  }

  @Test
  void testReplaceMeta() {
    // Test that replaceMeta now uses the copy factory
    TransformMeta target = new TransformMeta();

    // Mark source as changed
    sourceTransformMeta.setChanged();
    dummyMeta.setChanged();

    // Replace metadata
    target.replaceMeta(sourceTransformMeta);

    assertEquals(sourceTransformMeta.getName(), target.getName(), "Name should be replaced");
    assertEquals(
        sourceTransformMeta.getDescription(),
        target.getDescription(),
        "Description should be replaced");
    assertTrue(target.hasChanged(), "Changed state should be preserved");
    assertTrue(
        target.getTransform().hasChanged(), "Inner transform changed state should be preserved");
  }

  @Test
  void testCloneMethod() {
    // Test that the clone method now uses the copy factory
    sourceTransformMeta.setChanged();
    dummyMeta.setChanged();

    TransformMeta cloned = (TransformMeta) sourceTransformMeta.clone();

    assertNotNull(cloned, "Clone should not be null");
    assertNotSame(sourceTransformMeta, cloned, "Clone should be different instance");
    assertEquals(sourceTransformMeta.getName(), cloned.getName(), "Clone should have same name");
    assertTrue(cloned.hasChanged(), "Clone should preserve changed state");
    assertTrue(
        cloned.getTransform().hasChanged(), "Clone inner transform should preserve changed state");
  }
}
