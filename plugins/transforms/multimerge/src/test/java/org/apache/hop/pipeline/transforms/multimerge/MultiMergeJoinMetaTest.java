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
package org.apache.hop.pipeline.transforms.multimerge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class MultiMergeJoinMetaTest {
  private MultiMergeJoinMeta multiMergeMeta;

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeEach
  void setUp() {
    multiMergeMeta = new MultiMergeJoinMeta();
  }

  @Test
  void testSetGetInputTransforms() {
    // Constructor now initializes to empty list, not null
    assertTrue(multiMergeMeta.getInputTransforms().isEmpty());
    List<String> inputTransforms = Arrays.asList("Transform1", "Transform2");
    multiMergeMeta.setInputTransforms(inputTransforms);
    assertEquals(inputTransforms, multiMergeMeta.getInputTransforms());
  }

  @Test
  void testClone() throws Exception {
    MultiMergeJoinMeta meta = new MultiMergeJoinMeta();
    meta.setKeyFields(Arrays.asList("key1", "key2"));
    meta.setInputTransforms(Arrays.asList("transform1", "transform2", "transform3"));
    // scalars should be cloned using super.clone() - makes sure they're calling super.clone()
    meta.setJoinType("INNER");
    MultiMergeJoinMeta aClone = (MultiMergeJoinMeta) meta.clone();
    assertFalse(aClone == meta);
    assertEquals(meta.getKeyFields(), aClone.getKeyFields());
    assertEquals(meta.getInputTransforms(), aClone.getInputTransforms());
    assertEquals(meta.getJoinType(), aClone.getJoinType());
  }

  @Test
  void testCleanAfterHopToRemove() {
    MultiMergeJoinMeta meta = new MultiMergeJoinMeta();

    // Setup: Create a meta with 3 input transforms and their corresponding key fields
    List<String> inputTransforms = new ArrayList<>();
    inputTransforms.add("Transform1");
    inputTransforms.add("Transform2");
    inputTransforms.add("Transform3");
    meta.setInputTransforms(inputTransforms);

    List<String> keyFields = new ArrayList<>();
    keyFields.add("key1");
    keyFields.add("key2");
    keyFields.add("key3");
    meta.setKeyFields(keyFields);

    // Create a TransformMeta to represent the transform being removed
    TransformMeta removedTransform = new TransformMeta();
    removedTransform.setName("Transform2");

    // Act: Remove the transform
    boolean changed = meta.cleanAfterHopToRemove(removedTransform);

    // Assert: Verify the transform was removed
    assertTrue(changed, "cleanAfterHopToRemove should return true when a transform is removed");
    assertEquals(2, meta.getInputTransforms().size(), "Should have 2 input transforms remaining");
    assertEquals(2, meta.getKeyFields().size(), "Should have 2 key fields remaining");

    // Verify the correct transform was removed
    assertEquals("Transform1", meta.getInputTransforms().get(0));
    assertEquals("Transform3", meta.getInputTransforms().get(1));

    // Verify the corresponding key field was removed
    assertEquals("key1", meta.getKeyFields().get(0));
    assertEquals("key3", meta.getKeyFields().get(1));
  }

  @Test
  void testCleanAfterHopToRemove_NonExistentTransform() {
    MultiMergeJoinMeta meta = new MultiMergeJoinMeta();

    // Setup: Create a meta with input transforms
    List<String> inputTransforms = new ArrayList<>();
    inputTransforms.add("Transform1");
    inputTransforms.add("Transform2");
    meta.setInputTransforms(inputTransforms);

    List<String> keyFields = new ArrayList<>();
    keyFields.add("key1");
    keyFields.add("key2");
    meta.setKeyFields(keyFields);

    // Create a TransformMeta that doesn't exist in the list
    TransformMeta nonExistentTransform = new TransformMeta();
    nonExistentTransform.setName("NonExistent");

    // Act: Try to remove a non-existent transform
    boolean changed = meta.cleanAfterHopToRemove(nonExistentTransform);

    // Assert: Verify nothing was changed
    assertFalse(changed, "cleanAfterHopToRemove should return false when transform not found");
    assertEquals(2, meta.getInputTransforms().size(), "Should still have 2 input transforms");
    assertEquals(2, meta.getKeyFields().size(), "Should still have 2 key fields");
  }

  @Test
  void testCleanAfterHopToRemove_NullTransform() {
    MultiMergeJoinMeta meta = new MultiMergeJoinMeta();

    // Setup: Create a meta with input transforms
    List<String> inputTransforms = new ArrayList<>();
    inputTransforms.add("Transform1");
    meta.setInputTransforms(inputTransforms);

    // Act: Try to remove a null transform
    boolean changed = meta.cleanAfterHopToRemove(null);

    // Assert: Verify nothing was changed
    assertFalse(changed, "cleanAfterHopToRemove should return false when transform is null");
    assertEquals(1, meta.getInputTransforms().size(), "Should still have 1 input transform");
  }
}
