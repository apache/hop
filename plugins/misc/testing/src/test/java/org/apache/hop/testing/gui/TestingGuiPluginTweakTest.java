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

package org.apache.hop.testing.gui;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.testing.PipelineTweak;
import org.apache.hop.testing.PipelineUnitTest;
import org.apache.hop.testing.PipelineUnitTestTweak;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for multi-transform unit-test tweak application (issue #2742). Covers pure helpers in
 * {@link TestingGuiPlugin} that do not require a HopGui instance.
 */
class TestingGuiPluginTweakTest {

  @Test
  void resolveTargetsFallsBackToClickedWhenNothingSelected() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    TransformMeta a = transform("A", false);
    TransformMeta b = transform("B", false);
    pipelineMeta.addTransform(a);
    pipelineMeta.addTransform(b);

    List<TransformMeta> targets = TestingGuiPlugin.resolveTweakTargetTransforms(pipelineMeta, b);

    assertEquals(1, targets.size());
    assertSame(b, targets.get(0));
  }

  @Test
  void resolveTargetsUsesSelectionWhenPresent() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    TransformMeta a = transform("A", true);
    TransformMeta b = transform("B", true);
    TransformMeta c = transform("C", false);
    pipelineMeta.addTransform(a);
    pipelineMeta.addTransform(b);
    pipelineMeta.addTransform(c);

    List<TransformMeta> targets = TestingGuiPlugin.resolveTweakTargetTransforms(pipelineMeta, c);

    assertEquals(2, targets.size());
    assertTrue(targets.contains(a));
    assertTrue(targets.contains(b));
  }

  @Test
  void resolveTargetsEmptyWhenNoSelectionAndNoClick() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(transform("A", false));

    List<TransformMeta> targets = TestingGuiPlugin.resolveTweakTargetTransforms(pipelineMeta, null);

    assertTrue(targets.isEmpty());
  }

  @Test
  void applyTweakEnableAddsBypass() {
    PipelineUnitTest unitTest = new PipelineUnitTest();

    TestingGuiPlugin.applyTweakToTransform(unitTest, "A", PipelineTweak.BYPASS_TRANSFORM, true);

    PipelineUnitTestTweak tweak = unitTest.findTweak("A");
    assertEquals(PipelineTweak.BYPASS_TRANSFORM, tweak.getTweak());
    assertEquals("A", tweak.getTransformName());
  }

  @Test
  void applyTweakEnableReplacesExistingDifferentTweak() {
    PipelineUnitTest unitTest = new PipelineUnitTest();
    unitTest.getTweaks().add(new PipelineUnitTestTweak(PipelineTweak.REMOVE_TRANSFORM, "A"));

    TestingGuiPlugin.applyTweakToTransform(unitTest, "A", PipelineTweak.BYPASS_TRANSFORM, true);

    assertEquals(1, unitTest.getTweaks().size());
    assertEquals(PipelineTweak.BYPASS_TRANSFORM, unitTest.findTweak("A").getTweak());
  }

  @Test
  void applyTweakDisableRemovesOnlyMatchingType() {
    PipelineUnitTest unitTest = new PipelineUnitTest();
    unitTest.getTweaks().add(new PipelineUnitTestTweak(PipelineTweak.BYPASS_TRANSFORM, "A"));
    unitTest.getTweaks().add(new PipelineUnitTestTweak(PipelineTweak.REMOVE_TRANSFORM, "B"));

    // Disable REMOVE on A (which is bypassed) — leave A alone
    TestingGuiPlugin.applyTweakToTransform(unitTest, "A", PipelineTweak.REMOVE_TRANSFORM, false);
    // Disable BYPASS on A — remove it
    TestingGuiPlugin.applyTweakToTransform(unitTest, "A", PipelineTweak.BYPASS_TRANSFORM, false);
    // Disable REMOVE on B — remove it
    TestingGuiPlugin.applyTweakToTransform(unitTest, "B", PipelineTweak.REMOVE_TRANSFORM, false);

    assertNull(unitTest.findTweak("A"));
    assertNull(unitTest.findTweak("B"));
    assertTrue(unitTest.getTweaks().isEmpty());
  }

  @Test
  void applyTweakEnableOnMultipleTransforms() {
    PipelineUnitTest unitTest = new PipelineUnitTest();

    TestingGuiPlugin.applyTweakToTransform(unitTest, "A", PipelineTweak.REMOVE_TRANSFORM, true);
    TestingGuiPlugin.applyTweakToTransform(unitTest, "B", PipelineTweak.REMOVE_TRANSFORM, true);
    TestingGuiPlugin.applyTweakToTransform(unitTest, "C", PipelineTweak.REMOVE_TRANSFORM, true);

    assertEquals(3, unitTest.getTweaks().size());
    assertEquals(PipelineTweak.REMOVE_TRANSFORM, unitTest.findTweak("A").getTweak());
    assertEquals(PipelineTweak.REMOVE_TRANSFORM, unitTest.findTweak("B").getTweak());
    assertEquals(PipelineTweak.REMOVE_TRANSFORM, unitTest.findTweak("C").getTweak());
  }

  private static TransformMeta transform(String name, boolean selected) {
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName(name);
    transformMeta.setSelected(selected);
    return transformMeta;
  }
}
