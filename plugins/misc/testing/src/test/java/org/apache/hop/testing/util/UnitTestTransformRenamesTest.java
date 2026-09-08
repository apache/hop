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

package org.apache.hop.testing.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.testing.PipelineUnitTest;
import org.apache.hop.testing.PipelineUnitTestSetLocation;
import org.junit.jupiter.api.Test;

class UnitTestTransformRenamesTest {

  @Test
  void revertWhenUndoRestoredTheOldTransformName() {
    PipelineMeta pipelineMeta = pipelineWithTransform("reader");
    PipelineUnitTest unitTest = unitTestOn("csv-input");
    Map<String, Object> stateMap = new HashMap<>();
    UnitTestTransformRenames.record(stateMap, "reader", "csv-input");

    assertTrue(
        UnitTestTransformRenames.revertIfUndoRestoredOldNames(pipelineMeta, unitTest, stateMap));
    assertEquals("reader", unitTest.findInputLocation("reader").getTransformName());
    assertFalse(UnitTestTransformRenames.hasPending(stateMap));
  }

  @Test
  void leaveRenameInPlaceWhenNewNameStillExists() {
    PipelineMeta pipelineMeta = pipelineWithTransform("csv-input");
    PipelineUnitTest unitTest = unitTestOn("csv-input");
    Map<String, Object> stateMap = new HashMap<>();
    UnitTestTransformRenames.record(stateMap, "reader", "csv-input");

    assertFalse(
        UnitTestTransformRenames.revertIfUndoRestoredOldNames(pipelineMeta, unitTest, stateMap));
    assertEquals("csv-input", unitTest.findInputLocation("csv-input").getTransformName());
    assertTrue(UnitTestTransformRenames.hasPending(stateMap));
  }

  @Test
  void revertAllRestoresDiskStateOnClose() {
    PipelineUnitTest unitTest = unitTestOn("csv-input");
    Map<String, Object> stateMap = new HashMap<>();
    UnitTestTransformRenames.record(stateMap, "reader", "csv-input");

    assertTrue(UnitTestTransformRenames.revertAll(unitTest, stateMap));
    assertEquals("reader", unitTest.findInputLocation("reader").getTransformName());
    assertFalse(UnitTestTransformRenames.hasPending(stateMap));
  }

  @Test
  void chainedRenamesUnwindNewestFirst() {
    PipelineMeta pipelineMeta = pipelineWithTransform("b");
    PipelineUnitTest unitTest = unitTestOn("c");
    Map<String, Object> stateMap = new HashMap<>();
    UnitTestTransformRenames.record(stateMap, "a", "b");
    UnitTestTransformRenames.record(stateMap, "b", "c");

    assertTrue(
        UnitTestTransformRenames.revertIfUndoRestoredOldNames(pipelineMeta, unitTest, stateMap));
    assertEquals("b", unitTest.findInputLocation("b").getTransformName());
    assertEquals(1, UnitTestTransformRenames.pending(stateMap).size());
    assertEquals("a", UnitTestTransformRenames.pending(stateMap).get(0).getOldName());
  }

  private static PipelineMeta pipelineWithTransform(String name) {
    PipelineMeta pipelineMeta = new PipelineMeta();
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName(name);
    pipelineMeta.addTransform(transformMeta);
    return pipelineMeta;
  }

  private static PipelineUnitTest unitTestOn(String transformName) {
    PipelineUnitTest unitTest = new PipelineUnitTest();
    PipelineUnitTestSetLocation location = new PipelineUnitTestSetLocation();
    location.setTransformName(transformName);
    location.setDataSetName("set");
    unitTest.getInputDataSets().add(location);
    return unitTest;
  }
}
