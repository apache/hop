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

package org.apache.hop.testing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class PipelineUnitTestRenameTransformTest {

  @Test
  void renamesInputAndGoldenLocationsAndTweaks() {
    PipelineUnitTest unitTest = new PipelineUnitTest();
    unitTest.getInputDataSets().add(location("reader"));
    unitTest.getGoldenDataSets().add(location("writer"));
    unitTest.getTweaks().add(new PipelineUnitTestTweak(PipelineTweak.BYPASS_TRANSFORM, "skip-me"));

    assertTrue(unitTest.renameTransform("reader", "csv-input"));
    assertTrue(unitTest.renameTransform("writer", "file-output"));
    assertTrue(unitTest.renameTransform("skip-me", "bypass"));

    assertNotNull(unitTest.findInputLocation("csv-input"));
    assertNull(unitTest.findInputLocation("reader"));
    assertNotNull(unitTest.findGoldenLocation("file-output"));
    assertNull(unitTest.findGoldenLocation("writer"));
    assertEquals("bypass", unitTest.findTweak("bypass").getTransformName());
    assertNull(unitTest.findTweak("skip-me"));
  }

  @Test
  void renameIsCaseInsensitiveOnTheOldName() {
    PipelineUnitTest unitTest = new PipelineUnitTest();
    unitTest.getInputDataSets().add(location("Read Customers"));

    assertTrue(unitTest.renameTransform("read customers", "customers"));
    assertEquals("customers", unitTest.findInputLocation("customers").getTransformName());
  }

  @Test
  void noChangeWhenNamesAreEqualOrNothingIsAttached() {
    PipelineUnitTest unitTest = new PipelineUnitTest();
    unitTest.getGoldenDataSets().add(location("out"));

    assertFalse(unitTest.renameTransform("out", "out"));
    assertFalse(unitTest.renameTransform("missing", "other"));
    assertFalse(unitTest.renameTransform(null, "other"));
    assertFalse(unitTest.renameTransform("out", null));
    assertEquals("out", unitTest.findGoldenLocation("out").getTransformName());
  }

  private static PipelineUnitTestSetLocation location(String transformName) {
    PipelineUnitTestSetLocation location = new PipelineUnitTestSetLocation();
    location.setTransformName(transformName);
    location.setDataSetName(transformName + "-set");
    return location;
  }
}
