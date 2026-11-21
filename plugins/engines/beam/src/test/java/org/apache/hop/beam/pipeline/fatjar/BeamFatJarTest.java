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

package org.apache.hop.beam.pipeline.fatjar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class BeamFatJarTest {

  @Test
  void nativeTokenDetection() {
    assertTrue(BeamFatJar.isNativeSparkClientVersion("native"));
    assertTrue(BeamFatJar.isNativeSparkClientVersion("NATIVE"));
    assertTrue(BeamFatJar.isNativeSparkClientVersion(" native "));
    assertTrue(BeamFatJar.isNativeSparkClientVersion("native-provided"));
    assertTrue(BeamFatJar.isNativeProvidedSparkClientVersion("native-provided"));
    assertFalse(BeamFatJar.isNativeProvidedSparkClientVersion("native"));
    assertFalse(BeamFatJar.isNativeSparkClientVersion(null));
    assertFalse(BeamFatJar.isNativeSparkClientVersion(""));
    assertFalse(BeamFatJar.isNativeSparkClientVersion("3.5.8"));
  }

  @Test
  void aBeamJarIgnoresTheFormerNativeTokens() {
    String before = System.getProperty("HOP_SPARK_CLIENT_VERSION");
    try {
      // Older setups set HOP_SPARK_CLIENT_VERSION=native-provided for a native Spark jar
      System.setProperty("HOP_SPARK_CLIENT_VERSION", "native-provided");
      assertNull(BeamFatJar.beamSparkClientVersion(null));
      assertNull(BeamFatJar.beamSparkClientVersion("native"));
      assertEquals("3.5.8", BeamFatJar.beamSparkClientVersion("3.5.8"));
      System.setProperty("HOP_SPARK_CLIENT_VERSION", "3.4.4");
      assertEquals("3.4.4", BeamFatJar.beamSparkClientVersion(null));
    } finally {
      if (before == null) {
        System.clearProperty("HOP_SPARK_CLIENT_VERSION");
      } else {
        System.setProperty("HOP_SPARK_CLIENT_VERSION", before);
      }
    }
  }
}
