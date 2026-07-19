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

package org.apache.hop.spark.table;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Set;
import org.apache.hop.core.exception.HopException;
import org.junit.jupiter.api.Test;

/**
 * Default unit tests — no Delta/Iceberg JARs required. Uses an isolated empty classloader so probe
 * failures are deterministic even if a developer has connectors on the main test classpath.
 */
class SparkLakeConnectorProbeTest {

  /** Classloader that cannot see application classes (no parent). */
  private static ClassLoader emptyClassLoader() {
    return new URLClassLoader(new URL[0], /* parent */ null);
  }

  @Test
  void emptyFormatsDoesNotThrow() {
    assertDoesNotThrow(
        () -> SparkLakeConnectorProbe.verifyClasspath(List.of(), emptyClassLoader()));
    assertDoesNotThrow(() -> SparkLakeConnectorProbe.verifyClasspath(null, emptyClassLoader()));
  }

  @Test
  void missingDeltaFailsWithActionableMessage() {
    HopException ex =
        assertThrows(
            HopException.class,
            () ->
                SparkLakeConnectorProbe.verifyClasspath(
                    Set.of(SparkLakeFormats.FORMAT_DELTA), emptyClassLoader()));
    String msg = ex.getMessage();
    assertTrue(msg.contains("Delta Lake"), msg);
    assertTrue(msg.contains("plugins/engines/spark/lib"), msg);
    assertTrue(msg.contains("--packages"), msg);
    assertTrue(msg.contains("delta-spark_4.1_2.13"), msg);
    assertTrue(msg.contains(SparkLakeFormats.DELTA_EXTENSION), msg);
  }

  @Test
  void missingIcebergFailsWithActionableMessage() {
    HopException ex =
        assertThrows(
            HopException.class,
            () ->
                SparkLakeConnectorProbe.verifyClasspath(
                    Set.of(SparkLakeFormats.FORMAT_ICEBERG), emptyClassLoader()));
    String msg = ex.getMessage();
    assertTrue(msg.contains("Iceberg"), msg);
    assertTrue(msg.contains("plugins/engines/spark/lib"), msg);
    assertTrue(msg.contains("--packages"), msg);
    assertTrue(msg.contains("iceberg-spark-runtime-4.1_2.13"), msg);
    assertTrue(msg.contains(SparkLakeFormats.ICEBERG_EXTENSIONS), msg);
  }

  @Test
  void unknownFormatFails() {
    HopException ex =
        assertThrows(
            HopException.class,
            () -> SparkLakeConnectorProbe.verifyClasspath(List.of("hudi"), emptyClassLoader()));
    assertTrue(ex.getMessage().contains("Unsupported"), ex.getMessage());
    assertTrue(ex.getMessage().contains("hudi"), ex.getMessage());
  }

  @Test
  void formatNamesAreNormalized() {
    HopException ex =
        assertThrows(
            HopException.class,
            () ->
                SparkLakeConnectorProbe.verifyClasspath(List.of("  DELTA  "), emptyClassLoader()));
    assertTrue(ex.getMessage().contains("Delta Lake"), ex.getMessage());
  }

  @Test
  void isClassPresentFalseOnEmptyLoader() {
    assertFalse(
        SparkLakeConnectorProbe.isClassPresent(
            SparkLakeFormats.DELTA_EXTENSION, emptyClassLoader()));
  }

  @Test
  void isClassPresentTrueForJdkClass() {
    assertTrue(
        SparkLakeConnectorProbe.isClassPresent(
            "java.lang.String", ClassLoader.getSystemClassLoader()));
  }

  @Test
  void missingMessageHelpersAreStable() {
    String delta = SparkLakeConnectorProbe.missingDeltaMessage("x.Y");
    assertTrue(delta.contains("x.Y"));
    assertTrue(delta.contains("4.3.1"));
    String iceberg = SparkLakeConnectorProbe.missingIcebergMessage("a.B");
    assertTrue(iceberg.contains("a.B"));
    assertTrue(iceberg.contains("1.11.0"));
  }
}
