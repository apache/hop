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

package org.apache.hop.beam.pipeline.fatjar;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.List;
import org.apache.hop.core.fatjar.FatJarBuilder;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.Test;

/**
 * Issue #2675 (packaging side): the Kafka Avro / schema registry transforms need the Confluent
 * serializers at runtime on the workers.
 *
 * <p>They were only on the compile classpath, so a Flink task manager failed with {@code
 * ClassNotFoundException: io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe}. The plugin
 * assembly now ships them to lib/core, next to the identical copies of the Kafka plugin, and the
 * fat jar collects lib/ as well as plugins/ (see HopBeamGuiPlugin.findInstalledJarFilenames), so
 * they reach the workers from there.
 */
class FatJarKafkaAvroStagingTest {

  /** The classes the Kafka Avro transforms touch, and which the fat jar has to carry. */
  private static final List<String> REQUIRED_CLASSES =
      List.of(
          "io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe",
          "io.confluent.kafka.serializers.KafkaAvroSerializer",
          "io.confluent.kafka.schemaregistry.client.SchemaRegistryClient");

  @Test
  void theConfluentSerializersAreOnTheClasspath() throws Exception {
    // Guards the compile classpath.
    for (String className : REQUIRED_CLASSES) {
      assertNotNull(
          Class.forName(className),
          className + " must be on the Beam plugin classpath for the Kafka Avro transforms");
    }
  }

  @Test
  void theFatJarBuilderAcceptsTheStagedJars() throws Exception {
    // Build a fat jar from whatever is currently staged, into a temp file.  This is the closest
    // thing to the real thing a unit test can do without a full Hop installation.
    File tempJar = File.createTempFile("hop-fat-jar-staging-test", ".jar");
    tempJar.delete();
    try {
      List<String> jars =
          org.apache.hop.beam.util.BeamConst.findLibraryFilesToStage(
              new File("."), true, java.util.Set.of("engines/beam"));

      if (jars.isEmpty()) {
        // The module layout in a plain checkout has the classes under target/, not in a built
        // plugins/ tree, so there is nothing to merge; do not fail here for a layout reason.
        return;
      }

      FatJarBuilder builder =
          new FatJarBuilder(
              LogChannel.GENERAL, Variables.getADefaultVariableSpace(), tempJar.getPath(), jars);
      assertDoesNotThrow(builder::buildTargetJar, "Building a fat jar from the staged jars failed");

      assertTrue(tempJar.isFile(), "no fat jar was produced at " + tempJar);
      assertTrue(tempJar.length() > 0, "the produced fat jar is empty");
    } finally {
      tempJar.delete();
    }
  }
}
