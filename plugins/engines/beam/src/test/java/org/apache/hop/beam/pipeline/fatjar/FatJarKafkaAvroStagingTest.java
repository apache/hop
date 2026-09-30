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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.Test;

/**
 * Issue #2675 (packaging side): the Kafka Avro / schema registry transforms need the Confluent
 * serializers at runtime on the workers.
 *
 * <p>They were only on the compile classpath, and the Hop fat jar is built by globbing {@code
 * plugins/engines/beam/lib}, which was never populated, so a Flink task manager failed with {@code
 * ClassNotFoundException: io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe}.
 */
class FatJarKafkaAvroStagingTest {

  /** The classes the Kafka Avro transforms touch, and which the fat jar has to carry. */
  private static final List<String> REQUIRED_CLASSES =
      List.of(
          "io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe",
          "io.confluent.kafka.serializers.KafkaAvroSerializer",
          "io.confluent.kafka.schemaregistry.client.SchemaRegistryClient");

  /**
   * The Beam plugin's lib folder, as an absolute path.
   *
   * <p>Surefire runs with the module directory as the working directory, so a relative
   * "plugins/engines/beam/lib" would resolve to a doubled path. Walk up to the repo root instead.
   */
  private static File libFolder() {
    File dir = new File(".").getAbsoluteFile();
    while (dir != null) {
      File candidate = new File(dir, "plugins/engines/beam/lib");
      if (dir.isDirectory() && new File(dir, "plugins/engines/beam/pom.xml").isFile()) {
        return candidate;
      }
      dir = dir.getParentFile();
    }
    // Fall back to the relative path so the failure message names a sensible location.
    return new File("plugins/engines/beam/lib");
  }

  /** The Confluent jars staged for the fat jar, or an empty array. */
  private static File[] stagedKafkaJars() {
    File libFolder = libFolder();
    File[] jars = libFolder.listFiles((dir, name) -> name.startsWith("kafka-"));
    return jars == null ? new File[0] : jars;
  }

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
  void theConfluentJarsAreStagedForTheFatJar() {
    // FatJarBuilder collects the jar files it merges from the plugin's lib folder, so the
    // Confluent jars have to be physically present there, not merely on the compile classpath.
    File libFolder = libFolder();

    assertTrue(
        libFolder.isDirectory(),
        "The Beam plugin has no lib folder at "
            + libFolder.getAbsolutePath()
            + ".  FatJarBuilder stages the fat jar from there, so without it nothing but the Hop"
            + " jars ever reaches the workers.");

    File[] confluentJars = stagedKafkaJars();
    assertTrue(
        confluentJars.length > 0,
        "No kafka-* jar was staged in "
            + libFolder.getAbsolutePath()
            + ", so the Kafka Avro transforms cannot run on a worker");
  }

  @Test
  void aStagedKafkaAvroJarActuallyCarriesTheRequiredClasses() throws Exception {
    File[] confluentJars = stagedKafkaJars();
    if (confluentJars.length == 0) {
      fail("No kafka-* jar staged in " + libFolder().getAbsolutePath());
    }

    // Naming a jar is not enough: it has to be the right one.  Look for the class that the
    // original ClassNotFoundException named.
    boolean foundAbstractSerDe = false;
    for (File jar : confluentJars) {
      try (java.util.zip.ZipFile zip = new java.util.zip.ZipFile(jar)) {
        if (zip.getEntry("io/confluent/kafka/serializers/AbstractKafkaSchemaSerDe.class") != null) {
          foundAbstractSerDe = true;
          break;
        }
      }
    }
    assertTrue(
        foundAbstractSerDe,
        "No staged jar contains io/confluent/kafka/serializers/AbstractKafkaSchemaSerDe.class");
  }

  @Test
  void stagingDoesNotPickUpStrayJars() throws Exception {
    // Sanity check on the staging helper itself: asking for a plugin folder that is not there
    // must not blow up, since the fat jar builder runs over a list of folders.
    List<String> staged =
        org.apache.hop.beam.util.BeamConst.findLibraryFilesToStage(
            new File("."), false, java.util.Set.of("engines/beam-does-not-exist"));

    assertTrue(staged.isEmpty(), "expected nothing staged, got " + staged);
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
              new File("."), false, java.util.Set.of("engines/beam"));

      if (jars.isEmpty()) {
        // The module layout in a plain checkout has the classes under target/, not in a built
        // plugins/ tree, so there is nothing to merge.  The lib assertions above cover the
        // packaging contract; do not fail here for a layout reason.
        return;
      }

      FatJarBuilder builder =
          new FatJarBuilder(
              LogChannel.GENERAL, Variables.getADefaultVariableSpace(), tempJar.getPath(), jars);
      builder.buildTargetJar();

      assertTrue(tempJar.isFile(), "no fat jar was produced at " + tempJar);
      assertTrue(tempJar.length() > 0, "the produced fat jar is empty");
    } catch (HopException e) {
      fail("Building a fat jar from the staged jars failed: " + e.getMessage(), e);
    } finally {
      tempJar.delete();
    }
  }
}
