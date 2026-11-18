/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hop.bom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/** Exercises BOM generation through its public command-line entry point. */
class HopBomTest {
  private static final String BASE = "/project/dependencyManagement/dependencies/dependency";

  @TempDir Path temporaryDirectory;

  @Test
  void checkedInBomMatchesGeneratedPomExactly() throws Exception {
    final var root = sourceRoot();
    final var generated = temporaryDirectory.resolve("pom.xml");

    BomGenerator.main(new String[] {root.toString(), generated.toString()});

    assertEquals(Files.readString(root.resolve("bom/pom.xml")), Files.readString(generated));
  }

  @Test
  void includesDirectAndRecursivelyDeclaredModulesInBaseManagement() throws Exception {
    final var root =
        fixtureRoot(
            """
        <modules>
          <module>direct</module>
          <module>nested</module>
        </modules>
        """);
    module(root, "direct", "hop-direct", "");
    module(
        root,
        "nested",
        "hop-nested",
        """
        <packaging>pom</packaging>
        <modules><module>leaf</module></modules>
        """);
    module(root, "nested/leaf", "hop-leaf", "");

    final var generated = generate(root, "generated.xml");

    assertEquals(Set.of("hop-direct:jar", "hop-leaf:jar"), coordinates(generated, BASE));
    assertEquals("hop-bom", text(generated, "/project/artifactId"));
    assertEquals("0", text(generated, "count(/project/profiles/profile)"));
  }

  @Test
  void promotesBaseModulesRegardlessOfActivationAndIgnoresOtherProfiles() throws Exception {
    final var root =
        fixtureRoot(
            """
        <modules><module>direct</module></modules>
        <profiles>
          <profile>
            <id>base</id>
            <activation>
              <property><name>hop.base</name><value>enabled</value></property>
            </activation>
            <modules><module>base</module></modules>
          </profile>
          <profile>
            <id>assemblies</id>
            <activation><activeByDefault>true</activeByDefault></activation>
            <modules><module>missing-assemblies</module></modules>
          </profile>
          <profile>
            <id>config-doc</id>
            <modules><module>invalid-config-doc</module></modules>
          </profile>
          <profile>
            <id>webtest</id>
            <modules><module>missing-webtest</module></modules>
          </profile>
          <profile>
            <id>optional</id>
            <modules><module>missing-optional</module></modules>
          </profile>
        </profiles>
        """);
    module(root, "direct", "hop-direct", "");
    module(
        root,
        "base",
        "hop-base",
        """
        <packaging>pom</packaging>
        <profiles>
          <profile>
            <id>base</id>
            <activation><jdk>[999,)</jdk></activation>
            <modules><module>jdk</module></modules>
          </profile>
          <profile>
            <id>optional</id>
            <modules><module>missing-optional</module></modules>
          </profile>
        </profiles>
        """);
    module(root, "base/jdk", "hop-jdk", "");
    write(root.resolve("invalid-config-doc/pom.xml"), "not XML");

    final var generated = generate(root, "generated.xml");

    assertEquals(Set.of("hop-direct:jar", "hop-jdk:jar"), coordinates(generated, BASE));
    assertEquals("0", text(generated, "count(/project/profiles)"));
  }

  @Test
  void rejectsDuplicateCoordinates() throws Exception {
    final var root =
        fixtureRoot(
            """
        <modules><module>first</module><module>second</module></modules>
        """);
    module(root, "first", "hop-duplicate", "");
    module(root, "second", "hop-duplicate", "");
    final var output = temporaryDirectory.resolve("generated.xml");

    assertThrows(
        IllegalArgumentException.class,
        () -> BomGenerator.main(new String[] {root.toString(), output.toString()}));
  }

  @Test
  void rejectsModulesWithDifferentReleaseVersions() throws Exception {
    final var root = fixtureRoot("<modules><module>wrong-version</module></modules>");
    module(root, "wrong-version", "hop-wrong-version", "<version>2.0-SNAPSHOT</version>");
    final var output = temporaryDirectory.resolve("generated.xml");

    assertThrows(
        IllegalArgumentException.class,
        () -> BomGenerator.main(new String[] {root.toString(), output.toString()}));
  }

  @Test
  void emitsOnlyJarsIgnoringAssembliesAndTraversingPomAndZipModules() throws Exception {
    final var root =
        fixtureRoot(
            """
        <modules>
          <module>plugin</module><module>plain</module>
          <module>aggregator</module><module>archive</module>
        </modules>
        """);
    module(root, "plugin", "hop-plugin", "");
    module(root, "plain", "hop-plain", "<packaging>jar</packaging>");
    module(
        root,
        "aggregator",
        "hop-aggregator",
        """
        <packaging>pom</packaging>
        <modules><module>leaf</module></modules>
        """);
    module(root, "aggregator/leaf", "hop-pom-leaf", "");
    module(
        root,
        "archive",
        "hop-archive",
        """
        <packaging>zip</packaging>
        <modules><module>leaf</module></modules>
        """);
    module(root, "archive/leaf", "hop-zip-leaf", "");
    write(
        root.resolve("plugin/src/assembly/assembly.xml"),
        """
        <assembly xmlns="http://maven.apache.org/ASSEMBLY/2.2.0">
          <id>plugin</id>
          <formats><format>zip</format></formats>
        </assembly>
        """);

    final var generated = generate(root, "generated.xml");

    assertEquals(
        Set.of("hop-plugin:jar", "hop-plain:jar", "hop-pom-leaf:jar", "hop-zip-leaf:jar"),
        coordinates(generated, BASE));
    assertEquals("0", text(generated, "count(/project/profiles)"));
  }

  @Test
  void ignoresUndeclaredWorktreesAndStandalonePoms() throws Exception {
    final var root = fixtureRoot("<modules><module>declared</module></modules>");
    module(root, "declared", "hop-declared", "");
    // These would fail validation if generation scanned the filesystem instead of module edges.
    module(root, "worktrees/other/engine", "hop-declared", "<version>99.0</version>");
    module(root, "engine-bench", "hop-bench", "<version>99.0</version>");
    module(root, "tools/spark-client-pack", "hop-pack", "<version>99.0</version>");
    write(root.resolve("unrelated/pom.xml"), "not XML");

    final var generated = generate(root, "generated.xml");

    assertEquals(Set.of("hop-declared:jar"), coordinates(generated, BASE));
  }

  @Test
  void zeroArgumentsFindsRootInCurrentDirectoryOrOneParent() throws Exception {
    final var root = fixtureRoot("<modules><module>module</module></modules>");
    module(root, "module", "hop-module", "");
    final var output = root.resolve("bom/pom.xml");
    Files.createDirectories(output.getParent());

    for (final var directory : List.of(root, root.resolve("bom"))) {
      Files.deleteIfExists(output);

      runGenerator(directory, true);

      assertEquals("hop-bom", text(parse(output), "/project/artifactId"));
      assertEquals(Set.of("hop-module:jar"), coordinates(parse(output), BASE));
    }
  }

  @Test
  void zeroArgumentsRejectsRootMoreThanOneParentAway() throws Exception {
    final var root = fixtureRoot("");
    final var directory = Files.createDirectories(root.resolve("bom/child"));

    runGenerator(directory, false);
  }

  @Test
  void zeroArgumentsRequiresBothRootMarkers() throws Exception {
    final var root = fixtureRoot("");
    Files.delete(root.resolve("Jenkinsfile"));

    runGenerator(root, false);
  }

  @Test
  void explicitRootArgumentDefaultsOutputToBomPom() throws Exception {
    final var root = fixtureRoot("<modules><module>module</module></modules>");
    module(root, "module", "hop-module", "");

    runGenerator(temporaryDirectory, true, root.toString());

    final var generated = parse(root.resolve("bom/pom.xml"));
    assertEquals("hop-bom", text(generated, "/project/artifactId"));
    assertEquals(Set.of("hop-module:jar"), coordinates(generated, BASE));
  }

  @Test
  void generatesDeterministicallyWithoutUsingExistingBomAsTemplate() throws Exception {
    final var root = fixtureRoot("<modules><module>module</module><module>bom</module></modules>");
    module(root, "module", "hop-module", "");

    // The first invocation has no existing BOM at all.
    final var first = temporaryDirectory.resolve("first.xml");
    BomGenerator.main(new String[] {root.toString(), first.toString()});
    // A second invocation must ignore even an unusable existing BOM, not copy or patch it.
    write(root.resolve("bom/pom.xml"), "not a BOM template");
    final var second = temporaryDirectory.resolve("second.xml");
    BomGenerator.main(new String[] {root.toString(), second.toString()});

    assertEquals(Files.readString(first), Files.readString(second));
    assertEquals("hop-bom", text(parse(first), "/project/artifactId"));
    assertEquals(Set.of("hop-module:jar"), coordinates(parse(first), BASE));
    assertEquals("not a BOM template", Files.readString(root.resolve("bom/pom.xml")));
  }

  private static Path sourceRoot() {
    final var basedir = Path.of(System.getProperty("basedir", ".")).toAbsolutePath().normalize();
    for (final var candidate : new Path[] {basedir, basedir.getParent()}) {
      if (candidate != null
          && Files.isRegularFile(candidate.resolve("mvnw"))
          && Files.isRegularFile(candidate.resolve("Jenkinsfile"))) {
        return candidate;
      }
    }
    throw new IllegalStateException("Hop root not found in basedir or its parent: " + basedir);
  }

  private void runGenerator(final Path directory, final boolean succeeds, final String... arguments)
      throws Exception {
    final var command = new ArrayList<String>();
    command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
    command.add("-cp");
    command.add(
        Path.of(BomGenerator.class.getProtectionDomain().getCodeSource().getLocation().toURI())
            .toString());
    command.add(BomGenerator.class.getName());
    command.addAll(List.of(arguments));
    final var log = Files.createTempFile(temporaryDirectory, "generator-", ".log");
    final var process =
        new ProcessBuilder(command)
            // ProcessBuilder requires a File for the child process working directory.
            .directory(directory.toFile())
            .redirectErrorStream(true)
            .redirectOutput(log.toFile())
            .start();
    try (final var input = process.getInputStream();
        final var error = process.getErrorStream();
        final var output = process.getOutputStream()) {
      output.close();
      assertTrue(process.waitFor(30, TimeUnit.SECONDS), "Generator timed out in " + directory);
      final var message = Files.readString(log);
      if (succeeds) {
        assertEquals(0, process.exitValue(), message);
      } else {
        assertNotEquals(0, process.exitValue(), message);
        assertTrue(message.contains("Cannot find mvnw and Jenkinsfile"), message);
        assertTrue(message.contains("or its parent; supply the project root"), message);
      }
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
        assertTrue(process.waitFor(5, TimeUnit.SECONDS), "Generator did not terminate");
      }
    }
  }

  private Path fixtureRoot(final String content) throws IOException {
    final var root = temporaryDirectory.resolve("source");
    write(
        root.resolve("pom.xml"),
        """
        <project xmlns="http://maven.apache.org/POM/4.0.0">
          <modelVersion>4.0.0</modelVersion>
          <groupId>org.apache.hop</groupId>
          <artifactId>hop</artifactId>
          <version>1.0-SNAPSHOT</version>
          <packaging>pom</packaging>
          <properties><assembly_appendId>false</assembly_appendId></properties>
          %s
        </project>
        """
            .formatted(content));
    write(root.resolve("mvnw"), "");
    write(root.resolve("Jenkinsfile"), "");
    return root;
  }

  private static void module(
      final Path root, final String directory, final String artifact, final String content)
      throws IOException {
    final var parent = root.resolve(directory).relativize(root).resolve("pom.xml");
    write(
        root.resolve(directory).resolve("pom.xml"),
        """
        <project xmlns="http://maven.apache.org/POM/4.0.0">
          <modelVersion>4.0.0</modelVersion>
          <parent>
            <groupId>org.apache.hop</groupId>
            <artifactId>hop</artifactId>
            <version>1.0-SNAPSHOT</version>
            <relativePath>%s</relativePath>
          </parent>
          <artifactId>%s</artifactId>
          %s
        </project>
        """
            .formatted(parent.toString().replace('\\', '/'), artifact, content));
  }

  private static void write(final Path file, final String content) throws IOException {
    Files.createDirectories(file.getParent());
    Files.writeString(file, content);
  }

  private Document generate(final Path root, final String filename) throws Exception {
    final var output = temporaryDirectory.resolve(filename);
    BomGenerator.main(new String[] {root.toString(), output.toString()});
    return parse(output);
  }

  private static Document parse(final Path file) throws Exception {
    // Use only JDK XML support; this build-only module must not depend on Hop runtime artifacts.
    final var factory = DocumentBuilderFactory.newDefaultInstance();
    factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
    factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
    factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
    try (final var input = Files.newInputStream(file)) {
      return factory.newDocumentBuilder().parse(input);
    }
  }

  private static String text(final Node node, final String expression) throws Exception {
    return XPathFactory.newDefaultInstance().newXPath().evaluate(expression, node).trim();
  }

  private static Set<String> coordinates(final Document model, final String expression)
      throws Exception {
    final var nodes =
        (NodeList)
            XPathFactory.newDefaultInstance()
                .newXPath()
                .evaluate(expression, model, XPathConstants.NODESET);
    final var coordinates = new TreeSet<String>();
    for (int i = 0; i < nodes.getLength(); i++) {
      final var dependency = nodes.item(i);
      assertEquals("0", text(dependency, "count(type)"), "JAR dependencies must omit type");
      final var coordinate = text(dependency, "artifactId") + ":jar";
      assertTrue(coordinates.add(coordinate), "Duplicate dependency: " + coordinate);
      assertEquals("org.apache.hop", text(dependency, "groupId"), coordinate);
      assertEquals("${project.version}", text(dependency, "version"), coordinate);
      assertEquals("", text(dependency, "classifier"), coordinate);
      assertEquals("", text(dependency, "scope"), coordinate);
    }
    return coordinates;
  }
}
