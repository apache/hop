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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

class HopBomTest {
  // These modules produce JARs but are build/test tools, not consumer libraries.
  private static final Map<String, String> EXCLUSIONS =
      Map.of(
          "hop-config-doc", "Build-time documentation generator",
          "hop-maven-plugin", "Maven build plugin",
          "hop-web-integration-tests", "Browser integration tests");

  // Packagings that produce a JAR; anything other than these and "pom" needs a review.
  private static final Set<String> JAR_PACKAGINGS = Set.of("", "jar", "maven-plugin");

  @Test
  void generatedBomIsStandalone() throws Exception {
    final var bom = parse(Path.of("target/bom-pom.xml"));
    assertEquals("org.apache.hop", text(bom, "/project/groupId"));
    assertEquals("hop-bom", text(bom, "/project/artifactId"));
    assertEquals("pom", text(bom, "/project/packaging"));
    for (final var element :
        Set.of(
            "parent", "dependencies", "build", "profiles", "repositories", "pluginRepositories")) {
      assertEquals(0, nodes(bom, "/project/" + element).getLength(), element);
    }
    assertFalse(text(bom, "/project/name").isBlank());
    assertFalse(text(bom, "/project/description").isBlank());
    assertFalse(text(bom, "/project/url").isBlank());
    assertEquals(
        "https://www.apache.org/licenses/LICENSE-2.0.txt",
        text(bom, "/project/licenses/license/url"));
    assertFalse(text(bom, "/project/scm/connection").isBlank());
    assertFalse(text(bom, "/project/developers/developer/name").isBlank());
  }

  @Test
  void generatedBomCoversLibraryModulesOnly() throws Exception {
    final var root = Path.of("..").toAbsolutePath().normalize();
    final var expected = new TreeSet<String>();
    final var excluded = new TreeSet<String>();
    collectLibraries(root.resolve("pom.xml"), new HashSet<>(), expected, excluded);
    assertEquals(
        EXCLUSIONS.keySet(),
        excluded,
        "Remove stale exclusions or review excluded module packaging");

    final var bom = parse(Path.of("target/bom-pom.xml"));
    final var version = text(parse(root.resolve("pom.xml")), "/project/version");
    assertEquals(version, text(bom, "/project/version"));
    final var dependencies = nodes(bom, "/project/dependencyManagement/dependencies/dependency");
    final var actual = new TreeSet<String>();
    for (int i = 0; i < dependencies.getLength(); i++) {
      final var dependency = dependencies.item(i);
      final var artifact = text(dependency, "artifactId");
      assertEquals("org.apache.hop", text(dependency, "groupId"), artifact);
      assertTrue(actual.add(artifact), "Duplicate BOM entry: " + artifact);
      assertEquals(version, text(dependency, "version"), artifact);
      assertTrue(Set.of("", "jar").contains(text(dependency, "type")), artifact);
      assertEquals("", text(dependency, "classifier"), artifact);
      assertEquals("", text(dependency, "scope"), artifact);
    }
    assertFalse(expected.isEmpty());
    assertEquals(
        expected,
        actual,
        "BOM must cover all library modules and no tooling or third-party artifacts");
  }

  @Test
  void configuredExclusionsAreDocumented() throws Exception {
    final var config = parse(Path.of("pom.xml"));
    final var exclusions =
        nodes(
            config,
            "/project/build/plugins/plugin[artifactId='bom-builder3']/configuration/dependencyExclusions/dependencyExclusion");
    final var actual = new TreeSet<String>();
    for (int i = 0; i < exclusions.getLength(); i++) {
      final var exclusion = exclusions.item(i);
      assertEquals("org.apache.hop", text(exclusion, "groupId"));
      assertTrue(actual.add(text(exclusion, "artifactId")), "Duplicate exclusion");
    }
    assertEquals(EXCLUSIONS.keySet(), actual);
  }

  @Test
  void moduleTraversalIgnoresNestedWorktrees(final @TempDir Path directory) throws Exception {
    Files.writeString(
        directory.resolve("pom.xml"),
        """
        <project>
          <artifactId>root</artifactId>
          <packaging>pom</packaging>
          <modules><module>library</module></modules>
          <profiles><profile><id>optional</id>
            <modules><module>library</module><module>profile-library</module></modules>
          </profile></profiles>
        </project>
        """);
    for (final var module : Set.of("library", "profile-library")) {
      Files.createDirectories(directory.resolve(module));
      Files.writeString(
          directory.resolve(module).resolve("pom.xml"),
          "<project><artifactId>" + module + "</artifactId></project>");
    }
    final var worktree = directory.resolve(".claude/worktrees/demo/library");
    Files.createDirectories(worktree);
    Files.writeString(
        worktree.resolve("pom.xml"), "<project><artifactId>library</artifactId></project>");

    final var libraries = new TreeSet<String>();
    final var excluded = new TreeSet<String>();
    collectLibraries(directory.resolve("pom.xml"), new HashSet<>(), libraries, excluded);
    assertEquals(Set.of("library", "profile-library"), libraries);
    assertTrue(excluded.isEmpty());
  }

  private static void collectLibraries(
      final Path pom,
      final Set<Path> visited,
      final Set<String> libraries,
      final Set<String> excluded)
      throws Exception {
    final var normalized = pom.toAbsolutePath().normalize();
    if (!visited.add(normalized)) {
      return;
    }
    final var model = parse(normalized);
    final var packaging = text(model, "/project/packaging");
    if (JAR_PACKAGINGS.contains(packaging)) {
      final var artifact = text(model, "/project/artifactId");
      if (EXCLUSIONS.containsKey(artifact)) {
        excluded.add(artifact);
      } else {
        assertTrue(libraries.add(artifact), "Duplicate library: " + artifact);
      }
    } else {
      assertEquals("pom", packaging, "Review BOM handling of a new artifact type: " + normalized);
    }
    // Follow declared modules, including profiles, never unrelated POMs in nested worktrees.
    final var modules =
        nodes(model, "/project/modules/module | /project/profiles/profile/modules/module");
    for (int i = 0; i < modules.getLength(); i++) {
      collectLibraries(
          normalized
              .getParent()
              .resolve(modules.item(i).getTextContent().trim())
              .resolve("pom.xml"),
          visited,
          libraries,
          excluded);
    }
  }

  private static Node parse(final Path path) throws Exception {
    // Build-only tests cannot depend on HopVfs before Hop runtime artifacts have been built.
    final var factory = DocumentBuilderFactory.newInstance();
    factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
    factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
    factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
    return factory.newDocumentBuilder().parse(path.toFile());
  }

  private static String text(final Node node, final String expression) throws Exception {
    return XPathFactory.newInstance().newXPath().evaluate(expression, node).trim();
  }

  private static NodeList nodes(final Node node, final String expression) throws Exception {
    return (NodeList)
        XPathFactory.newInstance().newXPath().evaluate(expression, node, XPathConstants.NODESET);
  }
}
