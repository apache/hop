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

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.regex.PatternSyntaxException;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import javax.xml.xpath.XPathFactoryConfigurationException;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/** Checks the published BOM independently of the modules enabled in the current reactor. */
class HopBomTest {
  private static final DocumentBuilderFactory DOCUMENT_BUILDER_FACTORY = documentBuilderFactory();
  private static final XPathFactory XPATH_FACTORY = xpathFactory();

  private static final Map<String, String> EXCLUSIONS =
      Map.ofEntries(
          Map.entry("bom/pom.xml", "The BOM cannot manage itself"),
          Map.entry(
              "engine-bench/pom.xml", "Standalone JMH benchmarks, outside the release reactor"),
          Map.entry(
              "web-tests/pom.xml", "Selenium test harness; installation and deployment skipped"),
          Map.entry(
              "tools/spark-client-pack/pom.xml",
              "Standalone Spark pack materializer, not a Hop release artifact"),
          Map.entry("lib-p2/swtbot/pom.xml", "Unpublished local Eclipse wrapper"),
          Map.entry("lib-p2/tm4e/pom.xml", "Unpublished local Eclipse wrapper"),
          Map.entry("assemblies/pom.xml", "Unpublished assembly parent"),
          Map.entry("assemblies/debug/pom.xml", "Unpublished debug assembly"),
          Map.entry("assemblies/plugins/pom.xml", "Unpublished intermediate plugin assembly"),
          Map.entry("assemblies/static/pom.xml", "Unpublished intermediate static assembly"),
          Map.entry("assemblies/web/pom.xml", "Unpublished web assembly"));

  @Test
  void managesEveryProjectArtifact() throws Exception {
    final var bomDirectory =
        Path.of(System.getProperty("basedir", ".")).toAbsolutePath().normalize();
    final var root = bomDirectory.getParent();
    final var version = text(parse(root.resolve("pom.xml")), "/project/version");
    final var sourcePoms = sourcePoms(root);
    final var expected = new TreeMap<String, String>();
    final var unusedExclusions = new TreeSet<>(EXCLUSIONS.keySet());

    for (final var pom : sourcePoms) {
      final var relative = root.relativize(pom).toString().replace('\\', '/');
      if (EXCLUSIONS.containsKey(relative)) {
        unusedExclusions.remove(relative);
        continue;
      }
      final var model = parse(pom);
      final var group = inherited(model, "groupId");
      final var artifact = text(model, "/project/artifactId");
      final var packaging = defaultTo(text(model, "/project/packaging"), "jar");
      assertEquals("org.apache.hop", group, relative + ": unexpected group; review BOM coverage");
      assertEquals(
          version, inherited(model, "version"), relative + ": not aligned to the Hop release");
      assertTrue(
          Set.of("jar", "pom").contains(packaging),
          relative + ": review new packaging " + packaging);
      addExpected(expected, group + ":" + artifact + ":" + packaging, relative);

      // The inherited assembly profile publishes unclassified ZIPs when this descriptor exists.
      final var assembly = pom.getParent().resolve("src/assembly/assembly.xml");
      if (Files.isRegularFile(assembly)) {
        assertEquals(
            "",
            text(model, "/project/properties/assembly_appendId"),
            relative + ": review assembly classifier override");
        final var formats = nodes(parse(assembly), "/assembly/formats/format");
        assertTrue(formats.getLength() > 0, relative + ": assembly has no formats");
        for (int i = 0; i < formats.getLength(); i++) {
          addExpected(
              expected,
              group + ":" + artifact + ":" + formats.item(i).getTextContent().trim(),
              relative);
        }
      }
    }
    assertTrue(unusedExclusions.isEmpty(), "Remove stale BOM exclusions: " + unusedExclusions);
    assertFalse(expected.isEmpty(), "No source POMs found under " + root);
    assertEquals(
        "false",
        text(parse(root.resolve("pom.xml")), "/project/properties/assembly_appendId"),
        "Review BOM ZIP classifiers when the assembly convention changes");

    final var managed =
        nodes(
            parse(bomDirectory.resolve("pom.xml")),
            "/project/dependencyManagement/dependencies/dependency");
    final var actual = new TreeSet<String>();
    for (int i = 0; i < managed.getLength(); i++) {
      final var dependency = managed.item(i);
      final var coordinate =
          text(dependency, "groupId")
              + ":"
              + text(dependency, "artifactId")
              + ":"
              + defaultTo(text(dependency, "type"), "jar");
      assertTrue(actual.add(coordinate), "Duplicate BOM entry: " + coordinate);
      assertEquals(
          "${project.version}",
          text(dependency, "version"),
          coordinate + ": use the BOM release version");
      assertEquals(
          "", text(dependency, "scope"), coordinate + ": do not impose a dependency scope");
      assertEquals("", text(dependency, "classifier"), coordinate + ": unexpected classifier");
    }

    final var missing = new TreeMap<>(expected);
    actual.forEach(missing::remove);
    final var stale = new TreeSet<>(actual);
    stale.removeAll(expected.keySet());
    assertAll(
        () ->
            assertTrue(
                missing.isEmpty(),
                "Add entries to bom/pom.xml (coordinate=source POM): " + missing),
        () ->
            assertTrue(
                stale.isEmpty(),
                "Remove stale or incorrectly typed entries from bom/pom.xml: " + stale));
  }

  private static void addExpected(
      final Map<String, String> expected, final String coordinate, final String pom) {
    final var previous = expected.putIfAbsent(coordinate, pom);
    assertNull(
        previous, "Duplicate project coordinate " + coordinate + " in " + previous + " and " + pom);
  }

  private static Set<Path> sourcePoms(final Path root) throws IOException {
    final var result = new TreeSet<Path>();
    final var ignored = ignoredPaths(root);
    // This build-only module deliberately uses JDK I/O: depending on HopVfs would pull Hop
    // runtime artifacts into the BOM's tests and prevent testing before those artifacts exist.
    Files.walkFileTree(
        root,
        new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult preVisitDirectory(
              final Path directory, final BasicFileAttributes attributes) {
            if (Set.of("target", ".git", "node_modules")
                    .contains(directory.getFileName().toString())
                || ignored.stream().anyMatch(pattern -> pattern.test(root.relativize(directory)))) {
              return FileVisitResult.SKIP_SUBTREE;
            }
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFile(final Path file, final BasicFileAttributes attributes) {
            if (file.getFileName().toString().equals("pom.xml")
                && ignored.stream().noneMatch(pattern -> pattern.test(root.relativize(file)))) {
              result.add(file);
            }
            return FileVisitResult.CONTINUE;
          }
        });
    return result;
  }

  private static List<Predicate<Path>> ignoredPaths(final Path root) {
    final var patterns = new ArrayList<>(optionalLines(root.resolve(".gitignore")));
    final var home = Path.of(System.getProperty("user.home", "."));
    for (final var line : optionalLines(home.resolve(".gitconfig"))) {
      final var entry = line.split("=", 2);
      if (entry.length == 2 && entry[0].trim().equalsIgnoreCase("excludesfile")) {
        var value = entry[1].trim();
        if (value.startsWith("\"") && value.endsWith("\"") && value.length() >= 2) {
          value = value.substring(1, value.length() - 1);
        }
        try {
          final var file =
              value.startsWith("~/") ? home.resolve(value.substring(2)) : home.resolve(value);
          patterns.addAll(optionalLines(file));
        } catch (final InvalidPathException ignored) {
          // Local Git configuration is optional; an unusable path must not break BOM coverage.
        }
      }
    }

    final var result = new ArrayList<Predicate<Path>>();
    for (final var line : patterns) {
      var pattern = line.trim();
      // Best effort only: comments and negations are not exclusion globs.
      if (pattern.isEmpty() || pattern.startsWith("#") || pattern.startsWith("!")) {
        continue;
      }
      final var anchored = pattern.startsWith("/");
      if (anchored) {
        pattern = pattern.substring(1);
      }
      if (pattern.endsWith("/")) {
        pattern = pattern.substring(0, pattern.length() - 1);
      }
      try {
        final var matcher = root.getFileSystem().getPathMatcher("glob:" + pattern);
        final var basenameOnly = !anchored && !pattern.contains("/");
        result.add(path -> matcher.matches(basenameOnly ? path.getFileName() : path));
      } catch (final PatternSyntaxException ignored) {
        // Ignore Git syntax that cannot be represented by a simple filesystem glob.
      }
    }
    return result;
  }

  private static List<String> optionalLines(final Path file) {
    try {
      return Files.isRegularFile(file) ? Files.readAllLines(file) : List.of();
    } catch (final IOException ignored) {
      return List.of();
    }
  }

  private static DocumentBuilderFactory documentBuilderFactory() {
    // Pin the JDK provider instead of consulting system properties or classpath providers.
    final var factory = DocumentBuilderFactory.newDefaultInstance();
    try {
      factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
      factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
      factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
      factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
      factory.setXIncludeAware(false);
      factory.setExpandEntityReferences(false);
      return factory;
    } catch (final ParserConfigurationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static XPathFactory xpathFactory() {
    final var factory = XPathFactory.newDefaultInstance();
    try {
      factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
      return factory;
    } catch (final XPathFactoryConfigurationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static Document parse(final Path pom) throws Exception {
    // Factories are shared, but neither factories nor their builders are thread-safe.
    final DocumentBuilder builder;
    synchronized (DOCUMENT_BUILDER_FACTORY) {
      builder = DOCUMENT_BUILDER_FACTORY.newDocumentBuilder();
    }
    try (final var input = Files.newInputStream(pom)) {
      return builder.parse(input);
    }
  }

  private static String inherited(final Document model, final String field) throws Exception {
    return defaultTo(text(model, "/project/" + field), text(model, "/project/parent/" + field));
  }

  private static String defaultTo(final String value, final String fallback) {
    return value.isEmpty() ? fallback : value;
  }

  private static String text(final Node node, final String expression) throws Exception {
    return xpath().evaluate(expression, node).trim();
  }

  private static NodeList nodes(final Node node, final String expression) throws Exception {
    return (NodeList) xpath().evaluate(expression, node, XPathConstants.NODESET);
  }

  private static XPath xpath() {
    synchronized (XPATH_FACTORY) {
      return XPATH_FACTORY.newXPath();
    }
  }
}
