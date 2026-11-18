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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/** Generates the library BOM from the base reactor, without Hop runtime dependencies. */
public final class BomGenerator {
  private static final Map<String, String> EXCLUSIONS =
      Map.of(
          "bom/pom.xml", "The BOM cannot manage itself",
          "lib-p2/swtbot/pom.xml", "Unpublished local Eclipse wrapper",
          "lib-p2/tm4e/pom.xml", "Unpublished local Eclipse wrapper");

  private final Path root;
  private final String version;
  private final DocumentBuilderFactory documentBuilderFactory;
  private final XPath xpath;
  private final Map<String, Path> artifacts = new TreeMap<>();
  private final Set<Path> visited = new HashSet<>();
  private final Set<Path> visiting = new HashSet<>();
  private final Set<String> usedExclusions = new HashSet<>();

  private BomGenerator(final Path root) throws Exception {
    this.root = root;
    // Deliberately use JDK I/O and XML providers: generating the BOM must not require Hop
    // artifacts.
    documentBuilderFactory = DocumentBuilderFactory.newDefaultInstance();
    documentBuilderFactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
    documentBuilderFactory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
    documentBuilderFactory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
    documentBuilderFactory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
    documentBuilderFactory.setXIncludeAware(false);
    documentBuilderFactory.setExpandEntityReferences(false);
    final var xpathFactory = XPathFactory.newDefaultInstance();
    xpathFactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
    xpath = xpathFactory.newXPath();
    version = text(parse(root.resolve("pom.xml")), "/project/version");
    require(
        !version.isEmpty() && !version.contains("${"),
        "Root POM must declare a literal release version");
  }

  /**
   * Writes a complete BOM. Arguments are the optional project root and output POM, in that order.
   * Without a root, searches the working directory (Maven's basedir when supplied) and one parent.
   */
  public static void main(final String[] args) throws Exception {
    require(args.length <= 2, "Usage: BomGenerator [project-root [output-pom]]");
    final var root =
        args.length > 0
            ? Path.of(args[0]).toAbsolutePath().normalize()
            : findRoot(Path.of(System.getProperty("basedir", ".")).toAbsolutePath().normalize());
    final var output =
        args.length > 1
            ? Path.of(args[1]).toAbsolutePath().normalize()
            : root.resolve("bom/pom.xml");
    final var generator = new BomGenerator(root);
    generator.visit(root.resolve("pom.xml"));
    for (final var exclusion : EXCLUSIONS.keySet()) {
      require(
          exclusion.equals("bom/pom.xml")
              || !Files.exists(root.resolve(exclusion))
              || generator.usedExclusions.contains(exclusion),
          "Remove stale BOM exclusion: " + exclusion);
    }
    final var content = generator.generate();
    Files.createDirectories(output.getParent());
    Files.writeString(output, content);
  }

  private static Path findRoot(final Path start) {
    var candidate = start;
    for (int i = 0; i <= 1 && candidate != null; i++, candidate = candidate.getParent()) {
      if (Files.isRegularFile(candidate.resolve("mvnw"))
          && Files.isRegularFile(candidate.resolve("Jenkinsfile"))) {
        return candidate;
      }
    }
    throw new IllegalArgumentException(
        "Cannot find mvnw and Jenkinsfile in " + start + " or its parent; supply the project root");
  }

  private void visit(final Path pom) throws Exception {
    require(pom.startsWith(root), "Module outside project root: " + pom);
    require(!visiting.contains(pom), "Cyclic module declaration: " + pom);
    if (!visited.add(pom)) {
      return;
    }
    visiting.add(pom);
    final var relative = root.relativize(pom).toString().replace('\\', '/');
    // The generated file is an output, never an input, even when the reactor declares it.
    if (relative.equals("bom/pom.xml")) {
      usedExclusions.add(relative);
      visiting.remove(pom);
      return;
    }
    final var model = parse(pom);
    if (EXCLUSIONS.containsKey(relative)) {
      usedExclusions.add(relative);
    } else {
      addArtifact(pom, model);
    }
    // Base libraries are always managed, independent of the build-time profile activation.
    // Other profiles contain distributions and build/test tooling, not BOM libraries.
    final var modules =
        nodes(
            model, "/project/modules/module | /project/profiles/profile[id='base']/modules/module");
    for (int i = 0; i < modules.getLength(); i++) {
      final var module = modules.item(i).getTextContent().trim();
      require(!module.isEmpty() && !module.contains("${"), pom + ": module must be a literal path");
      final var path = pom.getParent().resolve(module).normalize();
      visit(Files.isRegularFile(path) ? path : path.resolve("pom.xml"));
    }
    visiting.remove(pom);
  }

  private void addArtifact(final Path pom, final Document model) throws Exception {
    final var packaging = defaultTo(text(model, "/project/packaging"), "jar");
    if (!"jar".equals(packaging)) {
      return;
    }
    final var group = inherited(model, "groupId");
    final var artifact = text(model, "/project/artifactId");
    require("org.apache.hop".equals(group), pom + ": unexpected group; review BOM coverage");
    require(!artifact.isEmpty() && !artifact.contains("${"), pom + ": artifact ID must be literal");
    require(version.equals(inherited(model, "version")), pom + ": not aligned to the Hop release");
    final var previous = artifacts.putIfAbsent(artifact, pom);
    require(
        previous == null,
        "Duplicate project coordinate "
            + group
            + ":"
            + artifact
            + " in "
            + previous
            + " and "
            + pom);
  }

  private String generate() {
    final var result =
        new StringBuilder(
            """
        <?xml version="1.0" encoding="UTF-8"?>
        <!--
          ~ Licensed to the Apache Software Foundation (ASF) under one or more
          ~ contributor license agreements.  See the NOTICE file distributed with
          ~ this work for additional information regarding copyright ownership.
          ~ The ASF licenses this file to You under the Apache License, Version 2.0
          ~ (the "License"); you may not use this file except in compliance with
          ~ the License.  You may obtain a copy of the License at
          ~
          ~       http://www.apache.org/licenses/LICENSE-2.0
          ~
          ~ Unless required by applicable law or agreed to in writing, software
          ~ distributed under the License is distributed on an "AS IS" BASIS,
          ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
          ~ See the License for the specific language governing permissions and
          ~ limitations under the License.
          -->
        <project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
            <!-- Generated by org.apache.hop.bom.BomGenerator. Do not edit; regenerate instead. -->
            <modelVersion>4.0.0</modelVersion>
            <parent>
                <groupId>org.apache.hop</groupId>
                <artifactId>hop</artifactId>
        """);
    element(result, 2, "version", version);
    result.append(
        """
            </parent>
            <artifactId>hop-bom</artifactId>
            <packaging>jar</packaging>
            <name>Hop Bill of Materials</name>
            <description>Version alignment for Apache Hop libraries, imported as a POM in dependencyManagement.</description>
            <dependencyManagement>
                <dependencies>
        """);
    for (final var artifact : artifacts.keySet()) {
      result.append("            <dependency>\n");
      element(result, 4, "groupId", "org.apache.hop");
      element(result, 4, "artifactId", artifact);
      element(result, 4, "version", "${project.version}");
      result.append("            </dependency>\n");
    }
    result.append(
        """
                </dependencies>
            </dependencyManagement>
            <build>
                <plugins>
                    <!-- The generator is a build tool, not a Hop runtime plugin. -->
                    <plugin>
                        <groupId>io.smallrye</groupId>
                        <artifactId>jandex-maven-plugin</artifactId>
                        <configuration>
                            <skip>true</skip>
                        </configuration>
                    </plugin>
                    <!-- Publish only this BOM's management, not inherited third-party pins. -->
                    <plugin>
                        <groupId>org.codehaus.mojo</groupId>
                        <artifactId>flatten-maven-plugin</artifactId>
                        <configuration>
                            <flattenMode>bom</flattenMode>
                            <outputDirectory>${project.build.directory}</outputDirectory>
                            <pomElements>
                                <dependencies>remove</dependencies>
                                <profiles>remove</profiles>
                                <build>remove</build>
                                <properties>remove</properties>
                            </pomElements>
                        </configuration>
                        <executions>
                            <execution>
                                <id>flatten</id>
                                <goals>
                                    <goal>flatten</goal>
                                </goals>
                                <phase>process-resources</phase>
                            </execution>
                        </executions>
                    </plugin>
                </plugins>
            </build>
        </project>
        """);
    return result.toString();
  }

  private static void element(
      final StringBuilder result, final int indent, final String name, final String value) {
    result
        .append("    ".repeat(indent))
        .append('<')
        .append(name)
        .append('>')
        .append(value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))
        .append("</")
        .append(name)
        .append(">\n");
  }

  private Document parse(final Path pom) throws Exception {
    try (final var input = Files.newInputStream(pom)) {
      return documentBuilderFactory.newDocumentBuilder().parse(input);
    }
  }

  private String inherited(final Document model, final String field) throws Exception {
    return defaultTo(text(model, "/project/" + field), text(model, "/project/parent/" + field));
  }

  private static String defaultTo(final String value, final String fallback) {
    return value.isEmpty() ? fallback : value;
  }

  private String text(final Node node, final String expression) throws Exception {
    return xpath.evaluate(expression, node).trim();
  }

  private NodeList nodes(final Node node, final String expression) throws Exception {
    return (NodeList) xpath.evaluate(expression, node, XPathConstants.NODESET);
  }

  private static void require(final boolean condition, final String message) {
    if (!condition) {
      throw new IllegalArgumentException(message);
    }
  }
}
