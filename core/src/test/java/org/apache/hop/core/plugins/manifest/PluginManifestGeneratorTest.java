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
package org.apache.hop.core.plugins.manifest;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;
import org.jboss.jandex.Index;
import org.jboss.jandex.IndexWriter;
import org.jboss.jandex.Indexer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PluginManifestGeneratorTest {

  private static final String FOLDER = "plugins/transforms/foo";
  private static final String OWN_JAR = FOLDER + "/hop-transform-foo-1.0.0.jar";

  @TempDir Path tempDir;

  @Test
  void describesEveryFileInAPluginZipAndAddsTheManifest() throws Exception {
    Path zip = tempDir.resolve("hop-transform-foo-1.0.0.zip");
    byte[] ownJar = jar(Map.of("META-INF/jandex.idx", jandexIndex(ManifestTestTransform.class)));
    byte[] privateJar =
        jar(
            Map.of(
                "META-INF/maven/org.acme/priv/pom.properties",
                "groupId=org.acme\nartifactId=priv\nversion=1.2\n"
                    .getBytes(StandardCharsets.UTF_8)));
    byte[] sharedJar = jar(Map.of("x/Shared.class", new byte[] {1, 2, 3}));
    byte[] driverJar = jar(Map.of("x/Driver.class", new byte[] {4, 5}));
    writeZip(
        zip,
        Map.of(
            FOLDER + "/version.xml",
            "<version>1.0.0</version>".getBytes(StandardCharsets.UTF_8),
            FOLDER + "/dependencies.xml",
            "<dependencies><folder>../bar</folder><folder>../foo</folder><libs><folder>../../tech/baz</folder><wildcard>.*</wildcard></libs></dependencies>"
                .getBytes(StandardCharsets.UTF_8),
            OWN_JAR,
            ownJar,
            FOLDER + "/lib/priv-1.2.jar",
            privateJar,
            "lib/core/shared-2.0.jar",
            sharedJar,
            "lib/jdbc/drv-3.0.jar",
            driverJar,
            "config/projects/samples/foo.hpl",
            "<pipeline/>".getBytes(StandardCharsets.UTF_8)));

    // priv is unknown to the dependency list on purpose: it must fall back to pom.properties
    DependencyList dependencies =
        DependencyList.parse(
            String.join(
                "\n",
                "   org.acme:shared:jar:2.0:provided:/m2/shared-2.0.jar",
                "   org.acme:drv:jar:3.0:provided:/m2/drv-3.0.jar",
                "   com.vendor:byo-driver:jar:9.9:provided:/m2/byo-driver-9.9.jar",
                "   org.apache.hop:hop-transform-bar:jar:1.0.0:provided:/m2/hop-transform-bar-1.0.0.jar",
                "   org.apache.hop:hop-core:jar:1.0.0:provided:/m2/hop-core-1.0.0.jar"));
    Path pom = tempDir.resolve("pom.xml");
    Files.writeString(
        pom,
        """
        <project>
          <dependencyManagement><dependencies>
            <dependency><groupId>org.acme</groupId><artifactId>managed-only</artifactId></dependency>
          </dependencies></dependencyManagement>
          <dependencies>
            <dependency><groupId>org.acme</groupId><artifactId>shared</artifactId><scope>provided</scope></dependency>
            <dependency><groupId>org.acme</groupId><artifactId>priv</artifactId></dependency>
            <dependency><groupId>com.vendor</groupId><artifactId>byo-driver</artifactId><scope>provided</scope></dependency>
            <dependency><groupId>org.apache.hop</groupId><artifactId>hop-transform-bar</artifactId><scope>provided</scope></dependency>
            <dependency><groupId>org.apache.hop</groupId><artifactId>hop-core</artifactId><scope>provided</scope></dependency>
            <dependency><groupId>org.acme</groupId><artifactId>only-for-tests</artifactId><scope>test</scope></dependency>
          </dependencies>
        </project>
        """);

    ByteArrayOutputStream warnings = new ByteArrayOutputStream();
    PluginManifestGenerator generator = new PluginManifestGenerator(new PrintStream(warnings));
    PluginManifest manifest =
        generator.generate(zip, "org.apache.hop", "hop-transform-foo", "1.0.0", dependencies, pom);

    assertEquals(PluginManifest.Kind.PLUGIN, manifest.getKind());
    assertEquals(FOLDER, manifest.getFolder());
    assertEquals(FOLDER + "/plugin-manifest.json", manifest.getManifestPath());
    assertEquals(Map.of("ManifestTestPlugin", List.of("TestTransform")), manifest.getPluginIds());
    assertEquals(List.of("org.apache.hop:hop-transform-bar"), manifest.getRequiresArtifacts());
    assertEquals(
        List.of("plugins/tech/baz", "plugins/transforms/bar"), manifest.getRequiresFolders());

    assertEquals(7, manifest.getFiles().size());
    ManifestFile own = file(manifest, OWN_JAR);
    assertEquals(ManifestFile.Location.PLUGIN, own.getLocation());
    assertEquals("org.apache.hop:hop-transform-foo", own.getGa());
    assertEquals("1.0.0", own.getVersion());
    assertEquals(sha256(ownJar), own.getSha256());
    assertEquals(ownJar.length, own.getSize());

    ManifestFile priv = file(manifest, FOLDER + "/lib/priv-1.2.jar");
    assertEquals(ManifestFile.Location.PRIVATE, priv.getLocation());
    assertEquals("org.acme:priv", priv.getGa(), "coordinates from pom.properties");
    assertEquals("1.2", priv.getVersion());
    assertNull(priv.getScope());

    ManifestFile shared = file(manifest, "lib/core/shared-2.0.jar");
    assertEquals(ManifestFile.Location.SHARED, shared.getLocation());
    assertEquals("provided", shared.getScope());
    assertEquals(sha256(sharedJar), shared.getSha256());

    assertEquals(ManifestFile.Location.JDBC, file(manifest, "lib/jdbc/drv-3.0.jar").getLocation());
    assertEquals(
        ManifestFile.Location.OTHER,
        file(manifest, "config/projects/samples/foo.hpl").getLocation());
    assertEquals(
        ManifestFile.Location.PLUGIN, file(manifest, FOLDER + "/version.xml").getLocation());

    assertEquals(1, manifest.getNotShipped().size());
    ManifestFile byo = manifest.getNotShipped().get(0);
    assertEquals("com.vendor:byo-driver", byo.getGa());
    assertEquals("9.9", byo.getVersion());
    assertEquals("provided", byo.getScope());
    assertNull(byo.getPath());

    assertEquals("", warnings.toString(StandardCharsets.UTF_8), "no scope/location mismatch");

    // Round trip through the zip
    generator.addToZip(zip, manifest);
    try (ZipFile zipFile = new ZipFile(zip.toFile())) {
      ZipEntry entry = zipFile.getEntry(FOLDER + "/plugin-manifest.json");
      assertNotNull(entry);
      PluginManifest reread;
      try (InputStream in = zipFile.getInputStream(entry)) {
        reread = PluginManifest.read(in);
      }
      assertEquals(PluginManifest.SCHEMA, reread.getSchema());
      assertEquals(manifest.getPluginIds(), reread.getPluginIds());
      assertEquals(7, reread.getFiles().size());
      assertEquals(own.getSha256(), file(reread, OWN_JAR).getSha256());
      assertNotNull(zipFile.getEntry(OWN_JAR), "existing entries survive the rewrite");
    }
  }

  @Test
  void runAddsTheManifestOnceAndWritesACopy() throws Exception {
    Path zip = tempDir.resolve("hop-transform-foo-1.0.0.zip");
    writeZip(
        zip,
        Map.of(
            FOLDER + "/version.xml",
            "<version>1.0.0</version>".getBytes(StandardCharsets.UTF_8),
            OWN_JAR,
            jar(Map.of("META-INF/jandex.idx", jandexIndex(ManifestTestTransform.class)))));
    Path out = tempDir.resolve("target/plugin-manifest.json");
    PluginManifestGenerator generator = new PluginManifestGenerator(message -> {});

    PluginManifest manifest =
        generator.run(
            zip,
            "org.apache.hop",
            "hop-transform-foo",
            "1.0.0",
            DependencyList.empty(),
            null,
            false,
            out);
    byte[] once = Files.readAllBytes(zip);
    generator.run(
        zip,
        "org.apache.hop",
        "hop-transform-foo",
        "1.0.0",
        DependencyList.empty(),
        null,
        false,
        out);

    assertEquals(2, manifest.getFiles().size(), "the manifest does not list itself");
    assertArrayEquals(once, Files.readAllBytes(zip), "a second run replaces the manifest");
    assertArrayEquals(manifest.toJson(), Files.readAllBytes(out));
    assertFalse(
        new String(manifest.toJson(), StandardCharsets.UTF_8).contains("\r"),
        "\\n line ends on every OS");
  }

  @Test
  void engineZipOwnsSharedJarsOnly() throws Exception {
    Path zip = tempDir.resolve("hop-core-1.0.0.zip");
    writeZip(
        zip,
        Map.of(
            "lib/core/hop-core-1.0.0.jar",
                jar(Map.of("META-INF/jandex.idx", jandexIndex(ManifestTestTransform.class))),
            "lib/core/commons-io-2.0.jar", jar(Map.of("a", new byte[] {1}))));
    DependencyList dependencies =
        DependencyList.parse("   commons-io:commons-io:jar:2.0:compile:/m2/commons-io-2.0.jar");

    PluginManifest manifest =
        new PluginManifestGenerator()
            .generate(zip, "org.apache.hop", "hop-core", "1.0.0", dependencies, null);

    assertEquals(PluginManifest.Kind.ENGINE, manifest.getKind());
    assertNull(manifest.getFolder());
    assertEquals("lib/core/hop-core-manifest.json", manifest.getManifestPath());
    assertEquals(2, manifest.getFiles(ManifestFile.Location.SHARED).size());
    assertEquals("compile", file(manifest, "lib/core/commons-io-2.0.jar").getScope());
    assertEquals(Map.of("ManifestTestPlugin", List.of("TestTransform")), manifest.getPluginIds());
  }

  @Test
  void baseOfAClientZipIsTheClientsOwnLibrariesOnly() throws Exception {
    Path zip = tempDir.resolve("hop-client-1.0.0.zip");
    writeZip(
        zip,
        Map.of(
            "hop/hop-run.sh", new byte[] {1},
            "hop/lib/core/commons-io-2.0.jar", jar(Map.of("a", new byte[] {1})),
            "hop/lib/core/lettuce-core-7.0.jar", jar(Map.of("b", new byte[] {2})),
            "hop/lib/swt/linux/x86_64/swt.jar", jar(Map.of("c", new byte[] {3})),
            "hop/plugins/transforms/foo/version.xml", new byte[] {4},
            "hop/plugins/transforms/foo/hop-transform-foo-1.0.0.jar",
                jar(Map.of("d", new byte[] {5}))));
    // The client's own runtime dependencies: lettuce-core is not one, a bundled plugin ships it
    DependencyList dependencies =
        DependencyList.parse("   commons-io:commons-io:jar:2.0:compile:/m2/commons-io-2.0.jar");

    PluginManifestGenerator generator = new PluginManifestGenerator();
    PluginManifest manifest =
        generator.generateBase(zip, "org.apache.hop", "hop-client", "1.0.0", dependencies);

    assertEquals(PluginManifest.Kind.ENGINE, manifest.getKind());
    assertEquals("lib/core/hop-client-manifest.json", manifest.getManifestPath());
    assertEquals(
        List.of("lib/core/commons-io-2.0.jar", "lib/swt/linux/x86_64/swt.jar"),
        manifest.getFiles().stream().map(ManifestFile::getPath).toList());
    assertEquals("commons-io", file(manifest, "lib/core/commons-io-2.0.jar").getArtifactId());
    assertEquals(
        ManifestFile.Location.SHARED, file(manifest, "lib/core/commons-io-2.0.jar").getLocation());

    // The manifest goes next to the other lib/core files, below the zip's top folder
    assertEquals("hop/", PluginManifestGenerator.topFolder(zip));
    generator.addToZip(zip, manifest, PluginManifestGenerator.topFolder(zip));
    try (ZipFile zipFile = new ZipFile(zip.toFile())) {
      assertNotNull(zipFile.getEntry("hop/lib/core/hop-client-manifest.json"));
    }
  }

  @Test
  void refusesAggregateZips() throws Exception {
    Path zip = tempDir.resolve("aggregate.zip");
    writeZip(
        zip,
        Map.of(
            "plugins/a/x/version.xml", new byte[] {1},
            "plugins/a/y/version.xml", new byte[] {1}));
    IOException e =
        assertThrows(
            IOException.class,
            () ->
                new PluginManifestGenerator()
                    .generate(zip, "g", "a", "1", DependencyList.empty(), null));
    assertTrue(e.getMessage().contains("2 plugin folders"));
  }

  @Test
  void warnsWhenScopeAndLocationDisagree() throws Exception {
    Path zip = tempDir.resolve("hop-transform-foo-1.0.0.zip");
    writeZip(
        zip,
        Map.of(
            FOLDER + "/version.xml",
            new byte[] {1},
            FOLDER + "/lib/misrouted-1.0.jar",
            jar(Map.of("a", new byte[] {1}))));
    DependencyList dependencies =
        DependencyList.parse("   org.acme:misrouted:jar:1.0:provided:/m2/misrouted-1.0.jar");
    ByteArrayOutputStream warnings = new ByteArrayOutputStream();

    new PluginManifestGenerator(new PrintStream(warnings))
        .generate(zip, "org.apache.hop", "hop-transform-foo", "1.0.0", dependencies, null);

    String text = warnings.toString(StandardCharsets.UTF_8);
    assertTrue(
        text.contains(
            "1 x provided dependency shipped as private: " + FOLDER + "/lib/misrouted-1.0.jar"),
        text);
    assertTrue(text.contains("no jar named hop-transform-foo-1.0.0.jar"), text);
  }

  @Test
  void resolvesDependencyFolders() {
    assertEquals(
        "plugins/databases/postgresql",
        PluginManifestGenerator.resolveRelative("plugins/databases/cratedb", "../postgresql"));
    assertEquals(
        "plugins/tech/avro",
        PluginManifestGenerator.resolveRelative("plugins/transforms/kafka", "../../tech/avro"));
    assertEquals(
        "plugins/transforms/kafka/extra",
        PluginManifestGenerator.resolveRelative("plugins/transforms/kafka", "./extra/"));
  }

  // --- helpers ---

  private static ManifestFile file(PluginManifest manifest, String path) {
    for (ManifestFile file : manifest.getFiles()) {
      if (path.equals(file.getPath())) {
        return file;
      }
    }
    throw new AssertionError("no manifest entry for " + path);
  }

  private static void writeZip(Path zip, Map<String, byte[]> entries) throws IOException {
    try (ZipOutputStream out = new ZipOutputStream(Files.newOutputStream(zip))) {
      for (Map.Entry<String, byte[]> entry : entries.entrySet()) {
        out.putNextEntry(new ZipEntry(entry.getKey()));
        out.write(entry.getValue());
        out.closeEntry();
      }
    }
  }

  private static byte[] jar(Map<String, byte[]> entries) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ZipOutputStream out = new ZipOutputStream(bytes)) {
      for (Map.Entry<String, byte[]> entry : entries.entrySet()) {
        out.putNextEntry(new ZipEntry(entry.getKey()));
        out.write(entry.getValue());
        out.closeEntry();
      }
    }
    return bytes.toByteArray();
  }

  private static byte[] jandexIndex(Class<?> clazz) throws IOException {
    Indexer indexer = new Indexer();
    String resource = clazz.getName().replace('.', '/') + ".class";
    try (InputStream in = clazz.getClassLoader().getResourceAsStream(resource)) {
      assertNotNull(in, resource);
      indexer.index(in);
    }
    Index index = indexer.complete();
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    new IndexWriter(bytes).write(index);
    return bytes.toByteArray();
  }

  private static String sha256(byte[] bytes) throws Exception {
    return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
  }
}
