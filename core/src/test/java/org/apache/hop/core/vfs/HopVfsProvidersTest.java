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
package org.apache.hop.core.vfs;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Coverage check for the VFS providers Hop registers by default in {@link HopVfs}. Smoke-tests
 * every scheme is registered; for providers that don't need a network or credentials, also
 * round-trips a real file to confirm the provider is functional and not just resolvable.
 */
class HopVfsProvidersTest {

  /** Schemes registered unconditionally in {@code HopVfs.createFileSystemManager}. */
  private static final List<String> BUILTIN_SCHEMES =
      List.of(
          "ram",
          "file",
          "res",
          "zip",
          "gz",
          "jar",
          "http",
          "https",
          "ftp",
          "ftps",
          "sftp",
          "war",
          "par",
          "ear",
          "sar",
          "ejb3",
          "tmp",
          "tar",
          "tbz2",
          "tgz",
          "bz2",
          "files-cache");

  @Test
  @DisplayName("All built-in providers are registered")
  void allBuiltinSchemesAreRegistered() {
    DefaultFileSystemManager fsm = HopVfs.getFileSystemManager();
    Set<String> registered = new HashSet<>(Arrays.asList(fsm.getSchemes()));

    for (String scheme : BUILTIN_SCHEMES) {
      assertTrue(
          fsm.hasProvider(scheme),
          "VFS provider missing for scheme '" + scheme + "'. Registered: " + registered);
    }
  }

  @Test
  @DisplayName("file:// round-trip: write, read back, exists, delete")
  void fileProviderRoundTrips(@TempDir Path tmp) throws Exception {
    Path path = tmp.resolve("hello.txt");
    String url = path.toUri().toString();
    writeBytes(url, "file-content".getBytes(StandardCharsets.UTF_8));
    assertEquals("file-content", readString(url));
    assertTrue(HopVfs.fileExists(url));
    try (FileObject obj = HopVfs.getFileObject(url)) {
      obj.delete();
    }
  }

  @Test
  @DisplayName("ram:// round-trip")
  void ramProviderRoundTrips() throws Exception {
    String url = "ram:///" + uniqueName(".txt");
    writeBytes(url, "ram-content".getBytes(StandardCharsets.UTF_8));
    assertEquals("ram-content", readString(url));
  }

  @Test
  @DisplayName("tmp:// round-trip")
  void tmpProviderRoundTrips() throws Exception {
    String url = "tmp:///" + uniqueName(".txt");
    writeBytes(url, "tmp-content".getBytes(StandardCharsets.UTF_8));
    assertEquals("tmp-content", readString(url));
  }

  @Test
  @DisplayName("files-cache:// resolves to the same temp provider as tmp://")
  void filesCacheProviderResolves() throws Exception {
    String url = "files-cache:///" + uniqueName(".txt");
    writeBytes(url, "cache".getBytes(StandardCharsets.UTF_8));
    assertEquals("cache", readString(url));
  }

  @Test
  @DisplayName("res:// reads a classpath resource")
  void resProviderReadsClasspathResource() throws Exception {
    // Use a resource that's guaranteed to be on the test classpath.
    String url = "res:" + getClass().getName().replace('.', '/') + ".class";
    try (FileObject obj = HopVfs.getFileObject(url)) {
      assertTrue(obj.exists(), url + " should exist on the classpath");
      assertTrue(obj.getContent().getSize() > 0);
    }
  }

  @Test
  @DisplayName("zip:// reads entries from a real zip file")
  void zipProviderReadsEntries(@TempDir Path tmp) throws Exception {
    Path archive = tmp.resolve("archive.zip");
    try (java.util.zip.ZipOutputStream out =
        new java.util.zip.ZipOutputStream(Files.newOutputStream(archive))) {
      out.putNextEntry(new java.util.zip.ZipEntry("entry.txt"));
      out.write("zip-entry".getBytes(StandardCharsets.UTF_8));
      out.closeEntry();
    }
    String url = "zip:" + archive.toUri() + "!/entry.txt";
    assertEquals("zip-entry", readString(url));
  }

  @Test
  @DisplayName("jar:// reads entries the same way zip:// does")
  void jarProviderReadsEntries(@TempDir Path tmp) throws Exception {
    Path archive = tmp.resolve("archive.jar");
    try (java.util.zip.ZipOutputStream out =
        new java.util.zip.ZipOutputStream(Files.newOutputStream(archive))) {
      out.putNextEntry(new java.util.zip.ZipEntry("payload.txt"));
      out.write("jar-entry".getBytes(StandardCharsets.UTF_8));
      out.closeEntry();
    }
    String url = "jar:" + archive.toUri() + "!/payload.txt";
    assertEquals("jar-entry", readString(url));
  }

  @Test
  @DisplayName("war/par/ear/sar/ejb3 share the jar provider")
  void jarAliasesShareTheJarProvider(@TempDir Path tmp) throws Exception {
    Path archive = tmp.resolve("archive.zip");
    try (java.util.zip.ZipOutputStream out =
        new java.util.zip.ZipOutputStream(Files.newOutputStream(archive))) {
      out.putNextEntry(new java.util.zip.ZipEntry("inside.txt"));
      out.write("hi".getBytes(StandardCharsets.UTF_8));
      out.closeEntry();
    }
    for (String alias : new String[] {"war", "par", "ear", "sar", "ejb3"}) {
      String url = alias + ":" + archive.toUri() + "!/inside.txt";
      assertEquals("hi", readString(url), alias + " should read the entry");
    }
  }

  @Test
  @DisplayName("gz:// decompresses a gzipped file")
  void gzProviderDecompresses(@TempDir Path tmp) throws Exception {
    Path gz = tmp.resolve("payload.gz");
    byte[] payload = "gzip-payload".getBytes(StandardCharsets.UTF_8);
    try (java.util.zip.GZIPOutputStream out =
        new java.util.zip.GZIPOutputStream(Files.newOutputStream(gz))) {
      out.write(payload);
    }
    String url = "gz:" + gz.toUri();
    try (FileObject obj = HopVfs.getFileObject(url);
        InputStream in = obj.getContent().getInputStream()) {
      assertArrayEquals(payload, in.readAllBytes());
    }
  }

  @Test
  @DisplayName("bz2:// decompresses a bzip2 file")
  void bz2ProviderDecompresses(@TempDir Path tmp) throws Exception {
    Path bz2 = tmp.resolve("payload.bz2");
    byte[] payload = "bz2-payload".getBytes(StandardCharsets.UTF_8);
    try (org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream out =
        new org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream(
            Files.newOutputStream(bz2))) {
      out.write(payload);
    }
    String url = "bz2:" + bz2.toUri();
    try (FileObject obj = HopVfs.getFileObject(url);
        InputStream in = obj.getContent().getInputStream()) {
      assertArrayEquals(payload, in.readAllBytes());
    }
  }

  @Test
  @DisplayName("tar:// reads entries from a tar archive")
  void tarProviderReadsEntries(@TempDir Path tmp) throws Exception {
    Path tar = tmp.resolve("archive.tar");
    try (org.apache.commons.compress.archivers.tar.TarArchiveOutputStream out =
        new org.apache.commons.compress.archivers.tar.TarArchiveOutputStream(
            Files.newOutputStream(tar))) {
      byte[] payload = "tar-entry".getBytes(StandardCharsets.UTF_8);
      org.apache.commons.compress.archivers.tar.TarArchiveEntry entry =
          new org.apache.commons.compress.archivers.tar.TarArchiveEntry("entry.txt");
      entry.setSize(payload.length);
      out.putArchiveEntry(entry);
      out.write(payload);
      out.closeArchiveEntry();
    }
    String url = "tar:" + tar.toUri() + "!/entry.txt";
    assertEquals("tar-entry", readString(url));
  }

  @Test
  @DisplayName(
      "Remaining network providers are registered (round-trip in HopVfsNetworkProvidersTest)")
  void remainingNetworkProvidersAreRegistered() {
    // http/https/ftp/ftps/sftp need real servers; HopVfsNetworkProvidersTest spins up embedded
    // ones for those. This case just checks they're present (the round-trip tests would fail
    // earlier if they weren't).
    DefaultFileSystemManager fsm = HopVfs.getFileSystemManager();
    for (String scheme : new String[] {"http", "https", "ftp", "ftps", "sftp"}) {
      assertTrue(fsm.hasProvider(scheme), scheme + " provider must be registered");
    }
  }

  // --- helpers -----------------------------------------------------------------------------

  private static String uniqueName(String suffix) {
    return "hopvfs-test-" + System.nanoTime() + suffix;
  }

  private static void writeBytes(String url, byte[] bytes) throws Exception {
    try (FileObject obj = HopVfs.getFileObject(url);
        OutputStream out = obj.getContent().getOutputStream()) {
      out.write(bytes);
    }
  }

  private static String readString(String url) throws Exception {
    try (FileObject obj = HopVfs.getFileObject(url);
        InputStream in = obj.getContent().getInputStream()) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }
}
