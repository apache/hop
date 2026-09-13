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

package org.apache.hop.ui.hopgui.perspective.explorer.web;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.OutputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.vfs.HopVfs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ExplorerFileServingTest {

  private String ramRoot;

  @BeforeEach
  void setUp() throws Exception {
    ramRoot = "ram:///" + UUID.randomUUID();
    FileObject root = HopVfs.getFileObject(ramRoot);
    root.createFolder();
    write(ramRoot + "/docs/index.html", "<html></html>");
    write(ramRoot + "/docs/assets/css/site.css", "body{}");
    write(ramRoot + "/workflows/workflows/page.html", "<html></html>");
    write(ramRoot + "/assets/css/x.css", "h1{}");
    write(ramRoot + "/docs/100%25 done.html", "<html></html>");
    write(ramRoot + "/docs/a%2520b.html", "<html></html>");
    write(ramRoot + "/report:2026-09-11.html", "<html></html>");
    write(ramRoot + "/secret.hpl", "<pipeline/>");
  }

  @AfterEach
  void tearDown() throws Exception {
    FileObject root = HopVfs.getFileObject(ramRoot);
    if (root.exists()) {
      root.deleteAll();
    }
  }

  @Test
  void relativePathOfDescendant() throws Exception {
    FileObject root = HopVfs.getFileObject(ramRoot);
    FileObject file = HopVfs.getFileObject(ramRoot + "/docs/index.html");
    assertEquals("docs/index.html", ExplorerFileServing.relativePath(root, file).orElseThrow());
  }

  @Test
  void relativePathFromFileObjectDecodesPercentNames() throws Exception {
    FileObject root = HopVfs.getFileObject(ramRoot);
    FileObject percentSpace = HopVfs.getFileObject(ramRoot + "/docs/100%25 done.html");
    FileObject percentTwenty = HopVfs.getFileObject(ramRoot + "/docs/a%2520b.html");

    assertEquals(
        "docs/100% done.html", ExplorerFileServing.relativePath(root, percentSpace).orElseThrow());
    assertEquals(
        "docs/a%20b.html", ExplorerFileServing.relativePath(root, percentTwenty).orElseThrow());

    assertRoundTripFromFileObject(root, percentSpace);
    assertRoundTripFromFileObject(root, percentTwenty);
  }

  @Test
  void relativePathRejectsFileOutsideRoot() throws Exception {
    FileObject root = HopVfs.getFileObject(ramRoot + "/docs");
    FileObject file = HopVfs.getFileObject(ramRoot + "/secret.hpl");
    assertTrue(ExplorerFileServing.relativePath(root, file).isEmpty());
  }

  @Test
  void resolveUnderRootAllowsNestedFile() throws Exception {
    FileObject root = HopVfs.getFileObject(ramRoot);
    Optional<FileObject> resolved =
        ExplorerFileServing.resolveUnderRoot(root, "docs/assets/css/site.css");
    assertTrue(resolved.isPresent());
    assertTrue(resolved.get().exists());
  }

  @Test
  void resolveUnderRootAllowsPercentAndColon() throws Exception {
    FileObject root = HopVfs.getFileObject(ramRoot);
    Optional<FileObject> percentFile =
        ExplorerFileServing.resolveUnderRoot(root, "docs/100% done.html");
    assertTrue(percentFile.isPresent());
    assertTrue(percentFile.get().exists());

    Optional<FileObject> colonFile =
        ExplorerFileServing.resolveUnderRoot(root, "report:2026-09-11.html");
    assertTrue(colonFile.isPresent());
    assertTrue(colonFile.get().exists());
  }

  @Test
  void resolveUnderRootRejectsUnknownExtension() throws Exception {
    FileObject root = HopVfs.getFileObject(ramRoot);
    assertTrue(ExplorerFileServing.resolveUnderRoot(root, "secret.hpl").isEmpty());
  }

  @Test
  void resolveUnderRootRejectsFolder() throws Exception {
    FileObject root = HopVfs.getFileObject(ramRoot);
    assertTrue(ExplorerFileServing.resolveUnderRoot(root, "docs").isEmpty());
  }

  @Test
  void sanitizeRejectsTraversal() {
    assertTrue(ExplorerFileServing.sanitizeRelativePath("../etc/passwd").isEmpty());
    assertTrue(ExplorerFileServing.sanitizeRelativePath("docs/../../etc/passwd").isEmpty());
    assertTrue(ExplorerFileServing.sanitizeRelativePath("/etc/passwd").isEmpty());
    assertTrue(ExplorerFileServing.sanitizeRelativePath("file:///etc/passwd").isEmpty());
    assertTrue(ExplorerFileServing.sanitizeRelativePath("").isEmpty());
    assertTrue(ExplorerFileServing.sanitizeRelativePath(".").isEmpty());
    assertTrue(ExplorerFileServing.sanitizeRelativePath("%2e%2e/etc/passwd").isEmpty());
    assertTrue(ExplorerFileServing.sanitizeRelativePath("docs/%2e%2e/%2e%2e/etc/passwd").isEmpty());
    assertTrue(ExplorerFileServing.sanitizeRelativePath("C:/Windows/win.ini").isEmpty());
    assertTrue(ExplorerFileServing.sanitizeRelativePath("C:win.ini").isEmpty());
    assertTrue(ExplorerFileServing.sanitizeRelativePath("file:secret.html").isEmpty());
    assertTrue(ExplorerFileServing.sanitizeRelativePath("http://evil.com/x.html").isEmpty());
  }

  @Test
  void sanitizeAcceptsNormalRelativePath() {
    assertEquals(
        "docs/index.html",
        ExplorerFileServing.sanitizeRelativePath("docs/index.html").orElseThrow());
    assertEquals(
        "docs/index.html",
        ExplorerFileServing.sanitizeRelativePath("./docs/index.html").orElseThrow());
    assertEquals(
        "docs/My File.html",
        ExplorerFileServing.sanitizeRelativePath("docs/My File.html").orElseThrow());
    assertEquals(
        "docs/100% done.html",
        ExplorerFileServing.sanitizeRelativePath("docs/100% done.html").orElseThrow());
    assertEquals(
        "docs/a%20b.html",
        ExplorerFileServing.sanitizeRelativePath("docs/a%20b.html").orElseThrow());
    assertEquals(
        "report:2026-09-11.html",
        ExplorerFileServing.sanitizeRelativePath("report:2026-09-11.html").orElseThrow());
    assertEquals(
        "docs/report:2026-09-11.html",
        ExplorerFileServing.sanitizeRelativePath("docs/report:2026-09-11.html").orElseThrow());
  }

  @Test
  void nestedHtmlRelativeCssStaysUnderRoot() {
    Optional<String> resolved =
        ExplorerFileServing.applyRelative(
            "workflows/workflows/page.html", "../../assets/css/x.css");
    assertEquals("assets/css/x.css", resolved.orElseThrow());
  }

  @Test
  void applyRelativeRejectsEscape() {
    assertTrue(
        ExplorerFileServing.applyRelative("docs/index.html", "../../../etc/passwd").isEmpty());
  }

  @Test
  void contentTypes() {
    assertEquals(
        "text/html; charset=UTF-8",
        ExplorerFileServing.contentType("docs/index.html").orElseThrow());
    assertEquals(
        "text/css; charset=UTF-8",
        ExplorerFileServing.contentType("assets/css/site.css").orElseThrow());
    assertEquals(
        "text/javascript; charset=UTF-8", ExplorerFileServing.contentType("app.js").orElseThrow());
    assertEquals("image/png", ExplorerFileServing.contentType("logo.png").orElseThrow());
    assertEquals("application/pdf", ExplorerFileServing.contentType("doc.pdf").orElseThrow());
    assertTrue(ExplorerFileServing.contentType("secret.hpl").isEmpty());
    assertTrue(ExplorerFileServing.contentType("noext").isEmpty());
  }

  @Test
  void allowedExtensions() {
    assertTrue(ExplorerFileServing.isAllowedExtension("a.html"));
    assertTrue(ExplorerFileServing.isAllowedExtension("a.PDF"));
    assertFalse(ExplorerFileServing.isAllowedExtension("a.hpl"));
    assertFalse(ExplorerFileServing.isAllowedExtension("a.hwf"));
    assertFalse(ExplorerFileServing.isAllowedExtension(".htaccess"));
  }

  @Test
  void publicPathIsRelativeAndNeverHttp() {
    String token = UUID.randomUUID().toString();
    String path = ExplorerFileServing.buildPublicPath("", token, "docs/index.html");
    assertEquals("/explorer-file/" + token + "/docs/index.html", path);
    assertTrue(path.startsWith("/"));
    assertFalse(path.startsWith("http"));

    String withContext = ExplorerFileServing.buildPublicPath("/hop", token, "docs/index.html");
    assertEquals("/hop/explorer-file/" + token + "/docs/index.html", withContext);
    assertFalse(withContext.startsWith("http"));

    String encoded = ExplorerFileServing.buildPublicPath("/", token, "docs/My File.html");
    assertEquals("/explorer-file/" + token + "/docs/My%20File.html", encoded);
  }

  @Test
  void parsePathInfo() {
    String token = UUID.randomUUID().toString();
    ExplorerFileServing.PathInfo info =
        ExplorerFileServing.parsePathInfo("/" + token + "/docs/index.html").orElseThrow();
    assertEquals(token, info.token());
    assertEquals("docs/index.html", info.relativePath());

    assertTrue(ExplorerFileServing.parsePathInfo("/" + token).isEmpty());
    assertTrue(ExplorerFileServing.parsePathInfo("/not-a-uuid/docs/index.html").isEmpty());
    assertTrue(ExplorerFileServing.parsePathInfo("/" + token + "/../secret.hpl").isEmpty());
  }

  @Test
  void buildPublicPathRequiresArgs() {
    assertThrows(
        IllegalArgumentException.class,
        () -> ExplorerFileServing.buildPublicPath("", "", "docs/a.html"));
  }

  /**
   * Produce a public URL from a real {@link FileObject}, simulate the servlet container decoding
   * path-info once, and resolve back to the same file.
   */
  private static void assertRoundTripFromFileObject(FileObject root, FileObject file)
      throws Exception {
    String relative = ExplorerFileServing.relativePath(root, file).orElseThrow();
    String token = UUID.randomUUID().toString();
    String publicPath = ExplorerFileServing.buildPublicPath("", token, relative);
    String pathInfo =
        URLDecoder.decode(publicPath.substring("/explorer-file".length()), StandardCharsets.UTF_8);
    ExplorerFileServing.PathInfo parsed = ExplorerFileServing.parsePathInfo(pathInfo).orElseThrow();
    Optional<FileObject> resolved =
        ExplorerFileServing.resolveUnderRoot(root, parsed.relativePath());
    assertTrue(resolved.isPresent());
    assertTrue(resolved.get().exists());
    assertEquals(file.getName().getURI(), resolved.get().getName().getURI());
  }

  private static void write(String path, String content) throws Exception {
    FileObject file = HopVfs.getFileObject(path);
    file.getParent().createFolder();
    try (OutputStream out = file.getContent().getOutputStream()) {
      out.write(content.getBytes(StandardCharsets.UTF_8));
    }
  }
}
