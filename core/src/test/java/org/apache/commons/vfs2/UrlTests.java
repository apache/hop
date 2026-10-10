/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.commons.vfs2;

import static org.apache.commons.vfs2.VfsTestUtils.assertSameMessage;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import org.junit.jupiter.api.Test;

/** URL test cases for providers. */
public class UrlTests extends AbstractProviderTestCase {

  /**
   * Returns the capabilities required by the tests of this test case. The tests are not run if the
   * provider being tested does not support all the required capabilities. Return null or an empty
   * array to always run the tests.
   */
  @Override
  protected Capability[] getRequiredCapabilities() {
    return new Capability[] {Capability.URI};
  }

  @Test
  public void testReservedCharacterSpace() throws FileSystemException {
    try (FileObject fileObject = getReadFolder().resolveFile("file with spaces.txt")) {
      final URL url = fileObject.getURL();
      final String string = url.toString();
      assertTrue(string.contains("file%20with%20spaces.txt"), string);
    }
    try (FileObject fileObject = getReadFolder().resolveFile("file%20with%20spaces.txt")) {
      final URL url = fileObject.getURL();
      final String string = url.toString();
      assertTrue(string.contains("file%20with%20spaces.txt"), string);
    }
  }

  /** Tests that unknown files have no content. */
  @Test
  public void testUnknownURL() throws Exception {
    // Try getting the content of an unknown file
    final FileObject unknownFile = getReadFolder().resolveFile("unknown-file");
    assertFalse(unknownFile.exists());

    final URLConnection connection = unknownFile.getURL().openConnection();
    try {
      connection.getInputStream();
      fail();
    } catch (final IOException e) {
      assertSameMessage("vfs.provider/read-not-file.error", unknownFile, e);
    }
    assertEquals(-1, connection.getContentLength());
  }

  /** Tests url. */
  @Test
  public void testURL() throws Exception {
    final FileObject file = getReadFolder().resolveFile("some-dir/");
    final URL url = file.getURL();

    assertEquals(file.getName().getURI(), url.toExternalForm());

    final URL parentURL = new URL(url, "..");
    assertEquals(file.getParent().getURL(), parentURL);

    final URL rootURL = new URL(url, "/");
    assertEquals(file.getFileSystem().getRoot().getURL(), rootURL);
  }

  /** Tests content. */
  @Test
  public void testURLContent() throws Exception {
    testURLContent(getReadFolder());
  }

  private void testURLContent(final FileObject readFolder)
      throws FileSystemException, IOException, Exception {
    // Test non-empty file
    FileObject file = readFolder.resolveFile("file1.txt");
    assertTrue(file.exists(), file.toString());

    URLConnection urlCon = file.getURL().openConnection();
    assertSameURLContent(FILE1_CONTENT, urlCon);

    // Test empty file
    file = readFolder.resolveFile("empty.txt");
    assertTrue(file.exists());

    urlCon = file.getURL().openConnection();
    assertSameURLContent("", urlCon);
  }

  /** Tests content. */
  @Test
  public void testURLContentProvider() throws Exception {
    // Test non-empty file
    final FileObject file = getReadFolder().resolveFile("file1.txt");
    assertTrue(file.exists());

    final String uri = file.getURL().toExternalForm();
    final FileSystemOptions options = getReadFolder().getFileSystem().getFileSystemOptions();

    final FileObject f1 = getManager().resolveFile(uri, options);
    final FileObject f2 = getManager().resolveFile(uri, options);

    assertEquals(f1, f2, "Two files resolved by URI must be equals on " + uri);
    assertSame(
        f1.getFileSystem(),
        f2.getFileSystem(),
        "Resolving two times should not produce new filesystem on " + uri);
  }
}
