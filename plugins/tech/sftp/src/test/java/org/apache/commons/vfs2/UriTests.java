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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import org.apache.commons.lang3.ObjectUtils;
import org.junit.jupiter.api.Test;

/** URI test cases for providers. */
public class UriTests extends AbstractProviderTestCase {

  /**
   * Returns the capabilities required by the tests of this test case. The tests are not run if the
   * provider being tested does not support all the required capabilities. Return null or an empty
   * array to always run the tests.
   */
  @Override
  protected Capability[] getRequiredCapabilities() {
    return new Capability[] {Capability.URI};
  }

  /** Tests resolution of absolute URI. */
  @Test
  public void testAbsoluteURI() throws Exception {
    final FileObject readFolder = getReadFolder();
    // Try fetching base folder again by its URI
    final String uri = readFolder.getName().getURI();
    FileObject file =
        getManager().resolveFile(uri, readFolder.getFileSystem().getFileSystemOptions());
    // assert properties
    assertEquals(readFolder.exists(), file.exists());
    assertEquals(readFolder.getName(), file.getName());
    // Needs JRE file providers
    // assertEquals(readFolder.getPath(), file.getPath());
    assertEquals(readFolder.getPublicURIString(), file.getPublicURIString());
    assertEquals(readFolder.getType(), file.getType());
    assertEquals(readFolder.getURI(), file.getURI());
    assertEquals(readFolder.getURL(), file.getURL());
    assertEquals(readFolder.isAttached(), file.isAttached());
    assertEquals(readFolder.isContentOpen(), file.isContentOpen());
    assertEquals(readFolder.isExecutable(), file.isExecutable());
    assertEquals(readFolder.isFile(), file.isFile());
    assertEquals(readFolder.isFolder(), file.isFolder());
    assertEquals(readFolder.isHidden(), file.isHidden());
    assertEquals(readFolder.isReadable(), file.isReadable());
    assertEquals(readFolder.isSymbolicLink(), file.isSymbolicLink());
    assertEquals(readFolder.isWriteable(), file.isWriteable());
    assertEquals(readFolder.toString(), file.toString());
    assertEquals(
        readFolder.toString(),
        file.toString(),
        String.format(
            "file object %s %s, %s %s",
            readFolder.getClass(),
            ObjectUtils.identityHashCodeHex(readFolder),
            file.getClass(),
            ObjectUtils.identityHashCodeHex(file)));
    // Try fetching the filesystem root by its URI
    final String rootUri = readFolder.getName().getRootURI();
    file = getManager().resolveFile(rootUri, readFolder.getFileSystem().getFileSystemOptions());
    assertEquals(readFolder.getFileSystem().getRoot().toString(), file.toString());
    assertEquals(rootUri, file.getName().getRootURI());
    assertEquals(rootUri, file.getName().getURI());
    assertEquals(FileName.ROOT_PATH, file.getName().getPath());
  }

  @Test
  public void testGetURI() throws Exception {
    final FileObject fileObject = getReadFolder().resolveFile("some-dir/");
    final URI uri = fileObject.getURI();

    // FileName#getURI() returns a String, not a URI.
    assertEquals(fileObject.getName().getURI(), uri.toString());
    assertEquals(URI.create(fileObject.getName().getURI()), uri);

    assertEquals(fileObject.getURL().toString(), fileObject.getURI().toString());
    assertEquals(fileObject.getURL().toURI(), fileObject.getURI());
  }

  @Test
  public void testReservedCharacterSpace() throws FileSystemException {
    try (FileObject fileObject = getReadFolder().resolveFile("file with spaces.txt")) {
      final URI url = fileObject.getURI();
      final String string = url.toString();
      assertTrue(string.contains("file%20with%20spaces.txt"), string);
    }
    try (FileObject fileObject = getReadFolder().resolveFile("file%20with%20spaces.txt")) {
      final URI url = fileObject.getURI();
      final String string = url.toString();
      assertTrue(string.contains("file%20with%20spaces.txt"), string);
    }
  }

  /** Tests content. */
  @Test
  public void testURIContentProvider() throws Exception {
    // Test non-empty file
    final FileObject fileObject = getReadFolder().resolveFile("file1.txt");
    assertTrue(fileObject.exists());

    final URI uri = fileObject.getURI();
    final String uriStr = uri.toString();
    final FileSystemOptions options = getReadFolder().getFileSystem().getFileSystemOptions();

    final FileObject f1 = getManager().resolveFile(uriStr, options);
    final FileObject f2 = getManager().resolveFile(uriStr, options);

    assertEquals(f1, f2, "Two files resolved by URI must be equals on " + uriStr);
    assertSame(
        f1.getFileSystem(),
        f2.getFileSystem(),
        "Resolving two times should not produce new filesystem on " + uriStr);
  }
}
