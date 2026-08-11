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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.InputStream;
import org.junit.jupiter.api.Test;

/** URL Test cases for providers that supply structural info. */
public class UrlStructureTests extends AbstractProviderTestCase {

  /** Returns the capabilities required by the tests of this test case. */
  @Override
  protected Capability[] getRequiredCapabilities() {
    return new Capability[] {Capability.GET_TYPE, Capability.URI};
  }

  /** Tests that folders have no content. */
  @Test
  public void testFolderURL() throws Exception {
    final FileObject folder = getReadFolder().resolveFile("dir1");
    if (folder.getFileSystem().hasCapability(Capability.DIRECTORY_READ_CONTENT)) {
      // test might not fail on e.g. HttpFileSystem as there are no directories.
      // A Directory do have a content on http. e.g a generated directory listing or the index.html
      // page.
      return;
    }

    assertTrue(folder.exists());

    // Try getting the content of a folder
    try (InputStream inputStream = folder.getURL().openConnection().getInputStream()) {
      fail();
    } catch (final IOException e) {
      assertSameMessage("vfs.provider/read-not-file.error", folder, e);
    }
  }
}
