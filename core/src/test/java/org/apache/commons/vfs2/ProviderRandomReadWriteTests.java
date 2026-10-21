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

import org.apache.commons.vfs2.util.RandomAccessMode;
import org.junit.jupiter.api.Test;

/** Random read and write test case for file providers. */
public class ProviderRandomReadWriteTests extends AbstractProviderTestCase {

  private static final String TEST_DATA = "This is a test file.";

  /** Sets up a scratch folder for the test to use. */
  protected FileObject createScratchFolder() throws Exception {
    final FileObject scratchFolder = getWriteFolder();

    // Make sure the test folder is empty
    scratchFolder.delete(Selectors.EXCLUDE_SELF);
    scratchFolder.createFolder();

    return scratchFolder;
  }

  /** Returns the capabilities required by the tests of this test case. */
  @Override
  protected Capability[] getRequiredCapabilities() {
    return new Capability[] {
      Capability.GET_TYPE,
      Capability.CREATE,
      Capability.RANDOM_ACCESS_READ,
      Capability.RANDOM_ACCESS_WRITE
    };
  }

  /** Writes a file. */
  @Test
  public void testRandomWrite() throws Exception {
    try (FileObject file = createScratchFolder().resolveFile("random_write.txt")) {
      file.createFile();
      final RandomAccessContent ra =
          file.getContent().getRandomAccessContent(RandomAccessMode.READWRITE);

      // write first byte
      ra.writeByte(TEST_DATA.charAt(0));

      // start at pos 4
      ra.seek(3);
      ra.writeByte(TEST_DATA.charAt(3));
      ra.writeByte(TEST_DATA.charAt(4));

      // restart at pos 4 (but overwrite with different content)
      ra.seek(3);
      ra.writeByte(TEST_DATA.charAt(7));
      ra.writeByte(TEST_DATA.charAt(8));

      // advance to pos 11
      ra.seek(10);
      ra.writeByte(TEST_DATA.charAt(10));
      ra.writeByte(TEST_DATA.charAt(11));

      // now read
      ra.seek(0);
      assertEquals(ra.readByte(), TEST_DATA.charAt(0));

      ra.seek(3);
      assertEquals(ra.readByte(), TEST_DATA.charAt(7));
      assertEquals(ra.readByte(), TEST_DATA.charAt(8));

      ra.seek(10);
      assertEquals(ra.readByte(), TEST_DATA.charAt(10));
      assertEquals(ra.readByte(), TEST_DATA.charAt(11));
    }
  }
}
