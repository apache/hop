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
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import org.apache.commons.vfs2.util.RandomAccessMode;
import org.junit.jupiter.api.Test;

/** Random set length test cases for file providers. */
public class ProviderRandomSetLengthTests extends AbstractProviderTestCase {

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
      Capability.RANDOM_ACCESS_READ,
      Capability.RANDOM_ACCESS_WRITE,
      Capability.RANDOM_ACCESS_SET_LENGTH
    };
  }

  /** Writes a file. */
  @Test
  public void testRandomSetLength() throws Exception {
    try (FileObject file = createScratchFolder().resolveFile("random_write.txt")) {
      file.createFile();
      final String fileString = file.toString();
      final RandomAccessContent ra =
          file.getContent().getRandomAccessContent(RandomAccessMode.READWRITE);

      // Write long string
      ra.writeBytes(TEST_DATA);
      assertEquals(TEST_DATA.length(), ra.length(), fileString);

      // Shrink to length 1
      ra.setLength(1);
      assertEquals(1, ra.length(), fileString);
      // now read 1
      ra.seek(0);
      assertEquals(TEST_DATA.charAt(0), ra.readByte(), fileString);

      try {
        ra.readByte();
        fail("Expected " + Exception.class.getName());
      } catch (final IOException e) {
        // Expected
      }

      // Grow to length 2
      ra.setLength(2);
      assertEquals(2, ra.length(), fileString);
      // We have an undefined extra byte
      ra.seek(1);
      ra.readByte();
    }
  }
}
