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

/** Random read-only test case for file providers. */
public class ProviderRandomReadTests extends AbstractProviderTestCase {

  private static final String TEST_DATA = "This is a test file.";

  /** Returns the capabilities required by the tests of this test case. */
  @Override
  protected Capability[] getRequiredCapabilities() {
    return new Capability[] {Capability.GET_TYPE, Capability.RANDOM_ACCESS_READ};
  }

  /** Read a file. */
  @Test
  public void testRandomRead() throws Exception {
    try (FileObject file = getReadFolder().resolveFile("file1.txt")) {
      final RandomAccessContent ra =
          file.getContent().getRandomAccessContent(RandomAccessMode.READ);

      // read first byte
      byte c = ra.readByte();
      assertEquals(TEST_DATA.charAt(0), c);
      assertEquals(1, ra.getFilePointer(), "fp");

      // start at pos 4
      ra.seek(3);
      c = ra.readByte();
      assertEquals(TEST_DATA.charAt(3), c);
      assertEquals(4, ra.getFilePointer(), "fp");

      c = ra.readByte();
      assertEquals(TEST_DATA.charAt(4), c);
      assertEquals(5, ra.getFilePointer(), "fp");

      // restart at pos 4
      ra.seek(3);
      c = ra.readByte();
      assertEquals(TEST_DATA.charAt(3), c);
      assertEquals(4, ra.getFilePointer(), "fp");

      c = ra.readByte();
      assertEquals(TEST_DATA.charAt(4), c);
      assertEquals(5, ra.getFilePointer(), "fp");

      // advance to pos 11
      ra.seek(10);
      c = ra.readByte();
      assertEquals(TEST_DATA.charAt(10), c);
      assertEquals(11, ra.getFilePointer(), "fp");

      c = ra.readByte();
      assertEquals(TEST_DATA.charAt(11), c);
      assertEquals(12, ra.getFilePointer(), "fp");
    }
  }
}
