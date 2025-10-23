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

package org.apache.hop.pipeline.transforms.getfilenames;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Test class for FileItem */
class FileItemTest {

  @Test
  void testDefaultConstructor() {
    FileItem fileItem = new FileItem();

    assertNotNull(fileItem);
    assertNull(fileItem.getFileName());
    assertNull(fileItem.getFileMask());
    assertNull(fileItem.getExcludeFileMask());
    assertEquals("N", fileItem.getFileRequired());
    assertEquals("N", fileItem.getIncludeSubFolders());
  }

  @Test
  void stParameterizedConstructor() {
    String fileName = "test.txt";
    String fileMask = "*.txt";
    String excludeFileMask = "*.tmp";
    String fileRequired = "Y";
    String includeSubFolders = "Y";

    FileItem fileItem =
        new FileItem(fileName, fileMask, excludeFileMask, fileRequired, includeSubFolders);

    assertEquals(fileName, fileItem.getFileName());
    assertEquals(fileMask, fileItem.getFileMask());
    assertEquals(excludeFileMask, fileItem.getExcludeFileMask());
    assertEquals(fileRequired, fileItem.getFileRequired());
    assertEquals(includeSubFolders, fileItem.getIncludeSubFolders());
  }

  @Test
  void testParameterizedConstructorWithEmptyValues() {
    String fileName = "test.txt";
    String fileMask = "*.txt";
    String excludeFileMask = "*.tmp";

    FileItem fileItem = new FileItem(fileName, fileMask, excludeFileMask, "", "");

    assertEquals(fileName, fileItem.getFileName());
    assertEquals(fileMask, fileItem.getFileMask());
    assertEquals(excludeFileMask, fileItem.getExcludeFileMask());
    assertEquals("N", fileItem.getFileRequired());
    assertEquals("N", fileItem.getIncludeSubFolders());
  }

  @Test
  void testParameterizedConstructorWithNullValues() {
    String fileName = "test.txt";
    String fileMask = "*.txt";
    String excludeFileMask = "*.tmp";

    FileItem fileItem = new FileItem(fileName, fileMask, excludeFileMask, null, null);

    assertEquals(fileName, fileItem.getFileName());
    assertEquals(fileMask, fileItem.getFileMask());
    assertEquals(excludeFileMask, fileItem.getExcludeFileMask());
    assertEquals("N", fileItem.getFileRequired());
    assertEquals("N", fileItem.getIncludeSubFolders());
  }

  @Test
  void testSettersAndGetters() {
    FileItem fileItem = new FileItem();

    fileItem.setFileName("newfile.txt");
    fileItem.setFileMask("*.txt");
    fileItem.setExcludeFileMask("*.tmp");
    fileItem.setFileRequired("Y");
    fileItem.setIncludeSubFolders("Y");

    assertEquals("newfile.txt", fileItem.getFileName());
    assertEquals("*.txt", fileItem.getFileMask());
    assertEquals("*.tmp", fileItem.getExcludeFileMask());
    assertEquals("Y", fileItem.getFileRequired());
    assertEquals("Y", fileItem.getIncludeSubFolders());
  }

  @Test
  void testEquals() {
    FileItem fileItem1 = new FileItem("test.txt", "*.txt", "*.tmp", "Y", "N");
    FileItem fileItem2 = new FileItem("test.txt", "*.txt", "*.tmp", "Y", "N");
    FileItem fileItem3 = new FileItem("test.txt", "*.txt", "*.tmp", "N", "Y");
    FileItem fileItem4 = new FileItem("other.txt", "*.txt", "*.tmp", "Y", "N");

    assertEquals(fileItem1, fileItem2);
    assertEquals(fileItem1, fileItem3); // equals only considers fileName, fileMask, excludeFileMask
    assertNotEquals(fileItem1, fileItem4);
    assertNotEquals(null, fileItem1);
    assertNotEquals("not a FileItem", fileItem1);
  }

  @Test
  void testEqualsWithNullValues() {
    FileItem fileItem1 = new FileItem("test.txt", null, null, "Y", "N");
    FileItem fileItem2 = new FileItem("test.txt", null, null, "Y", "N");
    FileItem fileItem3 = new FileItem("test.txt", "*.txt", null, "Y", "N");

    assertEquals(fileItem1, fileItem2);
    assertNotEquals(fileItem1, fileItem3);
  }

  @Test
  void testHashCode() {
    FileItem fileItem1 = new FileItem("test.txt", "*.txt", "*.tmp", "Y", "N");
    FileItem fileItem2 = new FileItem("test.txt", "*.txt", "*.tmp", "Y", "N");
    FileItem fileItem3 = new FileItem("test.txt", "*.txt", "*.tmp", "N", "Y");
    FileItem fileItem4 = new FileItem("other.txt", "*.txt", "*.tmp", "Y", "N");

    assertEquals(fileItem1.hashCode(), fileItem2.hashCode());
    assertEquals(
        fileItem1.hashCode(),
        fileItem3.hashCode()); // hashCode only considers fileName, fileMask, excludeFileMask
    assertNotEquals(fileItem1.hashCode(), fileItem4.hashCode());
  }

  @Test
  void testHashCodeWithNullValues() {
    FileItem fileItem1 = new FileItem("test.txt", null, null, "Y", "N");
    FileItem fileItem2 = new FileItem("test.txt", null, null, "Y", "N");

    assertEquals(fileItem1.hashCode(), fileItem2.hashCode());
  }

  @Test
  void testSelfEquality() {
    FileItem fileItem = new FileItem("test.txt", "*.txt", "*.tmp", "Y", "N");
    assertEquals(fileItem, fileItem);
  }

  @Test
  void testToString() {
    FileItem fileItem = new FileItem("test.txt", "*.txt", "*.tmp", "Y", "N");
    String result = fileItem.toString();
    assertNotNull(result);
    assertTrue(result.contains("FileItem"));
  }
}
