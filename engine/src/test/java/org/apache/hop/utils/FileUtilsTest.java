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

package org.apache.hop.utils;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Path;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.FileUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileUtilsTest {
  @TempDir Path testFolder;

  @Test
  void testCreateTempDir() {
    String tempDir = testFolder.toAbsolutePath().toString();
    File fl = new File(tempDir);
    assertTrue(fl.exists(), "Dir should be created");
    try {
      fl.delete();
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  @Test
  void testCreateParentFolder() {
    String tempDir = testFolder.toAbsolutePath().toString();
    String suff = tempDir.substring(tempDir.lastIndexOf(File.separator) + 1);
    tempDir += File.separator + suff + File.separator + suff;
    assertTrue(
        FileUtil.createParentFolder(getClass(), tempDir, true, new LogChannel(this)),
        "Dir should be created");
    File fl = new File(tempDir.substring(0, tempDir.lastIndexOf(File.separator)));
    assertTrue(fl.exists(), "Dir should exist");
    fl.delete();
    new File(tempDir).delete();
  }

  @Test
  void testIsFullyQualified() {
    assertTrue(FileUtil.isFullyQualified("/test"));
    assertTrue(FileUtil.isFullyQualified("\\test"));
  }
}
