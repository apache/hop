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

package org.apache.hop.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.Calendar;
import java.util.Date;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.rules.TemporaryFolder;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class ResultFileTest {

  @BeforeEach
  void before() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void testGetRow() throws HopFileException, FileSystemException {
    File tempDir = new File(new TemporaryFolder().toString());
    FileObject tempFile = HopVfs.createTempFile("prefix", "suffix", tempDir.toString());
    Date timeBeforeFile = Calendar.getInstance().getTime();
    ResultFile resultFile =
        new ResultFile(ResultFile.FILE_TYPE_GENERAL, tempFile, "myOriginParent", "myOrigin");
    Date timeAfterFile = Calendar.getInstance().getTime();

    assertNotNull(resultFile);
    IRowMeta rm = resultFile.getRow().getRowMeta();
    assertEquals(7, rm.getValueMetaList().size());
    assertEquals(IValueMeta.TYPE_STRING, rm.getValueMeta(0).getType());
    assertEquals(IValueMeta.TYPE_STRING, rm.getValueMeta(1).getType());
    assertEquals(IValueMeta.TYPE_STRING, rm.getValueMeta(2).getType());
    assertEquals(IValueMeta.TYPE_STRING, rm.getValueMeta(3).getType());
    assertEquals(IValueMeta.TYPE_STRING, rm.getValueMeta(4).getType());
    assertEquals(IValueMeta.TYPE_STRING, rm.getValueMeta(5).getType());
    assertEquals(IValueMeta.TYPE_DATE, rm.getValueMeta(6).getType());

    assertEquals(ResultFile.FILE_TYPE_GENERAL, resultFile.getType());
    assertEquals("myOrigin", resultFile.getOrigin());
    assertEquals("myOriginParent", resultFile.getOriginParent());
    assertTrue(
        timeBeforeFile.compareTo(resultFile.getTimestamp()) <= 0
            && timeAfterFile.compareTo(resultFile.getTimestamp()) >= 0,
        "ResultFile timestamp is created in the expected window");

    tempFile.delete();
    tempDir.delete();
  }
}
