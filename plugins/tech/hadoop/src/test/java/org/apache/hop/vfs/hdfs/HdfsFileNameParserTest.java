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
package org.apache.hop.vfs.hdfs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.commons.vfs2.FileType;
import org.junit.jupiter.api.Test;

class HdfsFileNameParserTest {

  @Test
  void singleton() {
    assertSame(HdfsFileNameParser.getInstance(), HdfsFileNameParser.getInstance());
  }

  @Test
  void parsesTripleSlashAbsolutePath() throws Exception {
    HdfsFileName name =
        (HdfsFileName)
            HdfsFileNameParser.getInstance()
                .parseUri(null, null, "cdp:///warehouse/db/table/file.parquet");
    assertEquals("cdp", name.getScheme());
    assertEquals("/warehouse/db/table/file.parquet", name.getPath());
    assertEquals(FileType.FILE, name.getType());
  }

  @Test
  void parsesFolderTrailingSlash() throws Exception {
    HdfsFileName name =
        (HdfsFileName)
            HdfsFileNameParser.getInstance().parseUri(null, null, "cdp:///warehouse/db/");
    assertEquals("/warehouse/db", name.getPath());
    assertEquals(FileType.FOLDER, name.getType());
  }

  @Test
  void parsesRoot() throws Exception {
    HdfsFileName name =
        (HdfsFileName) HdfsFileNameParser.getInstance().parseUri(null, null, "cdp:///");
    assertEquals("/", name.getPath());
    assertEquals(FileType.FOLDER, name.getType());
  }

  @Test
  void uriRoundTripUsesTwoSlashesAfterScheme() throws Exception {
    HdfsFileName name =
        (HdfsFileName)
            HdfsFileNameParser.getInstance().parseUri(null, null, "prod:///user/hop/file.txt");
    assertTrue(name.getURI().startsWith("prod://"));
    assertTrue(name.getURI().contains("/user/hop/file.txt"));
  }
}
