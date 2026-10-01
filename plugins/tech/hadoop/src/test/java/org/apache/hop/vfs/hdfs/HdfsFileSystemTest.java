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

import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.FileType;
import org.junit.jupiter.api.Test;

class HdfsFileSystemTest {

  @Test
  void defaultRootIsPrepended() {
    HdfsFileName root = new HdfsFileName("cdp", "/", FileType.FOLDER);
    HdfsFileSystem fs = new HdfsFileSystem(root, new FileSystemOptions());
    fs.setDefaultRoot("/user/hop");
    HdfsFileName file = new HdfsFileName("cdp", "/warehouse/t.parquet", FileType.FILE);
    assertEquals("/user/hop/warehouse/t.parquet", fs.toHdfsPath(file));
    assertEquals("/user/hop", fs.toHdfsPath(root));
  }

  @Test
  void emptyDefaultRootKeepsPath() {
    HdfsFileName root = new HdfsFileName("cdp", "/", FileType.FOLDER);
    HdfsFileSystem fs = new HdfsFileSystem(root, new FileSystemOptions());
    HdfsFileName file = new HdfsFileName("cdp", "/it/hello.txt", FileType.FILE);
    assertEquals("/it/hello.txt", fs.toHdfsPath(file));
  }
}
