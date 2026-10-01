/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.vfs.hdfs.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.Test;

class HdfsMetaBrowseRootTest {

  @Test
  void browseRootLeavesTheDefaultRootToTheProvider() {
    HdfsMeta meta = new HdfsMeta();
    meta.setName("cluster");
    meta.setDefaultRoot("/warehouse/tablespace/managed/hive");
    meta.setBasePath("/webhdfs/v1");
    String root = meta.getBrowseRoot(new Variables());
    assertEquals("cluster://", root);
    assertFalse(root.contains("warehouse"));
    assertFalse(root.contains("webhdfs"));

    Variables variables = new Variables();
    variables.setVariable("CONN", "cluster");
    meta.setName("${CONN}");
    assertEquals("cluster://", meta.getBrowseRoot(variables));

    meta.setName("  ");
    assertNull(meta.getBrowseRoot(new Variables()));
  }
}
