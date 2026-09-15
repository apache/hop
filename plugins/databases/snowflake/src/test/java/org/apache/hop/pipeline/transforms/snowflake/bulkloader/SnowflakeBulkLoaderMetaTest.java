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

package org.apache.hop.pipeline.transforms.snowflake.bulkloader;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class SnowflakeBulkLoaderMetaTest {

  @BeforeAll
  static void initHop() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void testDefaultTruncateFlagsAreOff() {
    SnowflakeBulkLoaderMeta meta = new SnowflakeBulkLoaderMeta();
    meta.setDefault();

    assertFalse(meta.isTruncateTable());
    assertFalse(meta.isOnlyWhenHaveRows());
  }

  @Test
  void testTruncateTableAccessors() {
    SnowflakeBulkLoaderMeta meta = new SnowflakeBulkLoaderMeta();

    meta.setTruncateTable(true);
    assertTrue(meta.isTruncateTable());

    meta.setTruncateTable(false);
    assertFalse(meta.isTruncateTable());
  }

  @Test
  void testOnlyWhenHaveRowsAccessors() {
    SnowflakeBulkLoaderMeta meta = new SnowflakeBulkLoaderMeta();

    meta.setOnlyWhenHaveRows(true);
    assertTrue(meta.isOnlyWhenHaveRows());

    meta.setOnlyWhenHaveRows(false);
    assertFalse(meta.isOnlyWhenHaveRows());
  }

  @Test
  void copyStatementDeclaresHexBinaryFormat() throws Exception {
    SnowflakeBulkLoaderMeta meta = new SnowflakeBulkLoaderMeta();
    meta.setDefault();
    meta.setTargetTable("orders");

    String sql = meta.getCopyStatement(new Variables(), List.of("/tmp/orders.csv"));

    assertTrue(sql.contains("BINARY_FORMAT = 'HEX'"), sql);
  }
}
