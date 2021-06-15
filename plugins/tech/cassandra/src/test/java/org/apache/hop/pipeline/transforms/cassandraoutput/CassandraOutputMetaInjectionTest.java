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
 *
 */
package org.apache.hop.pipeline.transforms.cassandraoutput;

import org.apache.hop.core.injection.BaseMetadataInjectionTest;
import org.junit.Before;
import org.junit.Test;

public class CassandraOutputMetaInjectionTest
    extends BaseMetadataInjectionTest<CassandraOutputMeta> {

  @Before
  public void setup() throws Exception {
    setup(new CassandraOutputMeta());
  }

  @Test
  public void test() throws Exception {
    check("CONNECTION", () -> meta.getConnectionName());
    check("SCHEMA_HOST", () -> meta.getSchemaHost());
    check("SCHEMA_PORT", () -> meta.getSchemaPort());
    check("TABLE", () -> meta.getTableName());
    check("CONSISTENCY_LEVEL", () -> meta.getConsistency());
    check("BATCH_SIZE", () -> meta.getBatchSize());
    check("USE_UNLOGGED_BATCH", () -> meta.isUseUnloggedBatch());
    check("CREATE_TABLE", () -> meta.isCreateTable());
    check("CREATE_TABLE_WITH_CLAUSE", () -> meta.getCreateTableWithClause());
    check("KEY_FIELD", () -> meta.getKeyField());
    check("BATCH_TIMEOUT", () -> meta.getCqlBatchInsertTimeout());
    check("SUB_BATCH_SIZE", () -> meta.getCqlSubBatchSize());
    check("INSERT_FIELDS_NOT_IN_META", () -> meta.isInsertFieldsNotInMeta());
    check("UPDATE_CASSANDRA_META", () -> meta.isUpdateCassandraMeta());
    check("TRUNCATE_TABLE", () -> meta.isTruncateTable());
    check("TTL", () -> meta.getTtl());
    check("TTL_UNIT", () -> meta.getTtlUnit());
  }
}
