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

package org.apache.hop.pipeline.transforms.execsqlrow;

import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.injection.BaseMetadataInjectionTest;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ExecSqlRowMetaInjectionTest extends BaseMetadataInjectionTest<ExecSqlRowMeta> {

  @Before
  public void setup() throws Exception {
    setup( new ExecSqlRowMeta() );
  }

  @Test
  public void test() throws Exception {
    check( "SQL_FIELD_NAME", () -> meta.getSqlFieldName() );
    check( "COMMIT_SIZE", () -> meta.getCommitSize() );
    check( "READ_SQL_FROM_FILE", () -> meta.isSqlFromfile() );
    check( "SEND_SINGLE_STATEMENT", () -> meta.IsSendOneStatement() );
    check( "UPDATE_STATS", () -> meta.getUpdateField() );
    check( "INSERT_STATS", () -> meta.getInsertField() );
    check( "DELETE_STATS", () -> meta.getDeleteField() );
    check( "READ_STATS", () -> meta.getReadField() );

    skipPropertyTest( "CONNECTION_NAME" );

    DatabaseMeta dbMeta = new DatabaseMeta( "testDBMeta", "Generic", "Native", "localhost", "test", "3306", "user", "password" );
    metadataProvider.getSerializer( DatabaseMeta.class ).save(dbMeta);
    meta.setMetadataProvider( metadataProvider );

    IValueMeta valueMeta = new ValueMetaString( "DBMETA" );
    injector.setProperty( meta, "CONNECTION_NAME", setValue( valueMeta, "testDBMeta" ), "DBMETA" );
    assertEquals( "testDBMeta", meta.getDatabaseMeta().getName() );
  }
}
