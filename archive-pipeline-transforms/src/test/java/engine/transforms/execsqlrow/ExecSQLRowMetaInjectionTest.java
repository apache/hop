/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

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
    check( "SQL_FIELD_NAME", new StringGetter() {
      @Override
      public String get() {
        return meta.getSqlFieldName();
      }
    } );
    check( "COMMIT_SIZE", new IntGetter() {
      @Override
      public int get() {
        return meta.getCommitSize();
      }
    } );
    check( "READ_SQL_FROM_FILE", new BooleanGetter() {
      @Override
      public boolean get() {
        return meta.isSqlFromfile();
      }
    } );
    check( "SEND_SINGLE_STATEMENT", new BooleanGetter() {
      @Override
      public boolean get() {
        return meta.IsSendOneStatement();
      }
    } );
    check( "UPDATE_STATS", new StringGetter() {
      @Override
      public String get() {
        return meta.getUpdateField();
      }
    } );
    check( "INSERT_STATS", new StringGetter() {
      @Override
      public String get() {
        return meta.getInsertField();
      }
    } );
    check( "DELETE_STATS", new StringGetter() {
      @Override
      public String get() {
        return meta.getDeleteField();
      }
    } );
    check( "READ_STATS", new StringGetter() {
      @Override
      public String get() {
        return meta.getReadField();
      }
    } );

    skipPropertyTest( "CONNECTION_NAME" );

    DatabaseMeta dbMeta = new DatabaseMeta( "testDBMeta", "MySQL", "Native", "localhost", "test", "3306", "user", "password" );
    DatabaseMeta.createFactory( metadataProvider ).save( dbMeta );
    meta.setMetadataProvider( metadataProvider );

    IValueMeta valueMeta = new ValueMetaString( "DBMETA" );
    injector.setProperty( meta, "CONNECTION_NAME", setValue( valueMeta, "testDBMeta" ), "DBMETA" );
    assertEquals( "testDBMeta", meta.getDatabaseMeta().getName() );
  }
}
