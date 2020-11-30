/*! ******************************************************************************
 *
 * Pentaho Data Integration
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

package org.apache.hop.pipeline.transforms.cassandraoutput;

import org.apache.hop.core.injection.BaseMetadataInjectionTest;
import org.apache.hop.pipeline.transforms.cassandraoutput.CassandraOutputMeta;
import org.junit.Before;
import org.junit.Test;

public class CassandraOutputMetaInjectionTest extends BaseMetadataInjectionTest<CassandraOutputMeta> {

  @Before
  public void setup() throws Exception {
    setup( new CassandraOutputMeta() );
  }

  @Test
  public void test() throws Exception {
    check( "CASSANDRA_HOST", new IStringGetter() {
      public String get() {
        return meta.getCassandraHost();
      }
    } );
    check( "CASSANDRA_PORT", new IStringGetter() {
      public String get() {
        return meta.getCassandraPort();
      }
    } );
    check( "USER_NAME", new IStringGetter() {
      public String get() {
        return meta.getUsername();
      }
    } );
    check( "PASSWORD", new IStringGetter() {
      public String get() {
        return meta.getPassword();
      }
    } );
    check( "CASSANDRA_KEYSPACE", new IStringGetter() {
      public String get() {
        return meta.getCassandraKeyspace();
      }
    } );
    check( "SCHEMA_HOST", new IStringGetter() {
      public String get() {
        return meta.getSchemaHost();
      }
    } );
    check( "SCHEMA_PORT", new IStringGetter() {
      public String get() {
        return meta.getSchemaPort();
      }
    } );
    check( "TABLE", new IStringGetter() {
      public String get() {
        return meta.getTableName();
      }
    } );
    check( "CONSISTENCY_LEVEL", new IStringGetter() {
      public String get() {
        return meta.getConsistency();
      }
    } );
    check( "BATCH_SIZE", new IStringGetter() {
      public String get() {
        return meta.getBatchSize();
      }
    } );
    check( "USE_UNLOGGED_BATCH", new IBooleanGetter() {
      public boolean get() {
        return meta.getUseUnloggedBatch();
      }
    } );
    check( "USE_QUERY_COMPRESSION", new IBooleanGetter() {
      public boolean get() {
        return meta.getUseCompression();
      }
    } );
    check( "CREATE_TABLE", new IBooleanGetter() {
      public boolean get() {
        return meta.getCreateTable();
      }
    } );
    check( "CREATE_TABLE_WITH_CLAUSE", new IStringGetter() {
      public String get() {
        return meta.getCreateTableWithClause();
      }
    } );
    check( "KEY_FIELD", new IStringGetter() {
      public String get() {
        return meta.getKeyField();
      }
    } );
    check( "BATCH_TIMEOUT", new IStringGetter() {
      public String get() {
        return meta.getCQLBatchInsertTimeout();
      }
    } );
    check( "SUB_BATCH_SIZE", new IStringGetter() {
      public String get() {
        return meta.getCQLSubBatchSize();
      }
    } );
    check( "INSERT_FIELDS_NOT_IN_META", new IBooleanGetter() {
      public boolean get() {
        return meta.getInsertFieldsNotInMeta();
      }
    } );
    check( "UPDATE_CASSANDRA_META", new IBooleanGetter() {
      public boolean get() {
        return meta.getUpdateCassandraMeta();
      }
    } );
    check( "TRUNCATE_TABLE", new IBooleanGetter() {
      public boolean get() {
        return meta.getTruncateTable();
      }
    } );
    check( "APRIORI_CQL", new IStringGetter() {
      public String get() {
        return meta.getAprioriCQL();
      }
    } );
    check( "DONT_COMPLAIN_IF_APRIORI_CQL_FAILS", new IBooleanGetter() {
      public boolean get() {
        return meta.getDontComplainAboutAprioriCQLFailing();
      }
    } );
    check( "SOCKET_TIMEOUT", new IStringGetter() {
      public String get() {
        return meta.getSocketTimeout();
      }
    } );
    check( "TTL", new IStringGetter() {
      public String get() {
        return meta.getTTL();
      }
    } );
    check( "TTL_UNIT", new IStringGetter() {
      public String get() {
        return meta.getTTLUnit();
      }
    } );
  }

}
