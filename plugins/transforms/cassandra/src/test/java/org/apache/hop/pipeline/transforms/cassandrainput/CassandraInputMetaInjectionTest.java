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

package org.apache.hop.pipeline.transforms.cassandrainput;

import org.apache.hop.core.injection.BaseMetadataInjectionTest;
import org.junit.Before;
import org.junit.Test;

public class CassandraInputMetaInjectionTest extends BaseMetadataInjectionTest<CassandraInputMeta> {

  @Before
  public void setup() throws Exception {
    setup( new CassandraInputMeta() );
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
    check( "USE_QUERY_COMPRESSION", new IBooleanGetter() {
      public boolean get() {
        return meta.getUseCompression();
      }
    } );
    check( "CQL_QUERY", new IStringGetter() {
      public String get() {
        return meta.getCQLSelectQuery();
      }
    } );
    check( "EXECUTE_FOR_EACH_ROW", new IBooleanGetter() {
      public boolean get() {
        return meta.getExecuteForEachIncomingRow();
      }
    } );
    check( "SOCKET_TIMEOUT", new IStringGetter() {
      public String get() {
        return meta.getSocketTimeout();
      }
    } );
    check( "TRANSPORT_MAX_LENGTH", new IStringGetter() {
      public String get() {
        return meta.getMaxLength();
      }
    } );
  }

}
