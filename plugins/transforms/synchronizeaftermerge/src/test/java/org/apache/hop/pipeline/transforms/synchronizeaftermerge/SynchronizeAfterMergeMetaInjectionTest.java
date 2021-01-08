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

package org.apache.hop.pipeline.transforms.synchronizeaftermerge;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.injection.BaseMetadataInjectionTest;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SynchronizeAfterMergeMetaInjectionTest extends BaseMetadataInjectionTest<SynchronizeAfterMergeMeta> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setup() throws Exception {
    super.setup( new SynchronizeAfterMergeMeta() );
  }

  @Test
  public void test() throws Exception {
    check( "SHEMA_NAME", new IStringGetter() {
      @Override
      public String get() {
        return meta.getSchemaName();
      }
    } );
    check( "TABLE_NAME", new IStringGetter() {
      @Override
      public String get() {
        return meta.getTableName();
      }
    } );
    check( "TABLE_FIELD", new IStringGetter() {
      @Override
      public String get() {
        return meta.getKeyLookup()[ 0 ];
      }
    } );
    check( "STREAM_FIELD1", new IStringGetter() {
      @Override
      public String get() {
        return meta.getKeyStream()[ 0 ];
      }
    } );
    check( "STREAM_FIELD2", new IStringGetter() {
      @Override
      public String get() {
        return meta.getKeyStream2()[ 0 ];
      }
    } );
    check( "COMPARATOR", new IStringGetter() {
      @Override
      public String get() {
        return meta.getKeyCondition()[ 0 ];
      }
    } );

    check( "UPDATE_TABLE_FIELD", new IStringGetter() {
      @Override
      public String get() {
        return meta.getUpdateLookup()[ 0 ];
      }
    } );
    check( "STREAM_FIELD", new IStringGetter() {
      @Override
      public String get() {
        return meta.getUpdateStream()[ 0 ];
      }
    } );
    check( "UPDATE", new IBooleanGetter() {
      @Override
      public boolean get() {
        return meta.getUpdate()[ 0 ];
      }
    } );

    check( "COMMIT_SIZE", new IStringGetter() {
      @Override
      public String get() {
        return meta.getCommitSize();
      }
    } );
    check( "TABLE_NAME_IN_FIELD", new IBooleanGetter() {
      @Override
      public boolean get() {
        return meta.istablenameInField();
      }
    } );
    check( "TABLE_NAME_FIELD", new IStringGetter() {
      @Override
      public String get() {
        return meta.gettablenameField();
      }
    } );
    check( "OPERATION_ORDER_FIELD", new IStringGetter() {
      @Override
      public String get() {
        return meta.getOperationOrderField();
      }
    } );
    check( "USE_BATCH_UPDATE", new IBooleanGetter() {
      @Override
      public boolean get() {
        return meta.useBatchUpdate();
      }
    } );
    check( "PERFORM_LOOKUP", new IBooleanGetter() {
      @Override
      public boolean get() {
        return meta.isPerformLookup();
      }
    } );
    check( "ORDER_INSERT", new IStringGetter() {
      @Override
      public String get() {
        return meta.getOrderInsert();
      }
    } );
    check( "ORDER_UPDATE", new IStringGetter() {
      @Override
      public String get() {
        return meta.getOrderUpdate();
      }
    } );
    check( "ORDER_DELETE", new IStringGetter() {
      @Override
      public String get() {
        return meta.getOrderDelete();
      }
    } );
    check( "CONNECTION_NAME", new IStringGetter() {
      public String get() {
        return "My Connection";
      }
    }, "My Connection" );
  }

  @Test
  public void getXml() throws HopException {
    skipProperties( "CONNECTION_NAME", "TABLE_NAME", "STREAM_FIELD2", "PERFORM_LOOKUP", "COMPARATOR",
      "OPERATION_ORDER_FIELD", "ORDER_DELETE", "SHEMA_NAME", "TABLE_NAME_IN_FIELD", "ORDER_UPDATE", "ORDER_INSERT",
      "USE_BATCH_UPDATE", "STREAM_FIELD", "TABLE_FIELD", "COMMIT_SIZE", "TABLE_NAME_FIELD" );
    meta.setDefault();
    check( "STREAM_FIELD1", new IStringGetter() {
      @Override
      public String get() {
        return meta.getKeyStream()[ 0 ];
      }
    } );
    check( "UPDATE_TABLE_FIELD", new IStringGetter() {
      @Override
      public String get() {
        return meta.getUpdateLookup()[ 0 ];
      }
    } );
    check( "UPDATE", new IBooleanGetter() {
      @Override
      public boolean get() {
        return meta.getUpdate()[ 0 ];
      }
    } );

    meta.getXml();

    String[] actualKeyLookup = meta.getKeyLookup();
    assertNotNull( actualKeyLookup );
    assertEquals( 1, actualKeyLookup.length );

    String[] actualKeyCondition = meta.getKeyCondition();
    assertNotNull( actualKeyCondition );
    assertEquals( 1, actualKeyCondition.length );

    String[] actualKeyStream2 = meta.getKeyCondition();
    assertNotNull( actualKeyStream2 );
    assertEquals( 1, actualKeyStream2.length );

    String[] actualUpdateStream = meta.getUpdateStream();
    assertNotNull( actualUpdateStream );
    assertEquals( 1, actualUpdateStream.length );
  }

  private void skipProperties( String... propertyName ) {
    for ( String property : propertyName ) {
      skipPropertyTest( property );
    }
  }

}
