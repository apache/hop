/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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

package org.apache.hop.pipeline.transforms.update;

import org.apache.hop.core.injection.BaseMetadataInjectionTest;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class UpdateMetaInjectionTest extends BaseMetadataInjectionTest<UpdateMeta> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setup() throws Exception {
    setup( new UpdateMeta() );
  }

  @Test
  public void test() throws Exception {
    check( "SCHEMA_NAME", new IStringGetter() {
      public String get() {
        return meta.getSchemaName();
      }
    } );
    check( "TABLE_NAME", new IStringGetter() {
      public String get() {
        return meta.getTableName();
      }
    } );
    check( "COMMIT_SIZE", new IStringGetter() {
      public String get() {
        return meta.getCommitSizeVar();
      }
    } );
    check( "BATCH_UPDATE", new IBooleanGetter() {
      public boolean get() {
        return meta.useBatchUpdate();
      }
    } );
    check( "SKIP_LOOKUP", new IBooleanGetter() {
      public boolean get() {
        return meta.isSkipLookup();
      }
    } );
    check( "IGNORE_LOOKUP_FAILURE", new IBooleanGetter() {
      public boolean get() {
        return meta.isErrorIgnored();
      }
    } );
    check( "FLAG_FIELD", new IStringGetter() {
      public String get() {
        return meta.getIgnoreFlagField();
      }
    } );
    check( "KEY_STREAM", new IStringGetter() {
      public String get() {
        return meta.getKeyStream()[ 0 ];
      }
    } );
    check( "KEY_LOOKUP", new IStringGetter() {
      public String get() {
        return meta.getKeyLookup()[ 0 ];
      }
    } );
    check( "KEY_CONDITION", new IStringGetter() {
      public String get() {
        return meta.getKeyCondition()[ 0 ];
      }
    } );
    check( "KEY_STREAM2", new IStringGetter() {
      public String get() {
        return meta.getKeyStream2()[ 0 ];
      }
    } );
    check( "UPDATE_LOOKUP", new IStringGetter() {
      public String get() {
        return meta.getUpdateLookup()[ 0 ];
      }
    } );
    check( "UPDATE_STREAM", new IStringGetter() {
      public String get() {
        return meta.getUpdateStream()[ 0 ];
      }
    } );
    skipPropertyTest( "CONNECTIONNAME" );
  }
}
