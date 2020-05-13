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
package org.apache.hop.pipeline.transforms.gettablenames;

import org.apache.hop.core.injection.BaseMetadataInjectionTest;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class GetTableNamesMetaInjectionTest extends BaseMetadataInjectionTest<GetTableNamesMeta> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setup() throws Exception {
    setup( new GetTableNamesMeta() );
  }

  @Test
  public void test() throws Exception {
    check( "SCHEMANAME", new StringGetter() {
      public String get() {
        return meta.getSchemaName();
      }
    } );
    check( "TABLENAMEFIELDNAME", new StringGetter() {
      public String get() {
        return meta.getTablenameFieldName();
      }
    } );
    check( "SQLCREATIONFIELDNAME", new StringGetter() {
      public String get() {
        return meta.getSqlCreationFieldName();
      }
    } );
    check( "OBJECTTYPEFIELDNAME", new StringGetter() {
      public String get() {
        return meta.getObjectTypeFieldName();
      }
    } );
    check( "ISSYSTEMOBJECTFIELDNAME", new StringGetter() {
      public String get() {
        return meta.isSystemObjectFieldName();
      }
    } );
    check( "INCLUDECATALOG", new BooleanGetter() {
      public boolean get() {
        return meta.isIncludeCatalog();
      }
    } );
    check( "INCLUDESCHEMA", new BooleanGetter() {
      public boolean get() {
        return meta.isIncludeSchema();
      }
    } );
    check( "INCLUDETABLE", new BooleanGetter() {
      public boolean get() {
        return meta.isIncludeTable();
      }
    } );
    check( "INCLUDEVIEW", new BooleanGetter() {
      public boolean get() {
        return meta.isIncludeView();
      }
    } );
    check( "INCLUDEPROCEDURE", new BooleanGetter() {
      public boolean get() {
        return meta.isIncludeProcedure();
      }
    } );
    check( "INCLUDESYNONYM", new BooleanGetter() {
      public boolean get() {
        return meta.isIncludeSynonym();
      }
    } );
    check( "ADDSCHEMAINOUTPUT", new BooleanGetter() {
      public boolean get() {
        return meta.isAddSchemaInOut();
      }
    } );
    check( "DYNAMICSCHEMA", new BooleanGetter() {
      public boolean get() {
        return meta.isDynamicSchema();
      }
    } );
    check( "SCHEMANAMEFIELD", new StringGetter() {
      public String get() {
        return meta.getSchemaFieldName();
      }
    } );
    check( "CONNECTIONNAME", new StringGetter() {
      public String get() {
        return "My Connection";
      }
    }, "My Connection" );
  }
}
