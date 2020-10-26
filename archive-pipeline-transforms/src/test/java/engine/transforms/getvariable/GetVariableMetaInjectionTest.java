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

package org.apache.hop.pipeline.transforms.getvariable;

import org.apache.hop.core.injection.BaseMetadataInjectionTest;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class GetVariableMetaInjectionTest extends BaseMetadataInjectionTest<GetVariableMeta> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setup() throws Exception {
    setup( new GetVariableMeta() );
  }

  @Test
  public void test() throws Exception {
    check( "FIELDNAME", new StringGetter() {
      public String get() {
        return meta.getFieldDefinitions()[ 0 ].getFieldName();
      }
    } );
    check( "VARIABLE", new StringGetter() {
      public String get() {
        return meta.getFieldDefinitions()[ 0 ].getVariableString();
      }
    } );
    check( "FIELDTYPE", new IntGetter() {
      public int get() {
        return meta.getFieldDefinitions()[ 0 ].getFieldType();
      }
    } );
    check( "FIELDFORMAT", new StringGetter() {
      public String get() {
        return meta.getFieldDefinitions()[ 0 ].getFieldFormat();
      }
    } );
    check( "FIELDLENGTH", new IntGetter() {
      public int get() {
        return meta.getFieldDefinitions()[ 0 ].getFieldLength();
      }
    } );
    check( "FIELDPRECISION", new IntGetter() {
      public int get() {
        return meta.getFieldDefinitions()[ 0 ].getFieldPrecision();
      }
    } );
    check( "CURRENCY", new StringGetter() {
      public String get() {
        return meta.getFieldDefinitions()[ 0 ].getCurrency();
      }
    } );
    check( "DECIMAL", new StringGetter() {
      public String get() {
        return meta.getFieldDefinitions()[ 0 ].getDecimal();
      }
    } );
    check( "GROUP", new StringGetter() {
      public String get() {
        return meta.getFieldDefinitions()[ 0 ].getGroup();
      }
    } );
    check( "TRIMTYPE", new IntGetter() {
      public int get() {
        return meta.getFieldDefinitions()[ 0 ].getTrimType();
      }
    } );
  }
}
