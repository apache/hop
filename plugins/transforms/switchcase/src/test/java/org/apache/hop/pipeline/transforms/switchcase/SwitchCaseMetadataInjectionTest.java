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

package org.apache.hop.pipeline.transforms.switchcase;

import org.apache.hop.core.injection.BaseMetadataInjectionTest;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class SwitchCaseMetadataInjectionTest extends BaseMetadataInjectionTest<SwitchCaseMeta> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setup() throws Exception {
    super.setup( new SwitchCaseMeta() );
  }

  @Test
  public void test() throws Exception {
    check( "FIELD_NAME", new IStringGetter() {
      @Override
      public String get() {
        return meta.getFieldname();
      }
    } );
    String[] typeNames = ValueMetaBase.getAllTypes();
    checkStringToInt( "VALUE_TYPE", new IIntGetter() {
      public int get() {
        return meta.getCaseValueType();
      }
    }, typeNames, getTypeCodes( typeNames ) );
    check( "VALUE_DECIMAL", new IStringGetter() {
      @Override
      public String get() {
        return meta.getCaseValueDecimal();
      }
    } );
    check( "VALUE_GROUP", new IStringGetter() {
      @Override
      public String get() {
        return meta.getCaseValueGroup();
      }
    } );
    check( "VALUE_FORMAT", new IStringGetter() {
      @Override
      public String get() {
        return meta.getCaseValueFormat();
      }
    } );
    check( "CONTAINS", new IBooleanGetter() {
      @Override
      public boolean get() {
        return meta.isContains();
      }
    } );
    check( "DEFAULT_TARGET_TRANSFORM_NAME", new IStringGetter() {
      @Override
      public String get() {
        return meta.getDefaultTargetTransformName();
      }
    } );
    check( "SWITCH_CASE_TARGET.CASE_VALUE", new IStringGetter() {
      @Override
      public String get() {
        return meta.getCaseTargets().get( 0 ).caseValue;
      }
    } );
    check( "SWITCH_CASE_TARGET.CASE_TARGET_TRANSFORM_NAME", new IStringGetter() {
      @Override
      public String get() {
        return meta.getCaseTargets().get( 0 ).caseTargetTransformName;
      }
    } );
  }

}
