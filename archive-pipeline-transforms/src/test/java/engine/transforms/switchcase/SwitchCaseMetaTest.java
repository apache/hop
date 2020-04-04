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

import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInterface;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.FieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.FieldLoadSaveValidatorFactory;
import org.apache.hop.pipeline.transforms.loadsave.validator.ListLoadSaveValidator;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * @author nhudak
 */
public class SwitchCaseMetaTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  LoadSaveTester<SwitchCaseMeta> loadSaveTester;

  public SwitchCaseMetaTest() {
    //SwitchCaseMeta bean-like attributes
    List<String> attributes = Arrays.asList(
      "fieldname",
      "isContains",
      "caseValueFormat", "caseValueDecimal", /* "caseValueType",*/"caseValueGroup",
      "defaultTargetTransformname",
      "caseTargets" );

    //Non-standard getters & setters
    Map<String, String> getterMap = new HashMap<>();
    getterMap.put( "isContains", "isContains" );

    Map<String, String> setterMap = new HashMap<>();
    setterMap.put( "isContains", "setContains" );

    Map<String, FieldLoadSaveValidator<?>> attrValidatorMap =
      new HashMap<String, FieldLoadSaveValidator<?>>();

    Map<String, FieldLoadSaveValidator<?>> typeValidatorMap =
      new HashMap<String, FieldLoadSaveValidator<?>>();

    this.loadSaveTester = new LoadSaveTester<>( SwitchCaseMeta.class,
      attributes,
      getterMap, setterMap,
      attrValidatorMap, typeValidatorMap );

    FieldLoadSaveValidatorFactory validatorFactory = loadSaveTester.getFieldLoadSaveValidatorFactory();

    FieldLoadSaveValidator<SwitchCaseTarget> targetValidator = new FieldLoadSaveValidator<SwitchCaseTarget>() {
      private final TransformMetaInterface targetTransformInterface = new DummyMeta();

      @Override
      public SwitchCaseTarget getTestObject() {
        return new SwitchCaseTarget() {
          {
            caseValue = UUID.randomUUID().toString();
            caseTargetTransformName = UUID.randomUUID().toString();
            caseTargetTransform = new TransformMeta( caseTargetTransformName, targetTransformInterface );
          }
        };
      }

      @Override
      public boolean validateTestObject( SwitchCaseTarget testObject, Object actual ) {
        return testObject.caseValue.equals( ( (SwitchCaseTarget) actual ).caseValue )
          && testObject.caseTargetTransformName.equals( ( (SwitchCaseTarget) actual ).caseTargetTransformName );
      }
    };

    validatorFactory.registerValidator( validatorFactory.getName( SwitchCaseTarget.class ), targetValidator );
    validatorFactory.registerValidator( validatorFactory.getName( List.class, SwitchCaseTarget.class ),
      new ListLoadSaveValidator<SwitchCaseTarget>( targetValidator ) );
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  // Note - cloneTest removed because it's now covered by the load/save tester


  @Test
  public void testsearchInfoAndTargetTransformsTwice() {
    TransformMetaInterface defTransform = new DummyMeta();
    TransformMeta transformMeta = new TransformMeta( "id", "default", defTransform );

    SwitchCaseMeta meta = new SwitchCaseMeta();
    meta.allocate();
    meta.setDefaultTargetTransformName( transformMeta.getName() );
    meta.searchInfoAndTargetTransforms( Collections.singletonList( transformMeta ) );
    // would throw npe
    meta.searchInfoAndTargetTransforms( Collections.singletonList( transformMeta ) );
  }
}
