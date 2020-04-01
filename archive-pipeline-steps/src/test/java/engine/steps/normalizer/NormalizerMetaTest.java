/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.steps.normalizer;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.steps.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.steps.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.steps.loadsave.validator.FieldLoadSaveValidator;
import org.apache.hop.pipeline.steps.normaliser.NormaliserMeta;
import org.apache.hop.pipeline.steps.normaliser.NormaliserMeta.NormaliserField;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NormalizerMetaTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Test
  public void loadSaveTest() throws HopException {
    List<String> attributes = Arrays.asList( "normaliserFields" );

    NormaliserField testField = new NormaliserField();
    testField.setName( "TEST_NAME" );
    testField.setValue( "TEST_VALUE" );
    testField.setNorm( "TEST" );

    Map<String, FieldLoadSaveValidator<?>> fieldLoadSaveValidatorTypeMap =
      new HashMap<String, FieldLoadSaveValidator<?>>();
    fieldLoadSaveValidatorTypeMap.put( NormaliserField[].class.getCanonicalName(),
      new ArrayLoadSaveValidator<NormaliserField>( new NormaliserFieldLoadSaveValidator( testField ), 50 ) );

    LoadSaveTester<NormaliserMeta> tester =
      new LoadSaveTester<NormaliserMeta>( NormaliserMeta.class, attributes, new HashMap<>(),
        new HashMap<>(), new HashMap<String, FieldLoadSaveValidator<?>>(),
        fieldLoadSaveValidatorTypeMap );

    tester.testSerialization();
  }

  public static class NormaliserFieldLoadSaveValidator implements FieldLoadSaveValidator<NormaliserField> {

    private final NormaliserField defaultValue;

    public NormaliserFieldLoadSaveValidator( NormaliserField defaultValue ) {
      this.defaultValue = defaultValue;
    }

    @Override
    public NormaliserField getTestObject() {
      return defaultValue;
    }

    @Override
    public boolean validateTestObject( NormaliserField testObject, Object actual ) {
      return testObject.equals( actual );
    }

  }

}
