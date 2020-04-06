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
package org.apache.hop.pipeline.transforms.validator;


import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.InitializerInterface;
import org.apache.hop.pipeline.transforms.loadsave.validator.FieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.ListLoadSaveValidator;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class ValidatorMetaTest implements InitializerInterface<ITransform> {
  LoadSaveTester loadSaveTester;
  Class<ValidatorMeta> testMetaClass = ValidatorMeta.class;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init( false );
    List<String> attributes =
      Arrays.asList( "validatingAll", "concatenatingErrors", "concatenationSeparator", "validations" );

    Map<String, String> getterMap = new HashMap<String, String>() {
      {
        put( "validatingAll", "isValidatingAll" );
        put( "concatenatingErrors", "isConcatenatingErrors" );
        put( "concatenationSeparator", "getConcatenationSeparator" );
        put( "validations", "getValidations" );
      }
    };
    Map<String, String> setterMap = new HashMap<String, String>() {
      {
        put( "validatingAll", "setValidatingAll" );
        put( "concatenatingErrors", "setConcatenatingErrors" );
        put( "concatenationSeparator", "setConcatenationSeparator" );
        put( "validations", "setValidations" );
      }
    };

    Map<String, FieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<String, FieldLoadSaveValidator<?>>();
    attrValidatorMap.put( "validations", new ListLoadSaveValidator<Validation>( new ValidationLoadSaveValidator(), 5 ) );

    Map<String, FieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<String, FieldLoadSaveValidator<?>>();

    loadSaveTester =
      new LoadSaveTester( testMetaClass, attributes, new ArrayList<>(),
        getterMap, setterMap, attrValidatorMap, typeValidatorMap, this );
  }

  // Call the allocate method on the LoadSaveTester meta class
  @Override
  public void modify( ITransform someMeta ) {
    if ( someMeta instanceof ValidatorMeta ) {
      ( (ValidatorMeta) someMeta ).allocate( 5 );
    }
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  //ValidationLoadSaveValidator
  public class ValidationLoadSaveValidator implements FieldLoadSaveValidator<Validation> {
    final Random rand = new Random();

    @Override
    public Validation getTestObject() {
      Validation rtn = new Validation();
      rtn.setName( UUID.randomUUID().toString() );
      rtn.setFieldName( UUID.randomUUID().toString() );
      rtn.setMaximumLength( UUID.randomUUID().toString() );
      rtn.setMinimumLength( UUID.randomUUID().toString() );
      rtn.setNullAllowed( rand.nextBoolean() );
      rtn.setOnlyNullAllowed( rand.nextBoolean() );
      rtn.setOnlyNumericAllowed( rand.nextBoolean() );
      rtn.setDataType( rand.nextInt( 9 ) );
      rtn.setDataTypeVerified( rand.nextBoolean() );
      rtn.setConversionMask( UUID.randomUUID().toString() );
      rtn.setDecimalSymbol( UUID.randomUUID().toString() );
      rtn.setGroupingSymbol( UUID.randomUUID().toString() );
      rtn.setMinimumValue( UUID.randomUUID().toString() );
      rtn.setMaximumValue( UUID.randomUUID().toString() );
      rtn.setAllowedValues( new String[] { UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString() } );
      rtn.setSourcingValues( rand.nextBoolean() );
      rtn.setSourcingTransformName( UUID.randomUUID().toString() );
      rtn.setSourcingTransform( null );
      rtn.setSourcingField( UUID.randomUUID().toString() );
      rtn.setStartString( UUID.randomUUID().toString() );
      rtn.setStartStringNotAllowed( UUID.randomUUID().toString() );
      rtn.setEndString( UUID.randomUUID().toString() );
      rtn.setEndStringNotAllowed( UUID.randomUUID().toString() );
      rtn.setRegularExpression( UUID.randomUUID().toString() );
      rtn.setRegularExpressionNotAllowed( UUID.randomUUID().toString() );
      rtn.setErrorCode( UUID.randomUUID().toString() );
      rtn.setErrorDescription( UUID.randomUUID().toString() );
      return rtn;
    }

    @Override
    public boolean validateTestObject( Validation testObject, Object actual ) {
      if ( !( actual instanceof Validation ) ) {
        return false;
      }
      Validation actualInput = (Validation) actual;
      return ( testObject.getXML().equals( actualInput.getXML() ) );
    }
  }
}
