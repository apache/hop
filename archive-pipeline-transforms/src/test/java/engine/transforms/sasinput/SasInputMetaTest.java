/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
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
package org.apache.hop.pipeline.transforms.sasinput;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.FieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.ListLoadSaveValidator;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class SasInputMetaTest {
  LoadSaveTester loadSaveTester;
  Class<SasInputMeta> testMetaClass = SasInputMeta.class;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init( false );
    List<String> attributes =
      Arrays.asList( "acceptingField", "outputFields" );

    Map<String, String> gsMap = new HashMap<>();

    Map<String, FieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<String, FieldLoadSaveValidator<?>>();
    attrValidatorMap.put( "outputFields",
      new ListLoadSaveValidator<SasInputField>( new SasInputFieldLoadSaveValidator(), 5 ) );

    Map<String, FieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<String, FieldLoadSaveValidator<?>>();

    loadSaveTester =
      new LoadSaveTester( testMetaClass, attributes, gsMap, gsMap, attrValidatorMap, typeValidatorMap );
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  public class SasInputFieldLoadSaveValidator implements FieldLoadSaveValidator<SasInputField> {
    final Random rand = new Random();

    @Override
    public SasInputField getTestObject() {
      SasInputField rtn = new SasInputField();
      rtn.setRename( UUID.randomUUID().toString() );
      rtn.setDecimalSymbol( UUID.randomUUID().toString() );
      rtn.setConversionMask( UUID.randomUUID().toString() );
      rtn.setGroupingSymbol( UUID.randomUUID().toString() );
      rtn.setName( UUID.randomUUID().toString() );
      rtn.setTrimType( rand.nextInt( 4 ) );
      rtn.setPrecision( rand.nextInt( 9 ) );
      rtn.setType( rand.nextInt( 7 ) );
      rtn.setLength( rand.nextInt( 50 ) );
      return rtn;
    }

    @Override
    public boolean validateTestObject( SasInputField testObject, Object actual ) {
      if ( !( actual instanceof SasInputField ) ) {
        return false;
      }
      SasInputField another = (SasInputField) actual;
      return new EqualsBuilder()
        .append( testObject.getName(), another.getName() )
        .append( testObject.getTrimType(), another.getTrimType() )
        .append( testObject.getType(), another.getType() )
        .append( testObject.getPrecision(), another.getPrecision() )
        .append( testObject.getRename(), another.getRename() )
        .append( testObject.getDecimalSymbol(), another.getDecimalSymbol() )
        .append( testObject.getConversionMask(), another.getConversionMask() )
        .append( testObject.getGroupingSymbol(), another.getGroupingSymbol() )
        .append( testObject.getLength(), another.getLength() )
        .isEquals();
    }
  }


}
