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
package org.apache.hop.trans.steps.parallelgzipcsv;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.trans.step.StepMetaInterface;
import org.apache.hop.trans.steps.loadsave.LoadSaveTester;
import org.apache.hop.trans.steps.loadsave.initializer.InitializerInterface;
import org.apache.hop.trans.steps.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.trans.steps.loadsave.validator.FieldLoadSaveValidator;
import org.apache.hop.trans.steps.textfileinput.TextFileInputField;
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

public class ParGzipCsvInputMetaTest implements InitializerInterface<StepMetaInterface> {
  LoadSaveTester loadSaveTester;
  Class<ParGzipCsvInputMeta> testMetaClass = ParGzipCsvInputMeta.class;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init( false );
    List<String> attributes =
      Arrays.asList( "filename", "filenameField", "includingFilename", "rowNumField", "headerPresent", "delimiter",
        "enclosure", "bufferSize", "lazyConversionActive", "addResultFile", "runningInParallel", "encoding",
        "inputFields" );

    Map<String, String> getterMap = new HashMap<>();
    Map<String, String> setterMap = new HashMap<>();

    Map<String, FieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<String, FieldLoadSaveValidator<?>>();
    attrValidatorMap.put( "inputFields", new ArrayLoadSaveValidator<TextFileInputField>( new TextFileInputFieldValidator(), 5 ) );

    Map<String, FieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<String, FieldLoadSaveValidator<?>>();

    loadSaveTester =
      new LoadSaveTester( testMetaClass, attributes, new ArrayList<>(), getterMap,
        setterMap, attrValidatorMap, typeValidatorMap, this );
  }

  // Call the allocate method on the LoadSaveTester meta class
  @Override
  public void modify( StepMetaInterface someMeta ) {
    if ( someMeta instanceof ParGzipCsvInputMeta ) {
      ( (ParGzipCsvInputMeta) someMeta ).allocate( 5 );
    }
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  // Note - cloneTest removed as clone is now covered by the load/save validator.

  private static class TextFileInputFieldValidator implements FieldLoadSaveValidator<TextFileInputField> {
    final Random rand = new Random();

    @Override
    public TextFileInputField getTestObject() {
      TextFileInputField rtn = new TextFileInputField();
      rtn.setCurrencySymbol( UUID.randomUUID().toString() );
      rtn.setDecimalSymbol( UUID.randomUUID().toString() );
      rtn.setFormat( UUID.randomUUID().toString() );
      rtn.setGroupSymbol( UUID.randomUUID().toString() );
      rtn.setName( UUID.randomUUID().toString() );
      rtn.setTrimType( rand.nextInt( 4 ) );
      rtn.setPrecision( rand.nextInt( 9 ) );
      rtn.setLength( rand.nextInt( 50 ) );
      rtn.setType( rand.nextInt( 7 ) );
      // Note - these fields aren't serialized by the meta class ... cannot test for them
      // rtn.setRepeated( rand.nextBoolean() );
      // rtn.setSamples( new String[] { UUID.randomUUID().toString(), UUID.randomUUID().toString(),
      // UUID.randomUUID().toString() } );
      // rtn.setNullString( UUID.randomUUID().toString() );
      // rtn.setIfNullValue( UUID.randomUUID().toString() );
      // rtn.setIgnored( rand.nextBoolean() );
      // rtn.setPosition( rand.nextInt( 10 ) );
      return rtn;
    }

    @Override
    public boolean validateTestObject( TextFileInputField testObject, Object actual ) {
      if ( !( actual instanceof TextFileInputField ) ) {
        return false;
      }

      TextFileInputField another = (TextFileInputField) actual;
      return new EqualsBuilder()
        .append( testObject.getCurrencySymbol(), another.getCurrencySymbol() )
        .append( testObject.getDecimalSymbol(), another.getDecimalSymbol() )
        .append( testObject.getFormat(), another.getFormat() )
        .append( testObject.getGroupSymbol(), another.getGroupSymbol() )
        .append( testObject.getName(), another.getName() )
        .append( testObject.getTrimType(), another.getTrimType() )
        .append( testObject.getPrecision(), another.getPrecision() )
        .append( testObject.getLength(), another.getLength() )
        .append( testObject.getType(), another.getType() )
        // Note - these fields aren't serialized by the meta class ... cannot test for them
        // .append( testObject.isRepeated(), another.isRepeated() )
        // .append( testObject.getSamples(), another.getSamples() )
        // .append( testObject.getNullString(), another.getNullString() )
        // .append( testObject.getIfNullValue(), another.getIfNullValue() )
        // .append( testObject.isIgnored(), another.isIgnored() )
        // .append( testObject.getPosition(), another.getPosition() )
        .isEquals();
    }
  }
}
