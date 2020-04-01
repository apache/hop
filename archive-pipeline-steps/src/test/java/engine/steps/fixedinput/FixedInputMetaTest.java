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
package org.apache.hop.pipeline.steps.fixedinput;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.step.StepMetaInterface;
import org.apache.hop.pipeline.steps.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.steps.loadsave.initializer.InitializerInterface;
import org.apache.hop.pipeline.steps.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.steps.loadsave.validator.FieldLoadSaveValidator;
import org.apache.hop.pipeline.steps.loadsave.validator.IntLoadSaveValidator;
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

public class FixedInputMetaTest implements InitializerInterface<StepMetaInterface> {
  LoadSaveTester loadSaveTester;
  Class<FixedInputMeta> testMetaClass = FixedInputMeta.class;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init( false );
    List<String> attributes =
      Arrays.asList( "filename", "headerPresent", "lineWidth", "bufferSize", "lazyConversionActive", "lineFeedPresent",
        "runningInParallel", "fileType", "addResultFile", "encoding", "fieldDefinition" );

    Map<String, String> getterMap = new HashMap<>();
    Map<String, String> setterMap = new HashMap<>();
    Map<String, FieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<String, FieldLoadSaveValidator<?>>();
    attrValidatorMap.put( "fieldDefinition",
      new ArrayLoadSaveValidator<FixedFileInputField>( new FixedFileInputFieldLoadSaveValidator(), 5 ) );
    attrValidatorMap.put( "fileType", new IntLoadSaveValidator( FixedInputMeta.fileTypeDesc.length ) );

    Map<String, FieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<String, FieldLoadSaveValidator<?>>();

    loadSaveTester =
      new LoadSaveTester( testMetaClass, attributes, new ArrayList<>(),
        getterMap, setterMap, attrValidatorMap, typeValidatorMap, this );
  }

  // Call the allocate method on the LoadSaveTester meta class
  @Override
  public void modify( StepMetaInterface someMeta ) {
    if ( someMeta instanceof FixedInputMeta ) {
      ( (FixedInputMeta) someMeta ).allocate( 5 );
    }
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  public class FixedFileInputFieldLoadSaveValidator implements FieldLoadSaveValidator<FixedFileInputField> {
    final Random rand = new Random();

    @Override
    public FixedFileInputField getTestObject() {
      FixedFileInputField rtn = new FixedFileInputField();
      rtn.setCurrency( UUID.randomUUID().toString() );
      rtn.setDecimal( UUID.randomUUID().toString() );
      rtn.setFormat( UUID.randomUUID().toString() );
      rtn.setGrouping( UUID.randomUUID().toString() );
      rtn.setName( UUID.randomUUID().toString() );
      rtn.setTrimType( rand.nextInt( 4 ) );
      rtn.setPrecision( rand.nextInt( 9 ) );
      rtn.setLength( rand.nextInt( 50 ) );
      rtn.setSamples( new String[] { UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString() } );
      rtn.setType( rand.nextInt( 7 ) );
      return rtn;
    }

    @Override
    public boolean validateTestObject( FixedFileInputField testObject, Object actual ) {
      if ( !( actual instanceof FixedFileInputField ) ) {
        return false;
      }
      FixedFileInputField actualInput = (FixedFileInputField) actual;
      return ( testObject.getXML().equals( actualInput.getXML() ) );
    }
  }

}
