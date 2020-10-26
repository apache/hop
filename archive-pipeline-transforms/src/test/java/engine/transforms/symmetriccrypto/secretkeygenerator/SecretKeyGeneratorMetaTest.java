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
package org.apache.hop.pipeline.transforms.symmetriccrypto.secretkeygenerator;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.InitializerInterface;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.FieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SecretKeyGeneratorMetaTest implements InitializerInterface<ITransform> {
  LoadSaveTester loadSaveTester;
  Class<SecretKeyGeneratorMeta> testMetaClass = SecretKeyGeneratorMeta.class;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init( false );
    List<String> attributes =
      Arrays.asList( "algorithm", "scheme", "secretKeyLength", "secretKeyCount", "secretKeyFieldName", "secretKeyLengthFieldName",
        "algorithmFieldName", "outputKeyInBinary" );

    Map<String, String> getterMap = new HashMap<String, String>() {
      {
        put( "algorithm", "getAlgorithm" );
        put( "scheme", "getScheme" );
        put( "secretKeyLength", "getSecretKeyLength" );
        put( "secretKeyCount", "getSecretKeyCount" );
        put( "secretKeyFieldName", "getSecretKeyFieldName" );
        put( "secretKeyLengthFieldName", "getSecretKeyLengthFieldName" );
        put( "algorithmFieldName", "getAlgorithmFieldName" );
        put( "outputKeyInBinary", "isOutputKeyInBinary" );
      }
    };
    Map<String, String> setterMap = new HashMap<String, String>() {
      {
        put( "algorithm", "setFieldAlgorithm" );
        put( "scheme", "setScheme" );
        put( "secretKeyLength", "setSecretKeyLength" );
        put( "secretKeyCount", "setSecretKeyCount" );
        put( "secretKeyFieldName", "setSecretKeyFieldName" );
        put( "secretKeyLengthFieldName", "setSecretKeyLengthFieldName" );
        put( "algorithmFieldName", "setAlgorithmFieldName" );
        put( "outputKeyInBinary", "setOutputKeyInBinary" );
      }
    };
    FieldLoadSaveValidator<String[]> stringArrayLoadSaveValidator =
      new ArrayLoadSaveValidator<String>( new StringLoadSaveValidator(), 5 );


    Map<String, FieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<String, FieldLoadSaveValidator<?>>();
    attrValidatorMap.put( "algorithm", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "scheme", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "secretKeyLength", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "secretKeyCount", stringArrayLoadSaveValidator );

    Map<String, FieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<String, FieldLoadSaveValidator<?>>();

    loadSaveTester =
      new LoadSaveTester( testMetaClass, attributes, new ArrayList<>(),
        getterMap, setterMap, attrValidatorMap, typeValidatorMap, this );
  }

  // Call the allocate method on the LoadSaveTester meta class
  @Override
  public void modify( ITransform someMeta ) {
    if ( someMeta instanceof SecretKeyGeneratorMeta ) {
      ( (SecretKeyGeneratorMeta) someMeta ).allocate( 5 );
    }
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }
}
