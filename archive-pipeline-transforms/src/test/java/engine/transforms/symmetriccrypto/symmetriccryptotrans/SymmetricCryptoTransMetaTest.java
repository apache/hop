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

package org.apache.hop.pipeline.transforms.symmetriccrypto.symmetriccrypto;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.FieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IntLoadSaveValidator;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SymmetricCryptoPipelineMetaTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Test
  public void testRoundTrip() throws HopException {
    HopEnvironment.init();

    List<String> attributes = Arrays.asList( "operation_type", "algorithm", "schema", "secretKeyField", "messageField",
      "resultfieldname", "secretKey", "secretKeyInField", "readKeyAsBinary", "outputResultAsBinary" );

    Map<String, String> getterMap = new HashMap<>();
    getterMap.put( "operation_type", "getOperationType" );
    getterMap.put( "algorithm", "getAlgorithm" );
    getterMap.put( "schema", "getSchema" );
    getterMap.put( "secretKeyField", "getSecretKeyField" );
    getterMap.put( "messageField", "getMessageField" );
    getterMap.put( "resultfieldname", "getResultfieldname" );
    getterMap.put( "secretKey", "getSecretKey" );
    getterMap.put( "secretKeyInField", "isSecretKeyInField" );
    getterMap.put( "readKeyAsBinary", "isReadKeyAsBinary" );
    getterMap.put( "outputResultAsBinary", "isOutputResultAsBinary" );

    Map<String, String> setterMap = new HashMap<>();
    setterMap.put( "operation_type", "setOperationType" );
    setterMap.put( "algorithm", "setAlgorithm" );
    setterMap.put( "schema", "setSchema" );
    setterMap.put( "secretKeyField", "setsecretKeyField" );
    setterMap.put( "messageField", "setMessageField" );
    setterMap.put( "resultfieldname", "setResultfieldname" );
    setterMap.put( "secretKey", "setSecretKey" );
    setterMap.put( "secretKeyInField", "setSecretKeyInField" );
    setterMap.put( "readKeyAsBinary", "setReadKeyAsBinary" );
    setterMap.put( "outputResultAsBinary", "setOutputResultAsBinary" );

    Map<String, FieldLoadSaveValidator<?>> fieldLoadSaveValidator = new HashMap<String, FieldLoadSaveValidator<?>>();
    fieldLoadSaveValidator.put( "operation_type",
      new IntLoadSaveValidator( SymmetricCryptoPipelineMeta.operationTypeCode.length ) );

    LoadSaveTester loadSaveTester = new LoadSaveTester( SymmetricCryptoPipelineMeta.class, attributes,
      getterMap, setterMap, fieldLoadSaveValidator, new HashMap<String, FieldLoadSaveValidator<?>>() );

    loadSaveTester.testSerialization();
  }
}
