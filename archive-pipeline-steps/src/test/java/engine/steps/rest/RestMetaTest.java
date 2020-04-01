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

package org.apache.hop.pipeline.steps.rest;

import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.step.StepMeta;
import org.apache.hop.pipeline.steps.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.steps.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.steps.loadsave.validator.FieldLoadSaveValidator;
import org.apache.hop.pipeline.steps.loadsave.validator.StringLoadSaveValidator;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RestMetaTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void beforeClass() throws HopException {
    PluginRegistry.addPluginType( TwoWayPasswordEncoderPluginType.getInstance() );
    PluginRegistry.init();
    String passwordEncoderPluginID =
      Const.NVL( EnvUtil.getSystemProperty( Const.HOP_PASSWORD_ENCODER_PLUGIN ), "Hop" );
    Encr.init( passwordEncoderPluginID );
  }

  @Test
  public void testLoadSaveRoundTrip() throws HopException {
    List<String> attributes =
      Arrays.asList( "applicationType", "method", "url", "urlInField", "dynamicMethod", "methodFieldName",
        "urlField", "bodyField", "httpLogin", "httpPassword", "proxyHost", "proxyPort", "preemptive",
        "trustStoreFile", "trustStorePassword", "headerField", "headerName", "parameterField", "parameterName",
        "matrixParameterField", "matrixParameterName", "fieldName", "resultCodeFieldName", "responseTimeFieldName",
        "responseHeaderFieldName" );

    Map<String, FieldLoadSaveValidator<?>> fieldLoadSaveValidatorAttributeMap =
      new HashMap<String, FieldLoadSaveValidator<?>>();

    // Arrays need to be consistent length
    FieldLoadSaveValidator<String[]> stringArrayLoadSaveValidator =
      new ArrayLoadSaveValidator<String>( new StringLoadSaveValidator(), 25 );
    fieldLoadSaveValidatorAttributeMap.put( "headerField", stringArrayLoadSaveValidator );
    fieldLoadSaveValidatorAttributeMap.put( "headerName", stringArrayLoadSaveValidator );
    fieldLoadSaveValidatorAttributeMap.put( "parameterField", stringArrayLoadSaveValidator );
    fieldLoadSaveValidatorAttributeMap.put( "parameterName", stringArrayLoadSaveValidator );
    fieldLoadSaveValidatorAttributeMap.put( "matrixParameterField", stringArrayLoadSaveValidator );
    fieldLoadSaveValidatorAttributeMap.put( "matrixParameterName", stringArrayLoadSaveValidator );

    LoadSaveTester<RestMeta> loadSaveTester =
      new LoadSaveTester<RestMeta>( RestMeta.class, attributes, new HashMap<>(),
        new HashMap<>(), fieldLoadSaveValidatorAttributeMap,
        new HashMap<String, FieldLoadSaveValidator<?>>() );

    loadSaveTester.testSerialization();
  }

  @Test
  public void testStepChecks() {
    RestMeta meta = new RestMeta();
    List<CheckResultInterface> remarks = new ArrayList<CheckResultInterface>();
    PipelineMeta pipelineMeta = new PipelineMeta();
    StepMeta step = new StepMeta();
    RowMetaInterface prev = new RowMeta();
    RowMetaInterface info = new RowMeta();
    String[] input = new String[ 0 ];
    String[] output = new String[ 0 ];
    VariableSpace variables = new Variables();
    IMetaStore metaStore = null;

    // In a default configuration, it's expected that some errors will occur.
    // For this, we'll grab a baseline count of the number of errors
    // as the error count should decrease as we change configuration settings to proper values.
    remarks.clear();
    meta.check( remarks, pipelineMeta, step, prev, input, output, info, variables, metaStore );
    final int errorsDefault = getCheckResultErrorCount( remarks );
    assertTrue( errorsDefault > 0 );

    // Setting the step to read the URL from a field should fix one of the check() errors
    meta.setUrlInField( true );
    meta.setUrlField( "urlField" );
    prev.addValueMeta( new ValueMetaString( "urlField" ) );
    remarks.clear();
    meta.check( remarks, pipelineMeta, step, prev, input, output, info, variables, metaStore );
    int errorsCurrent = getCheckResultErrorCount( remarks );
    assertTrue( errorsDefault > errorsCurrent );
  }

  private static int getCheckResultErrorCount( List<CheckResultInterface> remarks ) {
    return remarks.stream()
      .filter( p -> p.getType() == CheckResultInterface.TYPE_RESULT_ERROR )
      .collect( Collectors.toList() ).size();
  }

  @Test
  public void testEntityEnclosingMethods() {
    assertTrue( RestMeta.isActiveBody( RestMeta.HTTP_METHOD_POST ) );
    assertTrue( RestMeta.isActiveBody( RestMeta.HTTP_METHOD_PUT ) );
    assertTrue( RestMeta.isActiveBody( RestMeta.HTTP_METHOD_PATCH ) );

    assertFalse( RestMeta.isActiveBody( RestMeta.HTTP_METHOD_GET ) );
    assertFalse( RestMeta.isActiveBody( RestMeta.HTTP_METHOD_DELETE ) );
    assertFalse( RestMeta.isActiveBody( RestMeta.HTTP_METHOD_HEAD ) );
    assertFalse( RestMeta.isActiveBody( RestMeta.HTTP_METHOD_OPTIONS ) );
  }
}
