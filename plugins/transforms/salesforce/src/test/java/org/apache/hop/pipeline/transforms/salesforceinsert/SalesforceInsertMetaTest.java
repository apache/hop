/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.salesforceinsert;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.TransformLoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.BooleanLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.apache.hop.pipeline.transforms.salesforce.SalesforceMetaTest;
import org.apache.hop.pipeline.transforms.salesforce.SalesforceTransformMeta;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SalesforceInsertMetaTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void setUpBeforeClass() throws HopException {
    PluginRegistry.addPluginType( ValueMetaPluginType.getInstance() );
    PluginRegistry.addPluginType( TwoWayPasswordEncoderPluginType.getInstance() );
    PluginRegistry.init( true );
    String passwordEncoderPluginID =
      Const.NVL( EnvUtil.getSystemProperty( Const.HOP_PASSWORD_ENCODER_PLUGIN ), "Hop" );
    Encr.init( passwordEncoderPluginID );
  }

  @Test
  public void testErrorHandling() {
    SalesforceTransformMeta meta = new SalesforceInsertMeta();
    assertTrue( meta.supportsErrorHandling() );
  }

  @Test
  public void testBatchSize() {
    SalesforceInsertMeta meta = new SalesforceInsertMeta();
    meta.setBatchSize( "20" );
    assertEquals( "20", meta.getBatchSize() );
    assertEquals( 20, meta.getBatchSizeInt() );

    // Pass invalid batch size, should get default value of 10
    meta.setBatchSize( "unknown" );
    assertEquals( "unknown", meta.getBatchSize() );
    assertEquals( 10, meta.getBatchSizeInt() );
  }

  @Test
  public void testCheck() {
    SalesforceInsertMeta meta = new SalesforceInsertMeta();
    meta.setDefault();
    List<ICheckResult> remarks = new ArrayList<>();
    meta.check( remarks, null, null, null, null, null, null, null, null );
    boolean hasError = false;
    for ( ICheckResult cr : remarks ) {
      if ( cr.getType() == CheckResult.TYPE_RESULT_ERROR ) {
        hasError = true;
      }
    }
    assertFalse( remarks.isEmpty() );
    assertTrue( hasError );

    remarks.clear();
    meta.setDefault();
    meta.setUsername( "user" );
    meta.setUpdateLookup( new String[] { "SalesforceField" } );
    meta.setUpdateStream( new String[] { "StreamField" } );
    meta.setUseExternalId( new Boolean[] { false } );
    meta.check( remarks, null, null, null, null, null, null, null, null );
    hasError = false;
    for ( ICheckResult cr : remarks ) {
      if ( cr.getType() == CheckResult.TYPE_RESULT_ERROR ) {
        hasError = true;
      }
    }
    assertFalse( remarks.isEmpty() );
    assertFalse( hasError );
  }

  @Test
  public void testGetFields() throws HopTransformException {
    SalesforceInsertMeta meta = new SalesforceInsertMeta();
    meta.setDefault();
    IRowMeta r = new RowMeta();
    meta.getFields( r, "thisTransform", null, null, new Variables(), null );
    assertEquals( 1, r.size() );
    assertEquals( "Id", r.getFieldNames()[ 0 ] );

    meta.setSalesforceIDFieldName( "id_field" );
    r.clear();
    meta.getFields( r, "thisTransform", null, null, new Variables(), null );
    assertEquals( 1, r.size() );
    assertEquals( "id_field", r.getFieldNames()[ 0 ] );
  }

  @Test
  public void testSalesforceInsertMeta() throws HopException {
    List<String> attributes = new ArrayList<>();
    attributes.addAll( SalesforceMetaTest.getDefaultAttributes() );
    attributes.addAll( Arrays.asList( "batchSize", "salesforceIDFieldName", "updateLookup", "updateStream",
      "useExternalId", "rollbackAllChangesOnError" ) );
    Map<String, String> getterMap = new HashMap<>();
    Map<String, String> setterMap = new HashMap<>();
    Map<String, IFieldLoadSaveValidator<?>> fieldLoadSaveValidators = new HashMap<>();
    fieldLoadSaveValidators.put( "updateLookup",
      new ArrayLoadSaveValidator<>( new StringLoadSaveValidator(), 50 ) );
    fieldLoadSaveValidators.put( "updateStream",
      new ArrayLoadSaveValidator<>( new StringLoadSaveValidator(), 50 ) );
    fieldLoadSaveValidators.put( "useExternalId",
      new ArrayLoadSaveValidator<>( new BooleanLoadSaveValidator(), 50 ) );

    TransformLoadSaveTester<SalesforceInsertMeta> transformLoadSaveTester =
      new TransformLoadSaveTester( SalesforceInsertMeta.class, attributes, attributes, getterMap, setterMap,
        fieldLoadSaveValidators, new HashMap<>() );

    transformLoadSaveTester.testXmlRoundTrip();
  }
}
