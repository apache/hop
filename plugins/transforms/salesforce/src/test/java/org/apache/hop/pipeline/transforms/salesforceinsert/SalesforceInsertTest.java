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

import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.bind.XmlObject;
import com.sforce.ws.wsdl.Constants;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.apache.hop.pipeline.transforms.salesforce.SalesforceConnection;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SalesforceInsertTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  private static final String ACCOUNT_EXT_ID_ACCOUNT_ID_C_ACCOUNT = "Account:ExtID_AccountId__c/Account";
  private static final String ACCOUNT_ID = "AccountId";
  private TransformMockHelper<SalesforceInsertMeta, SalesforceInsertData> smh;

  @BeforeClass
  public static void setUpBeforeClass() throws HopException {
    PluginRegistry.addPluginType( TwoWayPasswordEncoderPluginType.getInstance() );
    PluginRegistry.init();
    String passwordEncoderPluginID =
        Const.NVL( EnvUtil.getSystemProperty( Const.HOP_PASSWORD_ENCODER_PLUGIN ), "Hop" );
    Encr.init( passwordEncoderPluginID );
  }

  @Before
  public void setUp() throws Exception {
    smh = new TransformMockHelper<>( "SalesforceInsert", SalesforceInsertMeta.class, SalesforceInsertData.class );
    when( smh.logChannelFactory.create( any(), any( ILoggingObject.class ) ) ).thenReturn(
      smh.iLogChannel );
  }

  @After
  public void cleanUp() {
    smh.cleanUp();
  }

  @Test
  public void testWriteToSalesForceForNullExtIdField_WithExtIdNO() throws Exception {
    SalesforceInsertMeta meta = generateSalesforceInsertMeta( new String[] { ACCOUNT_ID }, new Boolean[] { false } );
    SalesforceInsertData data = generateSalesforceInsertData();

    SalesforceInsert sfInputTransform = new SalesforceInsert( smh.transformMeta, meta, data, 0, smh.pipelineMeta, smh.pipeline );
    sfInputTransform.init();

    RowMeta rowMeta = new RowMeta();
    ValueMetaBase valueMeta = new ValueMetaString( "AccNoExtId" );
    rowMeta.addValueMeta( valueMeta );
    data.inputRowMeta = rowMeta;

    sfInputTransform.writeToSalesForce( new Object[] { null } );
    assertEquals( 1, data.sfBuffer[0].getFieldsToNull().length );
    assertEquals( ACCOUNT_ID, data.sfBuffer[0].getFieldsToNull()[0] );
    assertNull( SalesforceConnection.getChildren( data.sfBuffer[0] ) );
  }

  @Test
  public void testWriteToSalesForceForNullExtIdField_WithExtIdYES() throws Exception {
    SalesforceInsertMeta meta = generateSalesforceInsertMeta( new String[] { ACCOUNT_EXT_ID_ACCOUNT_ID_C_ACCOUNT }, new Boolean[] { true } );
    SalesforceInsertData data = generateSalesforceInsertData();
    SalesforceInsert sfInputTransform = new SalesforceInsert( smh.transformMeta, meta, data, 0, smh.pipelineMeta, smh.pipeline );
    sfInputTransform.init();

    RowMeta rowMeta = new RowMeta();
    ValueMetaBase valueMeta = new ValueMetaString( "AccExtId" );
    rowMeta.addValueMeta( valueMeta );
    data.inputRowMeta = rowMeta;

    sfInputTransform.writeToSalesForce( new Object[] { null } );
    assertEquals( 1, data.sfBuffer[0].getFieldsToNull().length );
    assertEquals( ACCOUNT_ID, data.sfBuffer[0].getFieldsToNull()[0] );
    assertNull( SalesforceConnection.getChildren( data.sfBuffer[0] ) );
  }

  @Test
  public void testWriteToSalesForceForNotNullExtIdField_WithExtIdNO() throws Exception {
    SalesforceInsertMeta meta = generateSalesforceInsertMeta( new String[] { ACCOUNT_ID }, new Boolean[] { false } );
    SalesforceInsertData data = generateSalesforceInsertData();
    SalesforceInsert sfInputTransform = new SalesforceInsert( smh.transformMeta, meta, data, 0, smh.pipelineMeta, smh.pipeline );
    sfInputTransform.init();

    RowMeta rowMeta = new RowMeta();
    ValueMetaBase valueMeta = new ValueMetaString( "AccNoExtId" );
    rowMeta.addValueMeta( valueMeta );
    data.inputRowMeta = rowMeta;

    sfInputTransform.writeToSalesForce( new Object[] { "001i000001c5Nv9AAE" } );
    assertEquals( 0, data.sfBuffer[0].getFieldsToNull().length );
    assertEquals( 1, SalesforceConnection.getChildren( data.sfBuffer[0] ).length );
    assertEquals( Constants.PARTNER_SOBJECT_NS,
      SalesforceConnection.getChildren( data.sfBuffer[0] )[0].getName().getNamespaceURI() );
    assertEquals( ACCOUNT_ID, SalesforceConnection.getChildren( data.sfBuffer[0] )[0].getName().getLocalPart() );
    assertEquals( "001i000001c5Nv9AAE", SalesforceConnection.getChildren( data.sfBuffer[0] )[0].getValue() );
    assertFalse( SalesforceConnection.getChildren( data.sfBuffer[0] )[0].hasChildren() );
  }

  @Test
  public void testWriteToSalesForceForNotNullExtIdField_WithExtIdYES() throws Exception {
    SalesforceInsertMeta meta = generateSalesforceInsertMeta( new String[] { ACCOUNT_EXT_ID_ACCOUNT_ID_C_ACCOUNT }, new Boolean[] { true } );
    SalesforceInsertData data = generateSalesforceInsertData();
    SalesforceInsert sfInputTransform = new SalesforceInsert( smh.transformMeta, meta, data, 0, smh.pipelineMeta, smh.pipeline );
    sfInputTransform.init( );

    RowMeta rowMeta = new RowMeta();
    ValueMetaBase valueMeta = new ValueMetaString( "AccExtId" );
    rowMeta.addValueMeta( valueMeta );
    data.inputRowMeta = rowMeta;

    sfInputTransform.writeToSalesForce( new Object[] { "tkas88" } );
    assertEquals( 0, data.sfBuffer[0].getFieldsToNull().length );
    assertEquals( 1, SalesforceConnection.getChildren( data.sfBuffer[0] ).length );
    assertEquals( Constants.PARTNER_SOBJECT_NS,
      SalesforceConnection.getChildren( data.sfBuffer[0] )[0].getName().getNamespaceURI() );
    assertEquals( "Account", SalesforceConnection.getChildren( data.sfBuffer[0] )[0].getName().getLocalPart() );
    assertNull( SalesforceConnection.getChildren( data.sfBuffer[0] )[0].getValue() );
    assertFalse( SalesforceConnection.getChildren( data.sfBuffer[0] )[0].hasChildren() );
  }

  @Test
  public void testLogMessageInDetailedModeFotWriteToSalesForce() throws HopException {
    SalesforceInsertMeta meta = generateSalesforceInsertMeta( new String[] { ACCOUNT_ID }, new Boolean[] { false } );
    SalesforceInsertData data = generateSalesforceInsertData();
    SalesforceInsert sfInputTransform = new SalesforceInsert( smh.transformMeta, meta, data, 0, smh.pipelineMeta, smh.pipeline );
    sfInputTransform.init( );
    when( sfInputTransform.getLogChannel().isDetailed() ).thenReturn( true );

    RowMeta rowMeta = new RowMeta();
    ValueMetaBase valueMeta = new ValueMetaString( "AccNoExtId" );
    rowMeta.addValueMeta( valueMeta );
    data.inputRowMeta = rowMeta;

    verify( sfInputTransform.getLogChannel(), never() ).logDetailed( anyString() );
    sfInputTransform.writeToSalesForce( new Object[] { "001i000001c5Nv9AAE" } );
    verify( sfInputTransform.getLogChannel() ).logDetailed( "Called writeToSalesForce with 0 out of 2" );
  }

  private SalesforceInsertData generateSalesforceInsertData() {
    SalesforceInsertData data = smh.iTransformData;
    data.nrFields = 1;
    data.fieldnrs = new int[] { 0 };
    data.sfBuffer = new SObject[] { null };
    data.outputBuffer = new Object[][] { null };
    return data;
  }

  private SalesforceInsertMeta generateSalesforceInsertMeta( String[] updateLookup, Boolean[] useExternalId ) {
    SalesforceInsertMeta meta = smh.iTransformMeta;
    doReturn( 2 ).when( meta ).getBatchSizeInt();
    doReturn( updateLookup ).when( meta ).getUpdateLookup();
    doReturn( useExternalId ).when( meta ).getUseExternalId();
    doReturn( UUID.randomUUID().toString() ).when( meta ).getTargetUrl();
    doReturn( UUID.randomUUID().toString() ).when( meta ).getUsername();
    doReturn( UUID.randomUUID().toString() ).when( meta ).getPassword();
    doReturn( UUID.randomUUID().toString() ).when( meta ).getModule();
    doReturn( UUID.randomUUID().toString() ).when( meta ).getSalesforceIDFieldName();
    return meta;
  }

  @Test
  public void testWriteToSalesForcePentahoIntegerValue() throws Exception {
    SalesforceInsertMeta meta = generateSalesforceInsertMeta( new String[] { ACCOUNT_ID }, new Boolean[] { false } );
    SalesforceInsertData data = generateSalesforceInsertData();
    SalesforceInsert sfInputTransform = new SalesforceInsert( smh.transformMeta, meta, data, 0, smh.pipelineMeta, smh.pipeline );
    sfInputTransform.init();

    RowMeta rowMeta = new RowMeta();
    ValueMetaBase valueMeta = new ValueMetaInteger( "IntValue" );
    rowMeta.addValueMeta( valueMeta );
    data.inputRowMeta = rowMeta;

    sfInputTransform.writeToSalesForce( new Object[] { 1L } );
    XmlObject sObject = data.sfBuffer[ 0 ].getChild( ACCOUNT_ID );
    Assert.assertEquals( sObject.getValue(), 1 );
  }
}
