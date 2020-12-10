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
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
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

import java.lang.reflect.Method;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.UUID;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

/**
 * Tests for SalesforceInsert transform
 *
 * @author Pavel Sakun
 * @see SalesforceInsert
 */
public class SalesForceDateFieldTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();
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
  public void init() {
    smh = new TransformMockHelper<>( "SalesforceInsert", SalesforceInsertMeta.class, SalesforceInsertData.class );
    when( smh.logChannelFactory.create( any(), any( ILoggingObject.class ) ) ).thenReturn(
        smh.iLogChannel );
  }

  @After
  public void cleanUp() {
    smh.cleanUp();
  }

  @Test
  public void testDateInsert() throws Exception {

    SalesforceInsertMeta meta = smh.iTransformMeta;
    doReturn( UUID.randomUUID().toString() ).when( meta ).getTargetUrl();
    doReturn( UUID.randomUUID().toString() ).when( meta ).getUsername();
    doReturn( UUID.randomUUID().toString() ).when( meta ).getPassword();
    doReturn( UUID.randomUUID().toString() ).when( meta ).getModule();
    doReturn( 2 ).when( meta ).getBatchSizeInt();
    doReturn( new String[] { "Date" } ).when( meta ).getUpdateLookup();
    doReturn( new Boolean[] {false}  ).when( meta ).getUseExternalId();

    SalesforceInsertData data = smh.iTransformData;
    data.nrFields = 1;
    data.fieldnrs = new int[] { 0 };
    data.sfBuffer = new SObject[]{ null };
    data.outputBuffer = new Object[][]{ null };

    SalesforceInsert transform = new SalesforceInsert( smh.transformMeta,
      smh.iTransformMeta, smh.iTransformData, 0, smh.pipelineMeta, smh.pipeline );

    transform.init();

    RowMeta rowMeta = new RowMeta();
    IValueMeta valueMeta = new ValueMetaDate( "date" );
    valueMeta.setDateFormatTimeZone( TimeZone.getTimeZone( "Europe/Minsk" ) );
    rowMeta.addValueMeta( valueMeta );
    smh.iTransformData.inputRowMeta = rowMeta;

    Calendar minskTime = Calendar.getInstance( valueMeta.getDateFormatTimeZone() );
    minskTime.clear();
    minskTime.set( 2013, Calendar.OCTOBER, 16 );

    Object[] args = new Object[] { minskTime.getTime() };

    Method m = SalesforceInsert.class.getDeclaredMethod( "writeToSalesForce", Object[].class );
    m.setAccessible( true );
    m.invoke( transform, new Object[] { args } );

    DateFormat utc = new SimpleDateFormat( "yyyy-MM-dd" );
    utc.setTimeZone( TimeZone.getTimeZone( "UTC" ) );

    XmlObject xmlObject = SalesforceConnection.getChildren( data.sfBuffer[ 0 ] )[ 0 ];
    Assert.assertEquals( "2013-10-16",
      utc.format( ( (Calendar) xmlObject.getValue() ).getTime() ) );
  }
}
