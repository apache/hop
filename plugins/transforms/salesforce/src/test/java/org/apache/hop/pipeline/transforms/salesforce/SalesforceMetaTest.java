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

package org.apache.hop.pipeline.transforms.salesforce;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class SalesforceMetaTest {
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

  public static List<String> getDefaultAttributes() {
    return Arrays.asList( "targetUrl", "username", "password", "timeout", "compression", "module" );
  }

  @SuppressWarnings( "deprecation" )
  @Test
  public void testBaseCheck() {
    SalesforceTransformMeta meta = mock( SalesforceTransformMeta.class, Mockito.CALLS_REAL_METHODS );
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
    meta.setUsername( "anonymous" );
    meta.check( remarks, null, null, null, null, null, null, null, null );
    hasError = false;
    for ( ICheckResult cr : remarks ) {
      if ( cr.getType() == CheckResult.TYPE_RESULT_ERROR ) {
        hasError = true;
      }
    }
    assertFalse( remarks.isEmpty() );
    assertFalse( hasError );
    remarks.clear();

    meta.setDefault();
    meta.setTargetUrl( null );
    meta.setUserName( "anonymous" );
    meta.setPassword( "password" );
    meta.check( remarks, null, null, null, null, null, null, null, null );
    hasError = false;
    for ( ICheckResult cr : remarks ) {
      if ( cr.getType() == CheckResult.TYPE_RESULT_ERROR ) {
        hasError = true;
      }
    }
    assertFalse( remarks.isEmpty() );
    assertTrue( hasError );
    remarks.clear();

    meta.setDefault();
    meta.setUsername( "anonymous" );
    meta.setModule( null );
    meta.check( remarks, null, null, null, null, null, null, null, null );
    hasError = false;
    for ( ICheckResult cr : remarks ) {
      if ( cr.getType() == CheckResult.TYPE_RESULT_ERROR ) {
        hasError = true;
      }
    }
    assertFalse( remarks.isEmpty() );
    assertTrue( hasError );
  }
}
