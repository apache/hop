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
package org.apache.hop.job.entries.http;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.never;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.repository.ObjectId;
import org.apache.hop.repository.kdr.HopDatabaseRepository;

/**
 * @author Tatsiana_Kasiankova
 *
 */
public class JobEntryHTTPTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  private JobEntryHTTP jobEntryHttp = new JobEntryHTTP();
  private HopDatabaseRepository ktlDbRepMock = mock( HopDatabaseRepository.class );
  private ObjectId objIdMock = mock( ObjectId.class );

  @BeforeClass
  public static void beforeClass() throws HopException {
    PluginRegistry.addPluginType( TwoWayPasswordEncoderPluginType.getInstance() );
    PluginRegistry.init();
    String passwordEncoderPluginID =
        Const.NVL( EnvUtil.getSystemProperty( Const.HOP_PASSWORD_ENCODER_PLUGIN ), "Hop" );
    Encr.init( passwordEncoderPluginID );
  }

  @Test
  public void testDateTimeAddedFieldIsSetInTrue_WhenRepoReturnsTrue() throws HopException {
    when( ktlDbRepMock.getJobEntryAttributeBoolean( objIdMock, "date_time_added" ) ).thenReturn( true );

    jobEntryHttp.loadRep( ktlDbRepMock, ktlDbRepMock.getMetaStore(), objIdMock, null, null );
    verify( ktlDbRepMock, never() ).getJobEntryAttributeString( objIdMock, "date_time_added" );
    verify( ktlDbRepMock ).getJobEntryAttributeBoolean( objIdMock, "date_time_added" );
    assertTrue( "DateTimeAdded field should be TRUE.", jobEntryHttp.isDateTimeAdded() );

  }

  @SuppressWarnings( "deprecation" )
  @Test
  public void testDeprecatedTargetFilenameExtension() {
    jobEntryHttp.setTargetFilenameExtention( "txt" );
    assertTrue( "txt".equals( jobEntryHttp.getTargetFilenameExtension() ) );
    jobEntryHttp.setTargetFilenameExtension( "zip" );
    assertTrue( "zip".equals( jobEntryHttp.getTargetFilenameExtention() ) );
  }
}
