/*! ******************************************************************************
 *
 * Pentaho Data Integration
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

package org.apache.hop.pipeline.transforms.mailinput;

import org.apache.hop.core.logging.LoggingObjectInterface;
import org.apache.hop.workflow.actions.getpop.MailConnectionMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MailInputTest {

  private TransformMockHelper<MailInputMeta, MailInputData> mockHelper;

  @Before
  public void setUp() throws Exception {
    mockHelper =
      new TransformMockHelper<MailInputMeta, MailInputData>( "MailInput", MailInputMeta.class, MailInputData.class );
    when( mockHelper.logChannelFactory.create( any(), any( LoggingObjectInterface.class ) ) ).thenReturn(
      mockHelper.logChannelInterface );
    when( mockHelper.pipeline.isRunning() ).thenReturn( true );
  }

  @After
  public void cleanUp() {
    mockHelper.cleanUp();
  }

  /**
   * PDI-10909 Check that imap retrieve ... first will be applied.
   */
  @Test
  public void testInitSetGetFirstForIMAP() {
    MailInput transform =
      new MailInput( mockHelper.transformMeta, mockHelper.iTransformData, 0, mockHelper.pipelineMeta, mockHelper.pipeline );
    MailInputData data = new MailInputData();
    MailInputMeta meta = mock( MailInputMeta.class );
    when( meta.isDynamicFolder() ).thenReturn( false );
    when( meta.getProtocol() ).thenReturn( MailConnectionMeta.PROTOCOL_STRING_IMAP );
    when( meta.getFirstIMAPMails() ).thenReturn( "2" );
    when( meta.getFirstMails() ).thenReturn( "3" );

    transform.init();

    Assert.assertEquals( "Row Limit is set up to 2 rows.", 2, data.rowlimit );
  }

  /**
   * PDI-10909 Check that pop3 retrieve ... first will be applied.
   */
  @Test
  public void testInitSetGetFirstForPOP3() {
    MailInput transform =
      new MailInput( mockHelper.transformMeta, mockHelper.iTransformData, 0, mockHelper.pipelineMeta, mockHelper.pipeline );
    MailInputData data = new MailInputData();
    MailInputMeta meta = mock( MailInputMeta.class );
    when( meta.isDynamicFolder() ).thenReturn( false );
    when( meta.getProtocol() ).thenReturn( MailConnectionMeta.PROTOCOL_STRING_POP3 );
    when( meta.getFirstIMAPMails() ).thenReturn( "2" );
    when( meta.getFirstMails() ).thenReturn( "3" );

    transform.init();

    Assert.assertEquals( "Row Limit is set up to 3 rows.", 3, data.rowlimit );
  }

  /**
   * PDI-10909 Check that Limit value overrides retrieve ... first if any.
   */
  @Test
  public void testInitSetGetFirstLimitOverride() {
    MailInput transform =
      new MailInput( mockHelper.transformMeta, mockHelper.iTransformData, 0, mockHelper.pipelineMeta, mockHelper.pipeline );
    MailInputData data = new MailInputData();
    MailInputMeta meta = mock( MailInputMeta.class );
    when( meta.isDynamicFolder() ).thenReturn( false );
    when( meta.getProtocol() ).thenReturn( MailConnectionMeta.PROTOCOL_STRING_POP3 );
    when( meta.getFirstIMAPMails() ).thenReturn( "2" );
    when( meta.getFirstMails() ).thenReturn( "3" );

    when( meta.getRowLimit() ).thenReturn( "5" );

    transform.init();

    Assert.assertEquals( "Row Limit is set up to 5 rows as the Limit has priority.", 5, data.rowlimit );
  }

  /**
   * We do not use any of retrieve ... first if protocol is MBOX
   */
  @Test
  public void testInitSetGetFirstForMBOXIgnored() {
    MailInput transform =
      new MailInput( mockHelper.transformMeta, mockHelper.iTransformData, 0, mockHelper.pipelineMeta, mockHelper.pipeline );
    MailInputData data = new MailInputData();
    MailInputMeta meta = mock( MailInputMeta.class );
    when( meta.isDynamicFolder() ).thenReturn( false );
    when( meta.getProtocol() ).thenReturn( MailConnectionMeta.PROTOCOL_STRING_MBOX );
    when( meta.getFirstIMAPMails() ).thenReturn( "2" );
    when( meta.getFirstMails() ).thenReturn( "3" );

    transform.init();

    Assert.assertEquals( "Row Limit is set up to 0 rows as the Limit has priority.", 0, data.rowlimit );
  }

}
