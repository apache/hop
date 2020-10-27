/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2018-2019 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *vg
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
package org.apache.hop.pipeline.transforms.terafast;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LoggingObjectInterface;
import org.apache.hop.core.util.GenericTransformData;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * User: dgriffen Date: 12/04/2018
 */
public class TeraFastTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();
  private TransformMockHelper<TeraFastMeta, GenericTransformData> transformMockHelper;
  private TeraFast teraFast;

  @BeforeClass
  public static void initEnvironment() throws HopException {
    HopEnvironment.init();
  }

  @Before
  public void setUp() {
    transformMockHelper = new TransformMockHelper<>( "TeraFast", TeraFastMeta.class, GenericTransformData.class );
    when( transformMockHelper.logChannelFactory.create( any(), any( LoggingObjectInterface.class ) ) ).thenReturn( transformMockHelper.logChannelInterface );
    when( transformMockHelper.pipeline.isRunning() ).thenReturn( true );
    teraFast = new TeraFast( transformMockHelper.transformMeta, transformMockHelper.iTransformMeta, transformMockHelper.iTransformData, 0, transformMockHelper.pipelineMeta, transformMockHelper.pipeline );
  }

  @After
  public void tearDown() throws Exception {
    transformMockHelper.cleanUp();
  }

  @Test
  public void testNullDataFilePrintStream() throws HopException {
    TeraFast teraFastDataFilePrintStreamIsNull = mock( TeraFast.class );
    doReturn( null ).when( teraFastDataFilePrintStreamIsNull ).getRow();
    TeraFastMeta meta = mock( TeraFastMeta.class );
    GenericTransformData data = mock( GenericTransformData.class );
    assertFalse( teraFastDataFilePrintStreamIsNull.init();
  }

  /**
   * [PDI-17481] Testing the ability that if no connection is specified, we will mark it as a fail and log the
   * appropriate reason to the user by throwing a HopException.
   */
  @Test
  public void testNoDatabaseConnection() {
    try {
      doReturn( null ).when( transformMockHelper.iTransformMeta ).getDbMeta();
      assertFalse( teraFast.init() );
      // Verify that the database connection being set to null throws a HopException with the following message.
      teraFast.verifyDatabaseConnection();
      // If the method does not throw a Hop Exception, then the DB was set and not null for this test. Fail it.
      fail( "Database Connection is not null, this fails the test." );
    } catch ( HopException aHopException ) {
      assertTrue( aHopException.getMessage().contains( "There is no connection defined in this transform." ) );
    }
  }
}
