/*! ******************************************************************************
 *
 * Pentaho Data Integration
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
package org.apache.hop.trans.steps.terafast;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LoggingObjectInterface;
import org.apache.hop.core.util.GenericStepData;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.trans.steps.mock.StepMockHelper;

import static org.junit.Assert.assertThat;
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
  private StepMockHelper<TeraFastMeta, GenericStepData> stepMockHelper;
  private TeraFast teraFast;

  @BeforeClass
  public static void initEnvironment() throws HopException {
    HopEnvironment.init();
  }

  @Before
  public void setUp() {
    stepMockHelper = new StepMockHelper<>( "TeraFast", TeraFastMeta.class, GenericStepData.class );
    when( stepMockHelper.logChannelInterfaceFactory.create( any(), any( LoggingObjectInterface.class ) ) ).thenReturn( stepMockHelper.logChannelInterface );
    when( stepMockHelper.trans.isRunning() ).thenReturn( true );
    teraFast = new TeraFast( stepMockHelper.stepMeta, stepMockHelper.stepDataInterface, 0, stepMockHelper.transMeta, stepMockHelper.trans );
  }

  @After
  public void tearDown() throws Exception {
    stepMockHelper.cleanUp();
  }

  @Test
  public void testNullDataFilePrintStream() throws HopException {
    TeraFast teraFastDataFilePrintStreamIsNull = mock( TeraFast.class );
    doReturn( null ).when( teraFastDataFilePrintStreamIsNull ).getRow();
    TeraFastMeta meta = mock( TeraFastMeta.class );
    GenericStepData data = mock( GenericStepData.class );
    assertFalse( teraFastDataFilePrintStreamIsNull.processRow( meta, data ) );
  }

  /**
   * [PDI-17481] Testing the ability that if no connection is specified, we will mark it as a fail and log the
   * appropriate reason to the user by throwing a HopException.
   */
  @Test
  public void testNoDatabaseConnection() {
    try {
      doReturn( null ).when( stepMockHelper.initStepMetaInterface ).getDbMeta();
      assertFalse( teraFast.init( stepMockHelper.initStepMetaInterface, stepMockHelper.initStepDataInterface ) );
      // Verify that the database connection being set to null throws a HopException with the following message.
      teraFast.verifyDatabaseConnection();
      // If the method does not throw a Hop Exception, then the DB was set and not null for this test. Fail it.
      fail( "Database Connection is not null, this fails the test." );
    } catch ( HopException aHopException ) {
      assertTrue( aHopException.getMessage().contains( "There is no connection defined in this step." ) );
    }
  }
}
