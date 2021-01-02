/*!
 * Copyright 2018 Hitachi Vantara.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hop.pipeline.transforms.cassandrasstableoutput;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.pipeline.transforms.steps.mock.StepMockHelper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SSTableOutputTest {
  private static StepMockHelper<SSTableOutputMeta, SSTableOutputData> helper;
  private static final SecurityManager sm = System.getSecurityManager();

  @BeforeClass
  public static void setUp() throws HopException {
    //KettleEnvironment.init();
    helper =
      new StepMockHelper<SSTableOutputMeta, SSTableOutputData>( "SSTableOutputIT", SSTableOutputMeta.class,
        SSTableOutputData.class );
    when( helper.logChannelInterfaceFactory.create( any(), any( ILoggingObject.class ) ) ).thenReturn(
      helper.logChannelInterface );
    when( helper.trans.isRunning() ).thenReturn( true );
  }

  @AfterClass
  public static void classTearDown() {
    //Cleanup class setup
    helper.cleanUp();
  }

  @After
  public void tearDown() throws Exception {
    // Restore original security manager if needed
    if ( System.getSecurityManager() != sm ) {
      System.setSecurityManager( sm );
    }
  }

  @Test( expected = SecurityException.class )
  public void testDisableSystemExit() throws Exception {
    SSTableOutput ssTableOutput =
      new SSTableOutput( helper.stepMeta, null, helper.stepDataInterface, 0, helper.transMeta, helper.trans );
    ssTableOutput.disableSystemExit( sm, helper.logChannelInterface );
    System.exit( 1 );
  }
}
