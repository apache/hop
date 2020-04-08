/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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

package org.apache.hop.workflow.actions.ftpsget;

import org.apache.hop.core.logging.HopLogStore;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

public class WorkflowEntryFTPSGetTest {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    HopLogStore.init();
  }

  /**
   * PDI-6868, attempt to set binary mode is after the connection.connect() succeeded.
   *
   * @throws Exception
   */
  @Test
  public void testBinaryModeSetAfterConnectionSuccess() throws Exception {
    ActionFtpsGet workflow = new ActionFtpsGetCustom();
    FtpsConnection connection = Mockito.mock( FtpsConnection.class );
    InOrder inOrder = Mockito.inOrder( connection );
    workflow.buildFTPSConnection( connection );
    inOrder.verify( connection ).connect();
    inOrder.verify( connection ).setBinaryMode( Mockito.anyBoolean() );
  }

  class ActionFtpsGetCustom extends ActionFtpsGet {
    @Override
    public boolean isBinaryMode() {
      return true;
    }
  }
}
