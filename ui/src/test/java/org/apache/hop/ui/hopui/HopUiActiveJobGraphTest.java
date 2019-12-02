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

package org.apache.hop.ui.hopui;

import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;

import org.apache.hop.ui.hopui.delegates.HopUiTabsDelegate;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.apache.hop.ui.hopui.delegates.HopUiDelegates;
import org.apache.hop.ui.hopui.job.JobGraph;
import org.apache.xul.swt.tab.TabItem;
import org.apache.xul.swt.tab.TabSet;

public class HopUiActiveJobGraphTest {

  private HopUi hopUi;

  @Before
  public void setUp() throws Exception {
    hopUi = mock( HopUi.class );
    hopUi.delegates = mock( HopUiDelegates.class );
    hopUi.delegates.tabs = mock( HopUiTabsDelegate.class );
    hopUi.tabfolder = mock( TabSet.class );

    doCallRealMethod().when( hopUi ).getActiveJobGraph();
  }

  @Test
  public void returnNullActiveJobGraphIfJobTabNotExists() {
    JobGraph actualJobGraph = hopUi.getActiveJobGraph();
    assertNull( actualJobGraph );
  }

  @Test
  public void returnActiveJobGraphIfJobTabExists() {
    TabMapEntry tabMapEntry = mock( TabMapEntry.class );
    JobGraph jobGraph = mock( JobGraph.class );
    Mockito.when( tabMapEntry.getObject() ).thenReturn( jobGraph );
    Mockito.when( hopUi.delegates.tabs.getTab( Mockito.any( TabItem.class ) ) ).thenReturn( tabMapEntry );

    JobGraph actualJobGraph = hopUi.getActiveJobGraph();
    assertNotNull( actualJobGraph );
  }

}
