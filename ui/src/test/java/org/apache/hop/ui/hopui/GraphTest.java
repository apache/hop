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

import org.apache.hop.core.gui.GUIPositionInterface;
import org.apache.hop.job.JobMeta;
import org.apache.hop.job.entry.JobEntryCopy;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.ui.hopui.job.JobGraph;
import org.apache.hop.ui.hopui.trans.TransGraph;
import org.junit.Test;
import org.mockito.AdditionalMatchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GraphTest {

  @Test
  public void testRightClickStepSelection() {
    TransGraph graph = mock( TransGraph.class );

    StepMeta meta1 = mock( StepMeta.class );
    StepMeta meta2 = mock( StepMeta.class );
    StepMeta meta3 = mock( StepMeta.class );
    wireSelected( meta1, meta2, meta3 );

    List<StepMeta> selected = new ArrayList<>( 2 );
    meta2.setSelected( true );
    meta3.setSelected( true );
    selected.add( meta2 );
    selected.add( meta3 );

    doCallRealMethod().when( graph ).doRightClickSelection( meta1, selected );
    graph.doRightClickSelection( meta1, selected );

    assertTrue( meta1.isSelected() );
    assertEquals( meta1, selected.get( 0 ) );
    assertEquals( 1, selected.size() );
    assertFalse( meta2.isSelected() || meta3.isSelected() );
  }


  @Test
  public void testRightClickAlreadySelected() {
    TransGraph graph = mock( TransGraph.class );
    StepMeta meta1 = mock( StepMeta.class );
    StepMeta meta2 = mock( StepMeta.class );
    wireSelected( meta1, meta2 );
    List<StepMeta> selected = new ArrayList<>( 2 );
    meta1.setSelected( true );
    meta2.setSelected( true );
    selected.add( meta1 );
    selected.add( meta2 );

    doCallRealMethod().when( graph ).doRightClickSelection( meta1, selected );
    graph.doRightClickSelection( meta1, selected );

    assertEquals( 2, selected.size() );
    assertTrue( selected.contains( meta1 ) );
    assertTrue( selected.contains( meta2 ) );
    assertTrue( meta1.isSelected() && meta2.isSelected() );
  }

  @Test
  public void testRightClickNoSelection() {
    TransGraph graph = mock( TransGraph.class );
    StepMeta meta1 = mock( StepMeta.class );
    wireSelected( meta1 );
    List<StepMeta> selected = new ArrayList<>();

    doCallRealMethod().when( graph ).doRightClickSelection( meta1, selected );
    graph.doRightClickSelection( meta1, selected );

    assertEquals( 1, selected.size() );
    assertTrue( selected.contains( meta1 ) );
    assertTrue( meta1.isSelected() );
  }

  @Test
  public void testDelJobNoSelections() {
    JobMeta jobMeta = mock( JobMeta.class );
    HopUi hopUi = mock( HopUi.class );
    when( jobMeta.getSelectedEntries() ).thenReturn( Collections.<JobEntryCopy>emptyList() );
    JobEntryCopy je = mock( JobEntryCopy.class );

    JobGraph jobGraph = mock( JobGraph.class );
    doCallRealMethod().when( jobGraph ).setJobMeta( any( JobMeta.class ) );
    doCallRealMethod().when( jobGraph ).setHopUi( any( HopUi.class ) );
    doCallRealMethod().when( jobGraph ).delSelected( any( JobEntryCopy.class ) );
    jobGraph.setJobMeta( jobMeta );
    jobGraph.setHopUi( hopUi );

    jobGraph.delSelected( je );
    verify( hopUi ).deleteJobEntryCopies( jobMeta, je );
  }

  @Test
  public void testDelSelectionsJob() {
    JobMeta jobMeta = mock( JobMeta.class );
    HopUi hopUi = mock( HopUi.class );
    JobEntryCopy selected1 = mock( JobEntryCopy.class );
    JobEntryCopy selected2 = mock( JobEntryCopy.class );
    when( jobMeta.getSelectedEntries() ).thenReturn( Arrays.asList( selected1, selected2 ) );

    JobGraph jobGraph = mock( JobGraph.class );
    doCallRealMethod().when( jobGraph ).setJobMeta( any( JobMeta.class ) );
    doCallRealMethod().when( jobGraph ).setHopUi( any( HopUi.class ) );
    doCallRealMethod().when( jobGraph ).delSelected( any( JobEntryCopy.class ) );
    jobGraph.setJobMeta( jobMeta );
    jobGraph.setHopUi( hopUi );

    jobGraph.delSelected( null );
    verify( hopUi ).deleteJobEntryCopies( eq( jobMeta ),
      AdditionalMatchers.aryEq( new JobEntryCopy[] { selected1, selected2 } ) );
  }

  private boolean[] wireSelected( GUIPositionInterface... mockedElements ) {
    final boolean[] selections = new boolean[ mockedElements.length ];
    Arrays.fill( selections, false );
    for ( int i = 0; i < mockedElements.length; i++ ) {
      final int j = i;
      when( mockedElements[ i ].isSelected() ).then( new Answer<Boolean>() {
        public Boolean answer( InvocationOnMock invocation ) throws Throwable {
          return selections[ j ];
        }
      } );
      doAnswer( new Answer<Void>() {
        public Void answer( InvocationOnMock invocation ) throws Throwable {
          selections[ j ] = (boolean) invocation.getArguments()[ 0 ];
          return null;
        }
      } ).when( mockedElements[ i ] ).setSelected( any( Boolean.class ) );
    }
    return selections;
  }

}
