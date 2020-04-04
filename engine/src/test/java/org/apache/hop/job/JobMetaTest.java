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

package org.apache.hop.job;

import org.apache.hop.core.Const;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.listeners.IContentChangedListener;
import org.apache.hop.job.entries.empty.JobEntryEmpty;
import org.apache.hop.job.entry.IJobEntry;
import org.apache.hop.job.entry.JobEntryCopy;
import org.apache.hop.metastore.api.IMetaStore;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.IOException;
import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class JobMetaTest {

  private static final String JOB_META_NAME = "jobName";

  private JobMeta jobMeta;
  private IContentChangedListener listener;

  @Before
  public void setUp() {
    jobMeta = new JobMeta();
    // prepare
    listener = mock( IContentChangedListener.class );
    jobMeta.addContentChangedListener( listener );
    jobMeta.setName( JOB_META_NAME );
  }

  @Test
  public void testPathExist() throws HopXMLException, IOException, URISyntaxException {
    assertTrue( testPath( "je1-je4" ) );
  }

  @Test
  public void testPathNotExist() throws HopXMLException, IOException, URISyntaxException {
    assertFalse( testPath( "je2-je4" ) );
  }

  private boolean testPath( String branch ) {
    JobEntryEmpty je1 = new JobEntryEmpty();
    je1.setName( "je1" );

    JobEntryEmpty je2 = new JobEntryEmpty();
    je2.setName( "je2" );

    JobHopMeta hop = new JobHopMeta( new JobEntryCopy( je1 ), new JobEntryCopy( je2 ) );
    jobMeta.addJobHop( hop );

    JobEntryEmpty je3 = new JobEntryEmpty();
    je3.setName( "je3" );
    hop = new JobHopMeta( new JobEntryCopy( je1 ), new JobEntryCopy( je3 ) );
    jobMeta.addJobHop( hop );

    JobEntryEmpty je4 = new JobEntryEmpty();
    je4.setName( "je4" );
    hop = new JobHopMeta( new JobEntryCopy( je3 ), new JobEntryCopy( je4 ) );
    jobMeta.addJobHop( hop );

    if ( branch.equals( "je1-je4" ) ) {
      return jobMeta.isPathExist( je1, je4 );
    } else if ( branch.equals( "je2-je4" ) ) {
      return jobMeta.isPathExist( je2, je4 );
    } else {
      return false;
    }
  }

  @Test
  public void testContentChangeListener() throws Exception {
    jobMeta.setChanged();
    jobMeta.setChanged( true );

    verify( listener, times( 2 ) ).contentChanged( same( jobMeta ) );

    jobMeta.clearChanged();
    jobMeta.setChanged( false );

    verify( listener, times( 2 ) ).contentSafe( same( jobMeta ) );

    jobMeta.removeContentChangedListener( listener );
    jobMeta.setChanged();
    jobMeta.setChanged( true );

    verifyNoMoreInteractions( listener );
  }

  @Test
  public void shouldUseCoordinatesOfItsTransformsAndNotesWhenCalculatingMinimumPoint() {
    Point jobEntryPoint = new Point( 500, 500 );
    Point notePadMetaPoint = new Point( 400, 400 );
    JobEntryCopy jobEntryCopy = mock( JobEntryCopy.class );
    when( jobEntryCopy.getLocation() ).thenReturn( jobEntryPoint );
    NotePadMeta notePadMeta = mock( NotePadMeta.class );
    when( notePadMeta.getLocation() ).thenReturn( notePadMetaPoint );

    // empty Job return 0 coordinate point
    Point point = jobMeta.getMinimum();
    assertEquals( 0, point.x );
    assertEquals( 0, point.y );

    // when Job contains a single transform or note, then jobMeta should return coordinates of it, subtracting borders
    jobMeta.addJobEntry( 0, jobEntryCopy );
    Point actualTransformPoint = jobMeta.getMinimum();
    assertEquals( jobEntryPoint.x - JobMeta.BORDER_INDENT, actualTransformPoint.x );
    assertEquals( jobEntryPoint.y - JobMeta.BORDER_INDENT, actualTransformPoint.y );

    // when Job contains transform or notes, then jobMeta should return minimal coordinates of them, subtracting borders
    jobMeta.addNote( notePadMeta );
    Point transformPoint = jobMeta.getMinimum();
    assertEquals( notePadMetaPoint.x - JobMeta.BORDER_INDENT, transformPoint.x );
    assertEquals( notePadMetaPoint.y - JobMeta.BORDER_INDENT, transformPoint.y );
  }

  @Test
  public void testEquals_oneNameNull() {
    assertFalse( testEquals( null, null ) );
  }

  @Test
  public void testEquals_secondNameNull() {
    jobMeta.setName( null );
    assertFalse( testEquals( JOB_META_NAME, null ) );
  }

  @Test
  public void testEquals_sameFilename() {
    String newFilename = "Filename";
    jobMeta.setFilename( newFilename );
    assertFalse( testEquals( null, newFilename ) );
  }

  @Test
  public void testEquals_difFilenameSameName() {
    jobMeta.setFilename( "Filename" );
    assertFalse( testEquals( JOB_META_NAME, "OtherFileName" ) );
  }

  @Test
  public void testEquals_sameFilenameSameName() {
    String newFilename = "Filename";
    jobMeta.setFilename( newFilename );
    assertTrue( testEquals( JOB_META_NAME, newFilename ) );
  }

  @Test
  public void testEquals_sameFilenameDifName() {
    String newFilename = "Filename";
    jobMeta.setFilename( newFilename );
    assertFalse( testEquals( "OtherName", newFilename ) );
  }

  private boolean testEquals( String name, String filename ) {
    JobMeta jobMeta2 = new JobMeta();
    jobMeta2.setName( name );
    jobMeta2.setFilename( filename );
    return jobMeta.equals( jobMeta2 );
  }

  @Test
  public void testLoadXml() throws HopException {
    String directory = "/home/admin";
    Node jobNode = Mockito.mock( Node.class );
    NodeList nodeList = new NodeList() {
      Node node = Mockito.mock( Node.class );

      {
        Mockito.when( node.getNodeName() ).thenReturn( "directory" );
        Node child = Mockito.mock( Node.class );
        Mockito.when( node.getFirstChild() ).thenReturn( child );
        Mockito.when( child.getNodeValue() ).thenReturn( directory );
      }

      @Override public Node item( int index ) {
        return node;
      }

      @Override public int getLength() {
        return 1;
      }
    };

    Mockito.when( jobNode.getChildNodes() ).thenReturn( nodeList );

    JobMeta meta = new JobMeta();

    meta.loadXML( jobNode, null, Mockito.mock( IMetaStore.class ) );
    Job job = new Job( meta );
    job.setInternalHopVariables( null );
  }

  @Test
  public void testAddRemoveJobEntryCopySetUnsetParent() throws Exception {
    JobEntryCopy jobEntryCopy = mock( JobEntryCopy.class );
    jobMeta.addJobEntry( jobEntryCopy );
    jobMeta.removeJobEntry( 0 );
    verify( jobEntryCopy, times( 1 ) ).setParentJobMeta( jobMeta );
    verify( jobEntryCopy, times( 1 ) ).setParentJobMeta( null );
  }

  @Test
  public void testHasLoop_simpleLoop() throws Exception {
    //main->2->3->main
    JobMeta jobMetaSpy = spy( jobMeta );
    JobEntryCopy jobEntryCopyMain = createJobEntryCopy( "mainTransform" );
    JobEntryCopy jobEntryCopy2 = createJobEntryCopy( "transform2" );
    JobEntryCopy jobEntryCopy3 = createJobEntryCopy( "transform3" );
    when( jobMetaSpy.findNrPrevJobEntries( jobEntryCopyMain ) ).thenReturn( 1 );
    when( jobMetaSpy.findPrevJobEntry( jobEntryCopyMain, 0 ) ).thenReturn( jobEntryCopy2 );
    when( jobMetaSpy.findNrPrevJobEntries( jobEntryCopy2 ) ).thenReturn( 1 );
    when( jobMetaSpy.findPrevJobEntry( jobEntryCopy2, 0 ) ).thenReturn( jobEntryCopy3 );
    when( jobMetaSpy.findNrPrevJobEntries( jobEntryCopy3 ) ).thenReturn( 1 );
    when( jobMetaSpy.findPrevJobEntry( jobEntryCopy3, 0 ) ).thenReturn( jobEntryCopyMain );
    assertTrue( jobMetaSpy.hasLoop( jobEntryCopyMain ) );
  }

  @Test
  public void testHasLoop_loopInPrevTransforms() throws Exception {
    //main->2->3->4->3
    JobMeta jobMetaSpy = spy( jobMeta );
    JobEntryCopy jobEntryCopyMain = createJobEntryCopy( "mainTransform" );
    JobEntryCopy jobEntryCopy2 = createJobEntryCopy( "transform2" );
    JobEntryCopy jobEntryCopy3 = createJobEntryCopy( "transform3" );
    JobEntryCopy jobEntryCopy4 = createJobEntryCopy( "transform4" );
    when( jobMetaSpy.findNrPrevJobEntries( jobEntryCopyMain ) ).thenReturn( 1 );
    when( jobMetaSpy.findPrevJobEntry( jobEntryCopyMain, 0 ) ).thenReturn( jobEntryCopy2 );
    when( jobMetaSpy.findNrPrevJobEntries( jobEntryCopy2 ) ).thenReturn( 1 );
    when( jobMetaSpy.findPrevJobEntry( jobEntryCopy2, 0 ) ).thenReturn( jobEntryCopy3 );
    when( jobMetaSpy.findNrPrevJobEntries( jobEntryCopy3 ) ).thenReturn( 1 );
    when( jobMetaSpy.findPrevJobEntry( jobEntryCopy3, 0 ) ).thenReturn( jobEntryCopy4 );
    when( jobMetaSpy.findNrPrevJobEntries( jobEntryCopy4 ) ).thenReturn( 1 );
    when( jobMetaSpy.findPrevJobEntry( jobEntryCopy4, 0 ) ).thenReturn( jobEntryCopy3 );
    //check no StackOverflow error
    assertFalse( jobMetaSpy.hasLoop( jobEntryCopyMain ) );
  }

  private JobEntryCopy createJobEntryCopy( String name ) {
    IJobEntry jobEntry = mock( IJobEntry.class );
    JobEntryCopy jobEntryCopy = new JobEntryCopy( jobEntry );
    when( jobEntryCopy.getName() ).thenReturn( name );
    jobEntryCopy.setNr( 0 );
    return jobEntryCopy;
  }

  @Test
  public void testSetInternalEntryCurrentDirectoryWithFilename() {
    JobMeta jobMetaTest = new JobMeta();
    jobMetaTest.setFilename( "hasFilename" );
    jobMetaTest.setVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY, "Original value defined at run execution" );
    jobMetaTest.setVariable( Const.INTERNAL_VARIABLE_JOB_FILENAME_DIRECTORY, "file:///C:/SomeFilenameDirectory" );
    jobMetaTest.setInternalEntryCurrentDirectory();

    assertEquals( "file:///C:/SomeFilenameDirectory", jobMetaTest.getVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY ) );

  }

  @Test
  public void testSetInternalEntryCurrentDirectoryWithoutFilename() {
    JobMeta jobMetaTest = new JobMeta();
    jobMetaTest.setVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY, "Original value defined at run execution" );
    jobMetaTest.setVariable( Const.INTERNAL_VARIABLE_JOB_FILENAME_DIRECTORY, "file:///C:/SomeFilenameDirectory" );
    jobMetaTest.setInternalEntryCurrentDirectory();

    assertEquals( "Original value defined at run execution", jobMetaTest.getVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY ) );
  }

  @Test
  public void testUpdateCurrentDirWithFilename() {
    JobMeta jobMetaTest = new JobMeta();
    jobMetaTest.setFilename( "hasFilename" );
    jobMetaTest.setVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY, "Original value defined at run execution" );
    jobMetaTest.setVariable( Const.INTERNAL_VARIABLE_JOB_FILENAME_DIRECTORY, "file:///C:/SomeFilenameDirectory" );
    jobMetaTest.updateCurrentDir();

    assertEquals( "file:///C:/SomeFilenameDirectory", jobMetaTest.getVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY ) );

  }

  @Test
  public void testUpdateCurrentDirWithoutFilename() {
    JobMeta jobMetaTest = new JobMeta();
    jobMetaTest.setVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY, "Original value defined at run execution" );
    jobMetaTest.setVariable( Const.INTERNAL_VARIABLE_JOB_FILENAME_DIRECTORY, "file:///C:/SomeFilenameDirectory" );
    jobMetaTest.updateCurrentDir();

    assertEquals( "Original value defined at run execution", jobMetaTest.getVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY ) );
  }

}
