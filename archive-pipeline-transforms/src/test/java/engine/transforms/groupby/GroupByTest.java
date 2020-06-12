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

package org.apache.hop.pipeline.transforms.groupby;

import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.BlockingRowSet;
import org.apache.hop.core.RowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.logging.LoggingObjectInterface;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GroupByTest {
  private TransformMockHelper<GroupByMeta, GroupByData> mockHelper;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void setUpBeforeClass() throws HopPluginException {
    ValueMetaPluginType.getInstance().searchPlugins();
  }

  @Before
  public void setUp() throws Exception {
    mockHelper =
      new TransformMockHelper<>( "Group By", GroupByMeta.class, GroupByData.class );
    when( mockHelper.logChannelFactory.create( any(), any( LoggingObjectInterface.class ) ) ).thenReturn(
      mockHelper.logChannelInterface );
    when( mockHelper.pipeline.isRunning() ).thenReturn( true );
  }

  @After
  public void tearDown() throws Exception {
    mockHelper.cleanUp();
  }

  @Test
  public void testProcessRow() throws HopException {
    GroupByMeta groupByMeta = mock( GroupByMeta.class );
    GroupByData groupByData = mock( GroupByData.class );

    GroupBy groupBySpy = Mockito.spy( new GroupBy( mockHelper.transformMeta, mockHelper.iTransformData, 0,
      mockHelper.pipelineMeta, mockHelper.pipeline ) );
    doReturn( null ).when( groupBySpy ).getRow();
    doReturn( null ).when( groupBySpy ).getInputRowMeta();

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaInteger( "ROWNR" ) );

    List<RowSet> outputRowSets = new ArrayList<RowSet>();
    BlockingRowSet rowSet = new BlockingRowSet( 1 );
    rowSet.putRow( rowMeta, new Object[] { new Long( 0 ) } );
    outputRowSets.add( rowSet );
    groupBySpy.setOutputRowSets( outputRowSets );

    final String[] sub = { "b" };
    doReturn( sub ).when( groupByMeta ).getSubjectField();

    final String[] groupField = { "a" };
    doReturn( groupField ).when( groupByMeta ).getGroupField();

    final String[] aggFields = { "b_g" };
    doReturn( aggFields ).when( groupByMeta ).getAggregateField();

    final int[] aggType = { GroupByMeta.TYPE_GROUP_CONCAT_COMMA };
    doReturn( aggType ).when( groupByMeta ).getAggregateType();

    when( mockHelper.pipelineMeta.getPrevTransformFields( mockHelper.transformMeta ) ).thenReturn( new RowMeta() );
    groupBySpy.processRow();

    assertTrue( groupBySpy.getOutputRowSets().get( 0 ).isDone() );
  }

  @Test
  public void testGetFields() {
    RowMeta outputFields = new RowMeta();
    outputFields.addValueMeta( new ValueMetaString( "group_by_field" ) );
    outputFields.addValueMeta( new ValueMetaInteger( "raw_integer" ) );
    outputFields.addValueMeta( new ValueMetaString( "raw_string" ) );

    GroupByMeta meta = new GroupByMeta();
    meta.allocate( 1, 8 );
    meta.setGroupField( new String[] { "group_by_field" } );
    meta.setAggregateField( new String[] {
      "perc_field", "stddev_field", "median_field", "count_distinct_field",
      "count_any_field", "count_all_field", "concat_comma_field", "concat_custom_field" } );
    meta.setSubjectField( new String[] {
      "raw_integer", "raw_integer", "raw_integer", "raw_integer",
      "raw_integer", "raw_integer", "raw_string", "raw_string" } );
    meta.setAggregateType( new int[] {
      GroupByMeta.TYPE_GROUP_PERCENTILE,
      GroupByMeta.TYPE_GROUP_STANDARD_DEVIATION,
      GroupByMeta.TYPE_GROUP_MEDIAN,
      GroupByMeta.TYPE_GROUP_COUNT_DISTINCT,
      GroupByMeta.TYPE_GROUP_COUNT_ANY,
      GroupByMeta.TYPE_GROUP_COUNT_ALL,
      GroupByMeta.TYPE_GROUP_CONCAT_COMMA,
      GroupByMeta.TYPE_GROUP_CONCAT_STRING } );

    meta.getFields( outputFields, "Group By Transform", (IRowMeta[]) null, (TransformMeta) null,
      (Variables) null, (IMetaStore) null );

    assertEquals( outputFields.getValueMetaList().size(), 9 );
    assertTrue( outputFields.getValueMeta( 0 ).getType() == IValueMeta.TYPE_STRING );
    assertTrue( outputFields.getValueMeta( 0 ).getName().equals( "group_by_field" ) );
    assertTrue( outputFields.getValueMeta( 1 ).getType() == IValueMeta.TYPE_NUMBER );
    assertTrue( outputFields.getValueMeta( 1 ).getName().equals( "perc_field" ) );
    assertTrue( outputFields.getValueMeta( 2 ).getType() == IValueMeta.TYPE_NUMBER );
    assertTrue( outputFields.getValueMeta( 2 ).getName().equals( "stddev_field" ) );
    assertTrue( outputFields.getValueMeta( 3 ).getType() == IValueMeta.TYPE_NUMBER );
    assertTrue( outputFields.getValueMeta( 3 ).getName().equals( "median_field" ) );
    assertTrue( outputFields.getValueMeta( 4 ).getType() == IValueMeta.TYPE_INTEGER );
    assertTrue( outputFields.getValueMeta( 4 ).getName().equals( "count_distinct_field" ) );
    assertTrue( outputFields.getValueMeta( 5 ).getType() == IValueMeta.TYPE_INTEGER );
    assertTrue( outputFields.getValueMeta( 5 ).getName().equals( "count_any_field" ) );
    assertTrue( outputFields.getValueMeta( 6 ).getType() == IValueMeta.TYPE_INTEGER );
    assertTrue( outputFields.getValueMeta( 6 ).getName().equals( "count_all_field" ) );
    assertTrue( outputFields.getValueMeta( 7 ).getType() == IValueMeta.TYPE_STRING );
    assertTrue( outputFields.getValueMeta( 7 ).getName().equals( "concat_comma_field" ) );
    assertTrue( outputFields.getValueMeta( 8 ).getType() == IValueMeta.TYPE_STRING );
    assertTrue( outputFields.getValueMeta( 8 ).getName().equals( "concat_custom_field" ) );
  }


  @Test
  public void testTempFileIsDeleted_AfterCallingDisposeMethod() throws Exception {
    GroupByData groupByData = new GroupByData();
    groupByData.tempFile = File.createTempFile( "test", ".txt" );

    // emulate connections to file are opened
    groupByData.fosToTempFile = new FileOutputStream( groupByData.tempFile );
    groupByData.fisToTmpFile = new FileInputStream( groupByData.tempFile );

    GroupBy groupBySpy = Mockito.spy( new GroupBy( mockHelper.transformMeta, groupByData, 0,
      mockHelper.pipelineMeta, mockHelper.pipeline ) );

    assertTrue( groupByData.tempFile.exists() );
    groupBySpy.dispose();
    // check file is deleted
    assertFalse( groupByData.tempFile.exists() );

  }

  @Test
  public void testAddToBuffer() throws HopException, FileSystemException {
    GroupByData groupByData = new GroupByData();
    ArrayList listMock = mock( ArrayList.class );
    when( listMock.size() ).thenReturn( 5001 );
    groupByData.bufferList = listMock;
    groupByData.rowsOnFile = 0;
    IRowMeta inputRowMetaMock = mock( IRowMeta.class );
    groupByData.inputRowMeta = inputRowMetaMock;

    GroupBy groupBySpy = Mockito.spy(
      new GroupBy( mockHelper.transformMeta, groupByData, 0, mockHelper.pipelineMeta, mockHelper.pipeline ) );

    GroupByMeta groupByMetaMock = mock( GroupByMeta.class );
    when( groupByMetaMock.getPrefix() ).thenReturn( "group-by-test-temp-file-" );
    when( groupBySpy.getMeta() ).thenReturn( groupByMetaMock );

    String userDir = System.getProperty( "user.dir" );
    String vfsFilePath = "file:///" + userDir;
    when( groupBySpy.environmentSubstitute( anyString() ) ).thenReturn( vfsFilePath );

    Object[] row = { "abc" };
    // tested method itself
    groupBySpy.addToBuffer( row );

    // check if file is created
    assertTrue( groupByData.tempFile.exists() );
    groupBySpy.dispose();
    // check file is deleted
    assertFalse( groupByData.tempFile.exists() );

    // since path started with "file:///"
    verify( groupBySpy, times( 1 ) ).retrieveVfsPath( anyString() );
  }
}
