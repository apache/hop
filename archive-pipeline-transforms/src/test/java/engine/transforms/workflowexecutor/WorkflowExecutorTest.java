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

package org.apache.hop.pipeline.transforms.workflowexecutor;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LoggingObjectInterface;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.workflow.Workflow;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.pipeline.transforms.TransformMockUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * @author Mikhail_Chen-Len-Son
 */
public class WorkflowExecutorTest {

  private WorkflowExecutor executor;
  private WorkflowExecutorMeta meta;
  private WorkflowExecutorData data;

  @Before
  public void setUp() throws Exception {
    executor = TransformMockUtil.getTransform( WorkflowExecutor.class, WorkflowExecutorMeta.class, "PipelineExecutorUnitTest" );
    executor = spy( executor );
    executor.setInputRowMeta( mock( IRowMeta.class ) );

    doNothing().when( executor ).discardLogLines( any( WorkflowExecutorData.class ) );

    meta = new WorkflowExecutorMeta();
    data = new WorkflowExecutorData();
    Workflow workflow = mock( Workflow.class );
    doReturn( workflow ).when( executor ).createWorkflow( any( WorkflowMeta.class ),
      any( LoggingObjectInterface.class ) );
    doReturn( ArrayUtils.EMPTY_STRING_ARRAY ).when( workflow ).listParameters();

    data.groupBuffer = new ArrayList<>();
    data.groupSize = -1;
    data.groupTime = -1;
    data.groupField = null;
  }

  @After
  public void tearDown() {
    executor = null;
    meta = null;
    data = null;
  }

  /**
   * Given an input data and a workflow executor with specified field to group rows on.
   * <br/>
   * When workflow executor is processing rows of an input data,
   * then rows should be accumulated in a group as long as the specified field value stays the same.
   */
  @Test
  public void shouldAccumulateRowsWhenGroupFieldIsSpecified() throws HopException {
    prepareMultipleRowsForExecutor();

    data.groupField = "groupField";
    executor.init();

    when( executor.getExecutorWorkflow() ).thenReturn( mock( Workflow.class ) );
    when( executor.getExecutorWorkflow().getWorkflowMeta() ).thenReturn( mock( WorkflowMeta.class ) );

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "groupField" ) );
    executor.setInputRowMeta( rowMeta );

    // start processing
    executor.init(); // 1st row - 'value1'
    // should be added to group buffer
    assertEquals( 1, data.groupBuffer.size() );

    executor.init();
    executor.init();
    executor.init(); // 4th row - still 'value1'
    // first 4 rows should be added to the same group
    assertEquals( 4, data.groupBuffer.size() );

    executor.init(); // 5th row - value has been changed - 'value12'
    // previous group buffer should be flushed
    // and a new group should be started
    assertEquals( 1, data.groupBuffer.size() );

    executor.init(); // 6th row - 'value12'
    executor.init(); // 7th row - 'value12'
    // the rest rows should be added to another group
    assertEquals( 3, data.groupBuffer.size() );

    executor.init(); // end of file
    // group buffer should be flushed in the end
    assertEquals( 0, data.groupBuffer.size() );
  }

  /**
   * Given an input data and a workflow executor
   * with specified number of rows to send to the pipeline (X).
   * <br/>
   * When workflow executor is processing rows of an input data,
   * then every X rows should be accumulated in a group.
   */
  @Test
  public void shouldAccumulateRowsByCount() throws HopException {
    prepareMultipleRowsForExecutor();

    data.groupSize = 5;
    executor.init();

    when( executor.getExecutorWorkflow() ).thenReturn( mock( Workflow.class ) );
    when( executor.getExecutorWorkflow().getWorkflowMeta() ).thenReturn( mock( WorkflowMeta.class ) );

    // start processing
    executor.init(); // 1st row
    // should be added to group buffer
    assertEquals( 1, data.groupBuffer.size() );

    executor.init();
    executor.init();
    executor.init(); // 4th row
    // first 4 rows should be added to the same group
    assertEquals( 4, data.groupBuffer.size() );

    executor.init(); // 5th row
    // once the 5th row is processed, the pipeline executor should be triggered
    // and thus, group buffer should be flushed
    assertEquals( 0, data.groupBuffer.size() );

    executor.init(); // 6th row
    // previous group buffer should be flushed
    // and a new group should be started
    assertEquals( 1, data.groupBuffer.size() );

    executor.init(); // 7th row
    // the rest rows should be added to another group
    assertEquals( 2, data.groupBuffer.size() );

    executor.init(); // end of file
    // group buffer should be flushed in the end
    assertEquals( 0, data.groupBuffer.size() );
  }

  // values to be grouped
  private void prepareMultipleRowsForExecutor() throws HopException {
    doReturn( new Object[] { "value1" } )
      .doReturn( new Object[] { "value1" } )
      .doReturn( new Object[] { "value1" } )
      .doReturn( new Object[] { "value1" } )
      .doReturn( new Object[] { "value12" } )
      .doReturn( new Object[] { "value12" } )
      .doReturn( new Object[] { "value12" } )
      .doReturn( null )
      .when( executor ).getRow();
  }
}
