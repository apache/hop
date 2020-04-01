/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.steps.concatfields;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.logging.LoggingObjectInterface;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.step.StepDataInterface;
import org.apache.hop.pipeline.step.StepMeta;
import org.apache.hop.pipeline.steps.mock.StepMockHelper;
import org.apache.hop.pipeline.steps.textfileoutput.TextFileField;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * User: Dzmitry Stsiapanau Date: 2/11/14 Time: 11:00 AM
 */
public class ConcatFieldsTest {

  private class ConcatFieldsHandler extends ConcatFields {

    private Object[] row;

    public ConcatFieldsHandler( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
                                PipelineMeta pipelineMeta, Pipeline pipeline ) {
      super( stepMeta, stepDataInterface, copyNr, pipelineMeta, pipeline );
    }

    /**
     * In case of getRow, we receive data from previous steps through the input rowset. In case we split the stream, we
     * have to copy the data to the alternate splits: rowsets 1 through n.
     */
    @Override
    public Object[] getRow() throws HopException {
      return row;
    }

    public void setRow( Object[] row ) {
      this.row = row;
    }

    @Override
    protected Object[] putRowFastDataDump( Object[] r ) throws HopStepException {
      return null;
    }

    @Override
    protected boolean writeHeader() {
      return true;
    }

    @Override
    Object[] putRowFromStream( Object[] r ) throws HopStepException {
      return prepareOutputRow( r );
    }
  }

  private StepMockHelper<ConcatFieldsMeta, ConcatFieldsData> stepMockHelper;
  private TextFileField textFileField = new TextFileField( "Name", 2, "", 10, 20, "", "", "", "" );
  private TextFileField textFileField2 = new TextFileField( "Surname", 2, "", 10, 20, "", "", "", "" );
  private TextFileField[] textFileFields = new TextFileField[] { textFileField, textFileField2 };

  @Before
  public void setUp() throws Exception {
    stepMockHelper =
      new StepMockHelper<ConcatFieldsMeta, ConcatFieldsData>( "CONCAT FIELDS TEST", ConcatFieldsMeta.class,
        ConcatFieldsData.class );
    when( stepMockHelper.logChannelInterfaceFactory.create( any(), any( LoggingObjectInterface.class ) ) ).thenReturn(
      stepMockHelper.logChannelInterface );
    when( stepMockHelper.pipeline.isRunning() ).thenReturn( true );
  }

  @After
  public void tearDown() throws Exception {
    stepMockHelper.cleanUp();
  }

  @Test
  public void testPrepareOutputRow() throws Exception {
    ConcatFieldsHandler concatFields =
      new ConcatFieldsHandler( stepMockHelper.stepMeta, stepMockHelper.stepDataInterface, 0,
        stepMockHelper.pipelineMeta, stepMockHelper.pipeline );
    Object[] row = new Object[] { "one", "two" };
    String[] fieldNames = new String[] { "one", "two" };
    concatFields.setRow( row );
    RowMetaInterface inputRowMeta = mock( RowMetaInterface.class );
    when( inputRowMeta.clone() ).thenReturn( inputRowMeta );
    when( inputRowMeta.size() ).thenReturn( 2 );
    when( inputRowMeta.getFieldNames() ).thenReturn( fieldNames );
    when( stepMockHelper.processRowsStepMetaInterface.getOutputFields() ).thenReturn( textFileFields );
    when( stepMockHelper.processRowsStepMetaInterface.isFastDump() ).thenReturn( Boolean.TRUE );
    when( stepMockHelper.processRowsStepMetaInterface.isFileAppended() ).thenReturn( Boolean.FALSE );
    when( stepMockHelper.processRowsStepMetaInterface.isFileNameInField() ).thenReturn( Boolean.FALSE );
    when( stepMockHelper.processRowsStepMetaInterface.isHeaderEnabled() ).thenReturn( Boolean.TRUE );
    when( stepMockHelper.processRowsStepMetaInterface.isRemoveSelectedFields() ).thenReturn( Boolean.TRUE );
    concatFields.setInputRowMeta( inputRowMeta );
    try {
      concatFields.processRow( stepMockHelper.processRowsStepMetaInterface,
        stepMockHelper.processRowsStepDataInterface );
      concatFields.prepareOutputRow( row );
    } catch ( NullPointerException npe ) {
      fail( "NullPointerException issue PDI-8870 still reproduced " );
    }
  }
}
