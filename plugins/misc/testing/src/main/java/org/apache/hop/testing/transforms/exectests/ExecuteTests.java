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

package org.apache.hop.testing.transforms.exectests;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.testing.PipelineUnitTest;
import org.apache.hop.testing.UnitTestResult;
import org.apache.hop.testing.gui.TestingGuiPlugin;
import org.apache.hop.testing.util.DataSetConst;

import java.util.ArrayList;
import java.util.List;

public class ExecuteTests extends BaseTransform<ExecuteTestsMeta, ExecuteTestsData> implements ITransform<ExecuteTestsMeta, ExecuteTestsData> {

  public ExecuteTests( TransformMeta transformMeta, ExecuteTestsMeta meta, ExecuteTestsData data, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  @Override
  public boolean init() {

    try {
      data.hasPrevious = false;
      TransformMeta[] prevTransforms = getPipelineMeta().getPrevTransforms( getTransformMeta() );
      if ( prevTransforms.length > 0 ) {
        data.hasPrevious = true;

        if ( StringUtils.isEmpty( meta.getTestNameInputField() ) ) {
          log.logError( "When this transform receives input it wants the name of a field to get the unit test name from to determine which transforms to execute" );
          setErrors( 1 );
          return false;
        }
      }
    } catch ( Exception e ) {
      log.logError( "Error analyzing ", e );
      setErrors( 1 );
      return false;
    }

    return super.init();
  }

  @Override
  public boolean processRow() throws HopException {

    if ( first ) {
      first = false;

      IHopMetadataSerializer<PipelineUnitTest> testSerializer = metadataProvider.getSerializer( PipelineUnitTest.class );

      // Read all the unit test names from the previous transform(s)
      //
      if ( data.hasPrevious ) {

        data.tests = new ArrayList<>();
        Object[] row = getRow();

        if ( row == null ) {
          // No input and as such no tests to execute. We're all done here.
          //
          setOutputDone();
          return false;
        }

        int inputFieldIndex = getInputRowMeta().indexOfValue( meta.getTestNameInputField() );
        if ( inputFieldIndex < 0 ) {
          throw new HopException( "Unable to find test name field '" + meta.getTestNameInputField() + "' in the input" );
        }

        while ( row != null ) {
          String testName = getInputRowMeta().getString( row, inputFieldIndex );
          try {
            PipelineUnitTest pipelineUnitTest = testSerializer.load( testName );

            data.tests.add( pipelineUnitTest );
          } catch ( Exception e ) {
            throw new HopException( "Unable to load test '" + testName + "'", e );
          }
          row = getRow();
        }
      } else {
        // Get all the unit tests from the meta store
        // Read them all, filter by type later below...
        //
        try {
          data.tests = new ArrayList<>();
          for ( String testName : testSerializer.listObjectNames() ) {
            PipelineUnitTest pipelineUnitTest = testSerializer.load( testName );
            if ( meta.getTypeToExecute() == null || meta.getTypeToExecute() == pipelineUnitTest.getType() ) {
              data.tests.add( pipelineUnitTest );
            }
          }
        } catch ( HopException e ) {
          throw new HopException( "Unable to read pipeline unit tests from the metadata", e );
        }
      }

      data.testsIterator = data.tests.iterator();
      data.outputRowMeta = new RowMeta();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );
    }

    // Execute one test per iteration.
    //
    if ( data.testsIterator.hasNext() ) {
      PipelineUnitTest test = data.testsIterator.next();

      // Let's execute this test.
      //
      // 1. Load the pipeline meta data, set unit test attributes...
      //
      PipelineMeta testPipelineMeta = null;

      try {
        testPipelineMeta = loadTestPipeline( test );

        // 2. Create the pipeline executor...
        //
        if ( log.isDetailed() ) {
          log.logDetailed( "Executing pipeline '" + testPipelineMeta.getName() + "' for unit test '" + test.getName() + "'" );
        }
        IPipelineEngine<PipelineMeta> testPipeline = new LocalPipelineEngine( testPipelineMeta, this, this );

        // 3. Pass execution details...
        //
        testPipeline.initializeVariablesFrom( this );
        testPipeline.setLogLevel( getPipeline().getLogLevel() );
        testPipeline.setMetadataProvider( getMetadataProvider() );

        // Don't show to unit tests results dialog in case of errors
        //
        testPipeline.setVariable( DataSetConst.VAR_DO_NOT_SHOW_UNIT_TEST_ERRORS, "Y" );

        // Make sure to run the unit test: gather data to compare after execution.
        //
        testPipeline.setVariable( DataSetConst.VAR_RUN_UNIT_TEST, "Y" );
        testPipeline.setVariable( DataSetConst.VAR_UNIT_TEST_NAME, test.getName() );

        // 4. Execute
        //
        testPipeline.execute();
        testPipeline.waitUntilFinished();

        // 5. Validate results...
        //
        Result transResult = testPipeline.getResult();
        if ( transResult.getNrErrors() != 0 ) {
          // The pipeline had a failure, report this too.
          //
          Object[] row = RowDataUtil.allocateRowData( data.outputRowMeta.size() );
          int index = 0;
          row[ index++ ] = testPipelineMeta.getName();
          row[ index++ ] = null;
          row[ index++ ] = null;
          row[ index++ ] = null;
          row[ index++ ] = Boolean.TRUE;
          row[ index++ ] = transResult.getLogText();

          putRow( data.outputRowMeta, row );
        }

        List<UnitTestResult> testResults = new ArrayList<>();
        DataSetConst.validateTransResultAgainstUnitTest( testPipeline, test, metadataProvider, testResults );

        for ( UnitTestResult testResult : testResults ) {
          Object[] row = RowDataUtil.allocateRowData( data.outputRowMeta.size() );
          int index = 0;
          row[ index++ ] = testResult.getPipelineName();
          row[ index++ ] = testResult.getUnitTestName();
          row[ index++ ] = testResult.getDataSetName();
          row[ index++ ] = testResult.getTransformName();
          row[ index++ ] = testResult.isError();
          row[ index++ ] = testResult.getComment();

          putRow( data.outputRowMeta, row );
        }

        return true;
      } catch ( HopException e ) {
        // Some configuration or setup error...
        //
        Object[] row = RowDataUtil.allocateRowData( data.outputRowMeta.size() );
        int index = 0;
        row[ index++ ] = testPipelineMeta == null ? null : testPipelineMeta.getName();
        row[ index++ ] = test.getName();
        row[ index++ ] = null;
        row[ index++ ] = null;
        row[ index++ ] = Boolean.TRUE; // ERROR!
        row[ index++ ] = e.getMessage() + " : " + Const.getStackTracker( e );

        putRow( data.outputRowMeta, row );
        return true;
      }
    } else {
      setOutputDone();
      return false;
    }
  }

  private PipelineMeta loadTestPipeline( PipelineUnitTest test ) throws HopException {
    PipelineMeta unitTestPipelineMeta = null;
    // Environment substitution is not yet supported in the UI
    //
    String filename = test.calculateCompleteFilename(this);
    if ( StringUtils.isNotEmpty( filename ) ) {
      unitTestPipelineMeta = new PipelineMeta( filename, metadataProvider, true, this );
    }
    if ( unitTestPipelineMeta == null ) {
      throw new HopException( "Unable to find a valid pipeline filename in unit test '" + test.getName() + "'" );
    }

    // Pass some data from the parent...
    //
    unitTestPipelineMeta.setMetadataProvider( metadataProvider );

    // clear and load attributes for unit test...
    //
    TestingGuiPlugin.selectUnitTest( unitTestPipelineMeta, test );

    return unitTestPipelineMeta;
  }


}
