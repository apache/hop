/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hop.testing.util;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.testing.PipelineUnitTest;
import org.apache.hop.testing.UnitTestResult;
import org.apache.hop.testing.gui.TestingGuiPlugin;

import java.util.ArrayList;
import java.util.List;

public class UnitTestUtil {

  public static final PipelineMeta loadTestPipeline( PipelineUnitTest test, IHopMetadataProvider metadataProvider, IVariables variables ) throws HopException {
    PipelineMeta unitTestPipelineMeta = null;
    // Environment substitution is not yet supported in the UI
    //
    String filename = test.calculateCompletePipelineFilename(variables);
    if ( StringUtils.isNotEmpty( filename ) ) {
      unitTestPipelineMeta = new PipelineMeta( filename, metadataProvider, true, variables );
    }
    if ( unitTestPipelineMeta == null ) {
      throw new HopException( "Unable to find a valid pipeline filename in unit test '" + test.getName() + "'" );
    }

    // Pass some data from the parent...
    //
    unitTestPipelineMeta.setMetadataProvider( metadataProvider );

    return unitTestPipelineMeta;
  }

  public static final void executeUnitTest( PipelineUnitTest test, ILoggingObject parentObject, LogLevel logLevel,
                                            IHopMetadataProvider metadataProvider, IVariables variables,
                                            IPipelineResultEvaluator pipelineResultEvaluator,
                                            ITestResultsEvaluator testResultsEvaluator,
                                            IExceptionEvaluator exceptionEvaluator) throws HopException {
    PipelineMeta testPipelineMeta = null;

    try {
      // 1. Load the pipeline meta data, set unit test attributes...
      //
      testPipelineMeta = loadTestPipeline( test, metadataProvider, variables );

      // 2. Create the pipeline executor...
      //
      IPipelineEngine<PipelineMeta> testPipeline = new LocalPipelineEngine( testPipelineMeta, variables, parentObject );

      // 3. Pass execution details...
      //
      testPipeline.initializeFrom( variables );
      testPipeline.setLogLevel( logLevel );
      testPipeline.setMetadataProvider( metadataProvider );

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
      Result pipelineResult = testPipeline.getResult();
      pipelineResultEvaluator.evaluatePipelineResults( testPipeline, pipelineResult );

      List<UnitTestResult> testResults = new ArrayList<>();
      DataSetConst.validateTransResultAgainstUnitTest( testPipeline, test, metadataProvider, testResults );
      testResultsEvaluator.evaluateTestResults( testPipeline, testResults );
    } catch ( HopException e ) {
      exceptionEvaluator.evaluateTestException( test, testPipelineMeta, e );
    }
  }

}
