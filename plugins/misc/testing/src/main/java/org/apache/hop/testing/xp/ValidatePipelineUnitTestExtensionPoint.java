/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.testing.xp;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.testing.PipelineUnitTest;
import org.apache.hop.testing.UnitTestResult;
import org.apache.hop.testing.util.DataSetConst;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;

import java.util.ArrayList;
import java.util.List;

/**
 * @author matt
 */
@ExtensionPoint(
  extensionPointId = "PipelineFinish",
  id = "ValidatePipelineUnitTestExtensionPoint",
  description = "Inject a bunch of rows into a transform during preview"
)
public class ValidatePipelineUnitTestExtensionPoint implements IExtensionPoint<IPipelineEngine<PipelineMeta>> {

  @Override
  public void callExtensionPoint( ILogChannel log, IVariables variables, IPipelineEngine<PipelineMeta> pipeline ) throws HopException {

    final PipelineMeta pipelineMeta = pipeline.getPipelineMeta();
    boolean runUnitTest = "Y".equalsIgnoreCase( pipeline.getVariable( DataSetConst.VAR_RUN_UNIT_TEST ) );
    if ( !runUnitTest ) {
      return;
    }

    // We should always have a unit test name here...
    String unitTestName = pipeline.getVariable( DataSetConst.VAR_UNIT_TEST_NAME );
    if ( StringUtil.isEmpty( unitTestName ) ) {
      return;
    }

    try {
      IHopMetadataProvider metadataProvider = pipelineMeta.getMetadataProvider();

      if ( metadataProvider == null ) {
        return; // Nothing to do here, we can't reference data sets.
      }

      // If the pipeline has a variable set with the unit test in it, we're dealing with a unit test situation.
      //
      PipelineUnitTest unitTest = metadataProvider.getSerializer( PipelineUnitTest.class).load( unitTestName );

      final List<UnitTestResult> results = new ArrayList<>();
      pipeline.getExtensionDataMap().put( DataSetConst.UNIT_TEST_RESULTS, results );

      // Validate execution results with what's in the data sets...
      //
      int errors = DataSetConst.validateTransResultAgainstUnitTest( pipeline, unitTest, metadataProvider, results );
      if ( errors == 0 ) {
        log.logBasic( "Unit test '" + unitTest.getName() + "' passed succesfully" );
      } else {
        log.logBasic( "Unit test '" + unitTest.getName() + "' failed, " + errors + " errors detected, " + results.size() + " comments to report." );

        String dontShowResults = pipeline.getVariable( DataSetConst.VAR_DO_NOT_SHOW_UNIT_TEST_ERRORS, "N" );

        final HopGui hopGui = HopGui.getInstance();
        if ( hopGui != null && "N".equalsIgnoreCase( dontShowResults ) ) {
          hopGui.getShell().getDisplay().asyncExec( () -> {
            PreviewRowsDialog dialog = new PreviewRowsDialog( hopGui.getShell(), pipeline, SWT.NONE,
              "Unit test results",
              UnitTestResult.getRowMeta(),
              UnitTestResult.getRowData( results ) );
            dialog.setDynamic( false );
            dialog.setProposingToGetMoreRows( false );
            dialog.setProposingToStop( false );
            dialog.setTitleMessage( "Unit test results", "Here are the results of the unit test validations:" );
            dialog.open();
          } );
        }
      }
      log.logBasic( "----------------------------------------------" );
      for ( UnitTestResult result : results ) {
        if ( result.getDataSetName() != null ) {
          log.logBasic( result.getTransformName() + " - " + result.getDataSetName() + " : " + result.getComment() );
        } else {
          log.logBasic( result.getComment() );
        }
      }
      log.logBasic( "----------------------------------------------" );
    } catch ( Throwable e ) {
      log.logError( "Unable to validate unit test/golden rows", e );
    }

  }
}
