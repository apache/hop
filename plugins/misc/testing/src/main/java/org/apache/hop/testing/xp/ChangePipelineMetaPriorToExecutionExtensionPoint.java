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

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.testing.PipelineUnitTest;
import org.apache.hop.testing.VariableValue;
import org.apache.hop.testing.util.DataSetConst;

import java.io.OutputStream;
import java.util.List;

@ExtensionPoint(
  extensionPointId = "PipelinePrepareExecution",
  id = "ChangePipelineMetaPriorToExecutionExtensionPoint",
  description = "Change the pipeline metadata in prior to execution preparation but only during execution in HopGui"
)
public class ChangePipelineMetaPriorToExecutionExtensionPoint implements IExtensionPoint<IPipelineEngine<PipelineMeta>> {

  @Override
  public void callExtensionPoint( ILogChannel log, IVariables variables, IPipelineEngine<PipelineMeta> pipeline ) throws HopException {
    PipelineMeta pipelineMeta = pipeline.getPipelineMeta();

    boolean runUnitTest = "Y".equalsIgnoreCase( variables.getVariable( DataSetConst.VAR_RUN_UNIT_TEST ) );
    if ( !runUnitTest ) {
      // No business here...
      if ( log.isDetailed() ) {
        log.logDetailed( "Not running a unit test..." );
      }
      return;
    }
    String unitTestName = pipeline.getVariable( DataSetConst.VAR_UNIT_TEST_NAME );

    // Do we have something to work with?
    // Unit test disabled?  Github issue #5
    //
    if ( StringUtils.isEmpty( unitTestName ) ) {
      if ( log.isDetailed() ) {
        log.logDetailed( "Unit test disabled." );
      }
      return;
    }

    PipelineUnitTest unitTest = null;

    try {
      unitTest = pipeline.getMetadataProvider().getSerializer(PipelineUnitTest.class).load( unitTestName );
    } catch ( HopException e ) {
      throw new HopException( "Unable to load unit test '" + unitTestName + "'", e );
    }

    if ( unitTest == null ) {
      throw new HopException( "Unit test '" + unitTestName + "' could not be found." );
    }

    // Get a modified copy of the pipeline using the unit test information
    //
    PipelineMetaModifier modifier = new PipelineMetaModifier( pipeline, pipelineMeta, unitTest );
    PipelineMeta copyPipelineMeta = modifier.getTestPipeline( log, pipeline, pipeline.getMetadataProvider() );

    // Now replace the metadata in the IPipelineEngine<PipelineMeta> object...
    //
    pipeline.setPipelineMeta( copyPipelineMeta );

    // Set parameters and variables...
    //
    String[] parameters = copyPipelineMeta.listParameters();
    List<VariableValue> variableValues = unitTest.getVariableValues();
    for ( VariableValue variableValue : variableValues ) {
      String key = pipeline.resolve( variableValue.getKey() );
      String value = pipeline.resolve( variableValue.getValue() );

      if ( StringUtils.isEmpty( key ) ) {
        continue;
      }
      if ( Const.indexOfString( key, parameters ) < 0 ) {
        // set the variable in the pipeline metadata...
        //
        pipeline.setVariable( key, value );
      } else {
        // Set the parameter value...
        //
        pipeline.setParameterValue( key, value );
      }
    }

    String testFilename = pipeline.resolve( unitTest.getFilename() );
    if ( !StringUtil.isEmpty( testFilename ) ) {
      try {
        OutputStream os = HopVfs.getOutputStream( testFilename, false );
        os.write( XmlHandler.getXmlHeader().getBytes() );
        os.write( copyPipelineMeta.getXml().getBytes() );
        os.close();
      } catch ( Exception e ) {
        throw new HopException( "Error writing test filename to '" + testFilename + "'", e );
      }
    }
  }
}
