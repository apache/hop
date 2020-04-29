/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
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

package org.apache.hop.testing.xp;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.testing.PipelineUnitTest;
import org.apache.hop.testing.util.DataSetConst;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.metastore.api.exceptions.MetaStoreException;

import java.io.OutputStream;

@ExtensionPoint(
  extensionPointId = "PipelinePrepareExecution",
  id = "ChangePipelineMetaPriorToExecutionExtensionPoint",
  description = "Change the pipeline metadata in prior to execution preperation but only during execution in HopGui"
)
public class ChangePipelineMetaPriorToExecutionExtensionPoint implements IExtensionPoint<IPipelineEngine<PipelineMeta>> {

  @Override
  public void callExtensionPoint( ILogChannel log, IPipelineEngine<PipelineMeta> pipeline ) throws HopException {
    PipelineMeta pipelineMeta = pipeline.getSubject();

    boolean runUnitTest = "Y".equalsIgnoreCase( pipelineMeta.getVariable( DataSetConst.VAR_RUN_UNIT_TEST ) );
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
      unitTest = PipelineUnitTest.createFactory( pipeline.getMetaStore() ).loadElement( unitTestName );
      unitTest.initializeVariablesFrom( pipelineMeta );
    } catch ( MetaStoreException e ) {
      throw new HopException( "Unable to load unit test '" + unitTestName + "'", e );
    }

    if ( unitTest == null ) {
      throw new HopException( "Unit test '" + unitTestName + "' could not be found." );
    }

    // Get a modified copy of the pipeline using the unit test information
    //
    PipelineMetaModifier modifier = new PipelineMetaModifier( pipelineMeta, unitTest );
    PipelineMeta copyPipelineMeta = modifier.getTestPipeline( log, pipeline, pipeline.getMetaStore() );


    // Now replace the metadata in the IPipelineEngine<PipelineMeta> object...
    //
    pipeline.setSubject( copyPipelineMeta );

    String testFilename = pipeline.environmentSubstitute( unitTest.getFilename() );
    if ( !StringUtil.isEmpty( testFilename ) ) {
      try {
        OutputStream os = HopVfs.getOutputStream( testFilename, false );
        os.write( XmlHandler.getXMLHeader().getBytes() );
        os.write( copyPipelineMeta.getXml().getBytes() );
        os.close();
      } catch ( Exception e ) {
        throw new HopException( "Error writing test filename to '" + testFilename + "'", e );
      }
    }
  }
}
