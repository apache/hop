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

import org.apache.hop.core.SourceToTargetMapping;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.testing.DataSet;
import org.apache.hop.testing.DataSetCsvUtil;
import org.apache.hop.testing.util.DataSetConst;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author matt
 */
@ExtensionPoint(
  id = "WriteToDataSetExtensionPoint",
  extensionPointId = "PipelineStartThreads",
  description = "Writes rows of data from a transform into a data set"
)
public class WriteToDataSetExtensionPoint implements IExtensionPoint<IPipelineEngine<PipelineMeta>> {

  // These maps have the name of the pipeline as key
  //
  public static Map<String, TransformMeta> transformsMap = new HashMap<>();
  public static Map<String, List<SourceToTargetMapping>> mappingsMap = new HashMap<>();
  public static Map<String, DataSet> setsMap = new HashMap<>();

  @Override
  public void callExtensionPoint( ILogChannel log, IVariables variables, IPipelineEngine<PipelineMeta> pipeline ) throws HopException {

    final PipelineMeta pipelineMeta = pipeline.getPipelineMeta();
    boolean writeToDataSet = "Y".equalsIgnoreCase( pipeline.getVariable( DataSetConst.VAR_WRITE_TO_DATASET ) );
    if ( !writeToDataSet ) {
      return;
    }

    pipeline.addExecutionFinishedListener( engine -> {
      // Remove the flag when done.
      // We don't want to write to the data set every time we run
      //
      pipeline.setVariable( DataSetConst.VAR_WRITE_TO_DATASET, null );

      // Prevent memory leaking as well
      //
      WriteToDataSetExtensionPoint.transformsMap.remove( pipelineMeta.getName() );
      WriteToDataSetExtensionPoint.mappingsMap.remove( pipelineMeta.getName() );
      WriteToDataSetExtensionPoint.setsMap.remove( pipelineMeta.getName() );
    } );

    try {
      IHopMetadataProvider metadataProvider = pipelineMeta.getMetadataProvider();

      if ( metadataProvider == null ) {
        return; // Nothing to do here, we can't reference data sets.
      }

      // Replace all transforms with input data sets with Injector transforms.
      // Replace all transforms with a golden data set, attached to a unit test, with a Dummy
      // Apply tweaks
      //
      for ( final TransformMeta transformMeta : pipeline.getPipelineMeta().getTransforms() ) {

        // We might want to pass the data from this transform into a data set all by itself...
        // For this we want to attach a row listener which writes the data.
        //
        TransformMeta injectMeta = transformsMap.get( pipelineMeta.getName() );
        if ( injectMeta != null && injectMeta.equals( transformMeta ) ) {
          final List<SourceToTargetMapping> mappings = mappingsMap.get( pipelineMeta.getName() );
          final DataSet dataSet = setsMap.get( pipelineMeta.getName() );
          if ( mappings != null && dataSet != null ) {
            passTransformRowsToDataSet( pipeline, pipelineMeta, transformMeta, mappings, dataSet );
          }
        }
      }
    } catch ( Throwable e ) {
      throw new HopException( "Unable to pass rows to data set", e );
    }
  }

  private void passTransformRowsToDataSet( final IPipelineEngine<PipelineMeta> pipeline, final PipelineMeta pipelineMeta, final TransformMeta transformMeta, final List<SourceToTargetMapping> mappings,
                                           final DataSet dataSet )
    throws HopException {

    // This is the transform to inject into the specified data set
    //
    final IRowMeta setRowMeta = dataSet.getSetRowMeta();

    IEngineComponent component = pipeline.findComponent( transformMeta.getName(), 0 );

    final List<Object[]> transformsForDbRows = new ArrayList<>();

    component.addRowListener( new RowAdapter() {
      public void rowWrittenEvent( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
        Object[] transformForDbRow = RowDataUtil.allocateRowData( setRowMeta.size() );
        for ( SourceToTargetMapping mapping : mappings ) {
          transformForDbRow[ mapping.getTargetPosition() ] = row[ mapping.getSourcePosition() ];
        }
        transformsForDbRows.add( transformForDbRow );
      }
    } );

    // At the end of the pipeline, write it...
    //
    pipeline.addExecutionFinishedListener( engine -> {
      // Write it
      //
      DataSetCsvUtil.writeDataSetData( pipeline, dataSet, setRowMeta, transformsForDbRows );
    } );

  }
}
