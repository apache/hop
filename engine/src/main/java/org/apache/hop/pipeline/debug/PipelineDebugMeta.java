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

package org.apache.hop.pipeline.debug;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.IExecutionFinishedListener;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * For a certain pipeline, we want to be able to insert break-points into a pipeline. These breakpoints can
 * be applied to transforms. When a certain condition is met, the pipeline will be paused and the caller will be
 * informed of this fact through a listener system.
 *
 * @author Matt
 */
public class PipelineDebugMeta {

  public static final String XML_TAG = "pipeline-debug-meta";

  public static final String XML_TAG_TRANSFORM_DEBUG_METAS = "transform-debug-metas";

  private PipelineMeta pipelineMeta;
  private Map<TransformMeta, TransformDebugMeta> transformDebugMetaMap;
  private boolean dataShown = false;

  public PipelineDebugMeta( PipelineMeta pipelineMeta ) {
    this.pipelineMeta = pipelineMeta;
    transformDebugMetaMap = new HashMap<>();
  }

  /**
   * @return the referenced pipeline metadata
   */
  public PipelineMeta getPipelineMeta() {
    return pipelineMeta;
  }

  /**
   * @param pipelineMeta the pipeline metadata to reference
   */
  public void setPipelineMeta( PipelineMeta pipelineMeta ) {
    this.pipelineMeta = pipelineMeta;
  }

  /**
   * @return the map that contains the debugging information per transform
   */
  public Map<TransformMeta, TransformDebugMeta> getTransformDebugMetaMap() {
    return transformDebugMetaMap;
  }

  /**
   * @param transformDebugMeta the map that contains the debugging information per transform
   */
  public void setTransformDebugMetaMap( Map<TransformMeta, TransformDebugMeta> transformDebugMeta ) {
    this.transformDebugMetaMap = transformDebugMeta;
  }

  public synchronized void addRowListenersToPipeline( final IPipelineEngine<PipelineMeta> pipeline ) {

    // for every transform in the map, add a row listener...
    //
    dataShown=false;
    for ( final TransformMeta transformMeta : transformDebugMetaMap.keySet() ) {
      final TransformDebugMeta transformDebugMeta = transformDebugMetaMap.get( transformMeta );

      // What is the pipeline thread to attach a listener to?
      //
      for ( IEngineComponent component : pipeline.getComponentCopies( transformMeta.getName() ) ) {
        // TODO: Make this functionality more generic in the pipeline engines
        //
        if ( component instanceof ITransform ) {
          ITransform baseTransform = (ITransform) component;
          baseTransform.addRowListener( new RowAdapter() {
               public void rowWrittenEvent( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
                 try {
                   synchronized ( transformDebugMeta ) {
                     // This block of code is called whenever there is a row written by the transform
                     // So we want to execute the debugging actions that are specified by the transform...
                     //
                     int rowCount = transformDebugMeta.getRowCount();

                     if ( transformDebugMeta.isReadingFirstRows() && rowCount > 0 ) {

                       int bufferSize = transformDebugMeta.getRowBuffer().size();
                       if ( bufferSize < rowCount ) {

                         // This is the classic preview mode.
                         // We add simply add the row to the buffer.
                         //
                         transformDebugMeta.setRowBufferMeta( rowMeta );
                         transformDebugMeta.getRowBuffer().add( rowMeta.cloneRow( row ) );
                       } else {
                         // pause the pipeline...
                         //
                         pipeline.pauseExecution();

                         // Also call the pause / break-point listeners on the transform debugger...
                         //
                         dataShown = true;
                         transformDebugMeta.fireBreakPointListeners( PipelineDebugMeta.this );
                       }
                     } else if ( transformDebugMeta.isPausingOnBreakPoint() && transformDebugMeta.getCondition() != null ) {
                       // A break-point is set
                       // Verify the condition and pause if required
                       // Before we do that, see if a row count is set.
                       // If so, keep the last rowCount rows in memory
                       //
                       if ( rowCount > 0 ) {
                         // Keep a number of rows in memory
                         // Store them in a reverse order to keep it intuitive for the user.
                         //
                         transformDebugMeta.setRowBufferMeta( rowMeta );
                         transformDebugMeta.getRowBuffer().add( 0, rowMeta.cloneRow( row ) );

                         // Only keep a number of rows in memory
                         // If we have too many, remove the last (oldest)
                         //
                         int bufferSize = transformDebugMeta.getRowBuffer().size();
                         if ( bufferSize > rowCount ) {
                           transformDebugMeta.getRowBuffer().remove( bufferSize - 1 );
                         }
                       } else {
                         // Just keep one row...
                         //
                         if ( transformDebugMeta.getRowBuffer().isEmpty() ) {
                           transformDebugMeta.getRowBuffer().add( rowMeta.cloneRow( row ) );
                         } else {
                           transformDebugMeta.getRowBuffer().set( 0, rowMeta.cloneRow( row ) );
                         }
                       }

                       // Now evaluate the condition and see if we need to pause the pipeline
                       //
                       if ( transformDebugMeta.getCondition().evaluate( rowMeta, row ) ) {
                         // We hit the break-point: pause the pipeline
                         //
                         pipeline.pauseExecution();

                         // Also fire off the break point listeners...
                         //
                         transformDebugMeta.fireBreakPointListeners( PipelineDebugMeta.this );
                       }
                     }
                   }
                 } catch ( HopException e ) {
                   throw new HopTransformException( e );
                 }
               }
             }
          );
        }
      }
    }

    // Also add a finished listener to the pipeline.
    // If no preview rows are shown before the end of the pipeline we can do this now...
    //
    try {
      pipeline.addExecutionFinishedListener( p -> {
        if (dataShown) {
          return;
        }
        for (TransformMeta transformMeta : transformDebugMetaMap.keySet()) {
          TransformDebugMeta transformDebugMeta = transformDebugMetaMap.get( transformMeta );
          if (transformDebugMeta!=null) {
            List<Object[]> rowBuffer = transformDebugMeta.getRowBuffer();
            if (rowBuffer!=null && !rowBuffer.isEmpty()) {
              transformDebugMeta.fireBreakPointListeners( this );
            }
          }
        }
      } );
    } catch(Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Add a break point listener to all defined transform debug meta data
   *
   * @param breakPointListener the break point listener to add
   */
  public void addBreakPointListers( IBreakPointListener breakPointListener ) {
    for ( TransformDebugMeta transformDebugMeta : transformDebugMetaMap.values() ) {
      transformDebugMeta.addBreakPointListener( breakPointListener );
    }
  }

  /**
   * @return the number of times the break-point listeners got called. This is the total for all the transforms.
   */
  public int getTotalNumberOfHits() {
    int total = 0;
    for ( TransformDebugMeta transformDebugMeta : transformDebugMetaMap.values() ) {
      total += transformDebugMeta.getNumberOfHits();
    }
    return total;
  }

  /**
   * @return the number of transforms used to preview or debug on
   */
  public int getNrOfUsedTransforms() {
    int nr = 0;

    for ( TransformDebugMeta transformDebugMeta : transformDebugMetaMap.values() ) {
      if ( transformDebugMeta.isReadingFirstRows() && transformDebugMeta.getRowCount() > 0 ) {
        nr++;
      } else if ( transformDebugMeta.isPausingOnBreakPoint()
        && transformDebugMeta.getCondition() != null && !transformDebugMeta.getCondition().isEmpty() ) {
        nr++;
      }
    }

    return nr;
  }
}
