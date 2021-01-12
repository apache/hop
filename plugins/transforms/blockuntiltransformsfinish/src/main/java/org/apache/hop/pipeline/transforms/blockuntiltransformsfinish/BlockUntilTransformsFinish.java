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

package org.apache.hop.pipeline.transforms.blockuntiltransformsfinish;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Block all incoming rows until defined transforms finish processing rows.
 *
 * @author Samatar
 * @since 30-06-2008
 */

public class BlockUntilTransformsFinish
  extends BaseTransform<BlockUntilTransformsFinishMeta, BlockUntilTransformsFinishData>
  implements ITransform<BlockUntilTransformsFinishMeta, BlockUntilTransformsFinishData> {

  private static final Class<?> PKG = BlockUntilTransformsFinishMeta.class; // For Translator

  public BlockUntilTransformsFinish( TransformMeta transformMeta, BlockUntilTransformsFinishMeta meta, BlockUntilTransformsFinishData data, int copyNr,
                                     PipelineMeta pipelineMeta, Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  @Override
  public boolean processRow() throws HopException {

    if ( first ) {
      first = false;
      String[] transformnames = null;
      int transformnrs = 0;
      if ( meta.getTransformName() != null && meta.getTransformName().length > 0 ) {
        transformnames = meta.getTransformName();
        transformnrs = transformnames.length;
      } else {
        throw new HopException( BaseMessages.getString( PKG, "BlockUntilTransformsFinish.Error.NotTransforms" ) );
      }
      // Get target transformnames
      String[] targetTransforms = getPipelineMeta().getNextTransformNames( getTransformMeta() );

      data.componentMap.clear();

      for ( int i = 0; i < transformnrs; i++ ) {
        // We can not get metrics from current transform
        if ( transformnames[ i ].equals( getTransformName() ) ) {
          throw new HopException( "You can not wait for transform [" + transformnames[ i ] + "] to finish!" );
        }
        if ( targetTransforms != null ) {
          // We can not metrics from the target transforms
          for ( int j = 0; j < targetTransforms.length; j++ ) {
            if ( transformnames[ i ].equals( targetTransforms[ j ] ) ) {
              throw new HopException( "You can not get metrics for the target transform [" + targetTransforms[ j ] + "]!" );
            }
          }
        }

        int copyNr = Const.toInt( meta.getTransformCopyNr()[ i ], 0 );
        IEngineComponent component = getDispatcher().findComponent( transformnames[ i ], copyNr );
        if ( component == null ) {
          throw new HopException( "Erreur finding transform [" + transformnames[ i ] + "] nr copy=" + copyNr + "!" );
        }

        data.componentMap.put( i, component );
      }
    } // end if first

    // Wait until all specified transforms have finished!
    while ( data.continueLoop && !isStopped() ) {
      data.continueLoop = false;
      Iterator<Entry<Integer, IEngineComponent>> it = data.componentMap.entrySet().iterator();
      while ( it.hasNext() ) {
        Entry<Integer, IEngineComponent> e = it.next();
        IEngineComponent transform = e.getValue();
        if ( transform.isRunning() ) {
          // This transform is still running...
          data.continueLoop = true;
        } else {
          // We have done with this transform.
          // remove it from the map
          data.componentMap.remove( e.getKey() );
          if ( log.isDetailed() ) {
            logDetailed( "Finished running transform [" + transform.getName() + "(" + transform.getCopyNr() + ")]." );
          }
        }
      }

      if ( data.continueLoop ) {
        try {
          Thread.sleep( 200 );
        } catch ( Exception e ) {
          // ignore
        }
      }
    }

    // All transforms we are waiting for are ended
    // let's now free all incoming rows
    Object[] r = getRow();

    if ( r == null ) {
      // no more input to be expected...
      setOutputDone();
      return false;
    }

    putRow( getInputRowMeta(), r );

    return true;
  }

  @Override
  public boolean init() {
    if ( super.init() ) {
      return true;
    }
    return false;
  }

}
