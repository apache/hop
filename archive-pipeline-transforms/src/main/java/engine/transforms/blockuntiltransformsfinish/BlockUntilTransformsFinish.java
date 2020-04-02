/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.blockuntiltransformsfinish;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.BaseTransformData.TransformExecutionStatus;
import org.apache.hop.pipeline.transform.TransformDataInterface;
import org.apache.hop.pipeline.transform.TransformInterface;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInterface;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Block all incoming rows until defined transforms finish processing rows.
 *
 * @author Samatar
 * @since 30-06-2008
 */

public class BlockUntilTransformsFinish extends BaseTransform implements TransformInterface {
  private static Class<?> PKG = BlockUntilTransformsFinishMeta.class; // for i18n purposes, needed by Translator!!

  private BlockUntilTransformsFinishMeta meta;
  private BlockUntilTransformsFinishData data;

  public BlockUntilTransformsFinish( TransformMeta transformMeta, TransformDataInterface transformDataInterface, int copyNr,
                                     PipelineMeta pipelineMeta, Pipeline pipeline ) {
    super( transformMeta, transformDataInterface, copyNr, pipelineMeta, pipeline );
  }

  public boolean processRow( TransformMetaInterface smi, TransformDataInterface sdi ) throws HopException {
    meta = (BlockUntilTransformsFinishMeta) smi;
    data = (BlockUntilTransformsFinishData) sdi;

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

      data.transformInterfaces = new ConcurrentHashMap<Integer, TransformInterface>();
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

        int CopyNr = Const.toInt( meta.getTransformCopyNr()[ i ], 0 );
        TransformInterface transform = getDispatcher().findBaseTransforms( transformnames[ i ] ).get( CopyNr );
        if ( transform == null ) {
          throw new HopException( "Erreur finding transform [" + transformnames[ i ] + "] nr copy=" + CopyNr + "!" );
        }

        data.transformInterfaces.put( i, getDispatcher().findBaseTransforms( transformnames[ i ] ).get( CopyNr ) );
      }
    } // end if first

    // Wait until all specified transforms have finished!
    while ( data.continueLoop && !isStopped() ) {
      data.continueLoop = false;
      Iterator<Entry<Integer, TransformInterface>> it = data.transformInterfaces.entrySet().iterator();
      while ( it.hasNext() ) {
        Entry<Integer, TransformInterface> e = it.next();
        TransformInterface transform = e.getValue();
        if ( transform.getStatus() != TransformExecutionStatus.STATUS_FINISHED ) {
          // This transform is still running...
          data.continueLoop = true;
        } else {
          // We have done with this transform.
          // remove it from the map
          data.transformInterfaces.remove( e.getKey() );
          if ( log.isDetailed() ) {
            logDetailed( "Finished running transform [" + transform.getTransformName() + "(" + transform.getCopy() + ")]." );
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

  public boolean init( TransformMetaInterface smi, TransformDataInterface sdi ) {
    meta = (BlockUntilTransformsFinishMeta) smi;
    data = (BlockUntilTransformsFinishData) sdi;

    if ( super.init( smi, sdi ) ) {
      return true;
    }
    return false;
  }

}
