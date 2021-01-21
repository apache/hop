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

package org.apache.hop.pipeline.transforms.append;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopRowException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.errorhandling.IStream;

import java.util.List;

/**
 * Read all rows from a hop until the end, and then read the rows from another hop.
 *
 * @author Sven Boden
 * @since 3-june-2007
 */
public class Append extends BaseTransform<AppendMeta, AppendData> implements ITransform<AppendMeta, AppendData> {

  private static final Class<?> PKG = Append.class; // For Translator

  public Append( TransformMeta transformMeta, AppendMeta meta, AppendData data, int copyNr, PipelineMeta pipelineMeta,
                 Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  @Override
  public boolean processRow() throws HopException {

    Object[] input = null;
    if ( data.processHead ) {
      input = getRowFrom( data.headRowSet );

      if ( input == null ) {
        // Switch to tail processing
        data.processHead = false;
        data.processTail = true;
      } else {
        if ( data.outputRowMeta == null ) {
          data.outputRowMeta = data.headRowSet.getRowMeta();
        }
      }

    }

    if ( data.processTail ) {
      input = getRowFrom( data.tailRowSet );
      if ( input == null ) {
        setOutputDone();
        return false;
      }
      if ( data.outputRowMeta == null ) {
        data.outputRowMeta = data.tailRowSet.getRowMeta();
      }

      if ( data.firstTail ) {
        data.firstTail = false;

        // Check here for the layout (which has to be the same) when we
        // read the first row of the tail.
        try {
          checkInputLayoutValid( data.headRowSet.getRowMeta(), data.tailRowSet.getRowMeta() );
        } catch ( HopRowException e ) {
          throw new HopException( BaseMessages.getString( PKG, "Append.Exception.InvalidLayoutDetected" ), e );
        }
      }
    }

    if ( input != null ) {
      putRow( data.outputRowMeta, input );
    }

    if ( checkFeedback( getLinesRead() ) ) {
      if ( log.isBasic() ) {
        logBasic( BaseMessages.getString( PKG, "Append.Log.LineNumber" ) + getLinesRead() );
      }
    }

    return true;
  }

  @Override
  public boolean init() {
    if ( super.init() ) {
      data.processHead = true;
      data.processTail = false;
      data.firstTail = true;

      List<IStream> infoStreams = meta.getTransformIOMeta().getInfoStreams();
      IStream headStream = infoStreams.get( 0 );
      IStream tailStream = infoStreams.get( 1 );
      if ( meta.headTransformName != null ) {
        headStream.setTransformMeta( getPipelineMeta().findTransform( meta.headTransformName ) );
      }
      if ( meta.tailTransformName != null ) {
        tailStream.setTransformMeta( getPipelineMeta().findTransform( meta.tailTransformName ) );
      }

      if ( headStream.getTransformName() == null || tailStream.getTransformName() == null ) {
        logError( BaseMessages.getString( PKG, "Append.Log.BothHopsAreNeeded" ) );
      } else {
        try {
          data.headRowSet = findInputRowSet( headStream.getTransformName() );
          data.tailRowSet = findInputRowSet( tailStream.getTransformName() );
          return true;
        } catch ( Exception e ) {
          logError( e.getMessage() );
          return false;
        }
      }
    }
    return false;
  }

  /**
   * Checks whether 2 template rows are compatible for the merge transform.
   *
   * @param referenceRowMeta Reference row
   * @param compareRowMeta   Row to compare to
   * @return true when templates are compatible.
   * @throws HopRowException in case there is a compatibility error.
   */
  protected void checkInputLayoutValid( IRowMeta referenceRowMeta, IRowMeta compareRowMeta ) throws HopRowException {
    if ( referenceRowMeta != null && compareRowMeta != null ) {
      BaseTransform.safeModeChecking( referenceRowMeta, compareRowMeta );
    }
  }

}
