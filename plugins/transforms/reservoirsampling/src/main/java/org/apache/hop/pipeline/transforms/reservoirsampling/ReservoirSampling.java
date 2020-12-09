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

package org.apache.hop.pipeline.transforms.reservoirsampling;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.reservoirsampling.ReservoirSamplingData.PROC_MODE;

import java.util.Arrays;
import java.util.List;

public class ReservoirSampling extends BaseTransform<ReservoirSamplingMeta, ReservoirSamplingData> implements ITransform<ReservoirSamplingMeta, ReservoirSamplingData> {


  /**
   * Creates a new <code>ReservoirSampling</code> instance.
   * <p>
   * <p>
   * Implements the reservoir sampling algorithm "R" by Jeffrey Scott Vitter. (algorithm is implemented in
   * ReservoirSamplingData.java
   * <p>
   * For more information see:<br>
   * <br>
   * <p>
   * Vitter, J. S. Random Sampling with a Reservoir. ACM Transactions on Mathematical Software, Vol. 11, No. 1, March
   * 1985. Pages 37-57.
   *
   * @param transformMeta          holds the transform's meta data
   * @param meta
   * @param data holds the transform's temporary data
   * @param copyNr            the number assigned to the transform
   * @param pipelineMeta         meta data for the pipeline
   * @param pipeline             a <code>Pipeline</code> value
   */
  public ReservoirSampling( TransformMeta transformMeta, ReservoirSamplingMeta meta, ReservoirSamplingData data, int copyNr,
                            PipelineMeta pipelineMeta, Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  /**
   * Process an incoming row of data.
   *
   * @return a <code>boolean</code> value
   * @throws HopException if an error occurs
   */
  public boolean processRow() throws HopException {

    if ( data.getProcessingMode() == PROC_MODE.DISABLED ) {
      setOutputDone();
      data.cleanUp();
      return ( false );
    }

    Object[] r = getRow();

    // Handle the first row
    if ( first ) {
      first = false;
      if ( r == null ) { // no input to be expected...

        setOutputDone();
        return false;
      }

      // Initialize the data object
      data.setOutputRowMeta( getInputRowMeta().clone() );
      String sampleSize = resolve( meta.getSampleSize() );
      String seed = resolve( meta.getSeed() );
      data.initialize( Integer.valueOf( sampleSize ), Integer.valueOf( seed ) );

      // no real reason to determine the output fields here
      // as we don't add/delete any fields
    } // end (if first)

    if ( data.getProcessingMode() == PROC_MODE.PASSTHROUGH ) {
      if ( r == null ) {
        setOutputDone();
        data.cleanUp();
        return ( false );
      }
      putRow( data.getOutputRowMeta(), r );
    } else if ( data.getProcessingMode() == PROC_MODE.SAMPLING ) {
      if ( r == null ) {
        // Output the rows in the sample
        List<Object[]> samples = data.getSample();

        int numRows = ( samples != null ) ? samples.size() : 0;
        logBasic( this.getTransformName()
          + " Actual/Sample: " + numRows + "/" + data.m_k + " Seed:"
          + resolve( meta.m_randomSeed ) );
        if ( samples != null ) {
          for ( int i = 0; i < samples.size(); i++ ) {
            Object[] sample = samples.get( i );
            if ( sample != null ) {
              putRow( data.getOutputRowMeta(), sample );
            } else {
              // user probably requested more rows in
              // the sample than there were in total
              // in the end. Just break in this case
              break;
            }
          }
        }
        setOutputDone();
        data.cleanUp();
        return false;
      }

      // just pass the row to the data class for possible caching
      // in the sample
      data.processRow( r );
    }

    if ( log.isRowLevel() ) {
      logRowlevel( "Read row #" + getLinesRead() + " : " + Arrays.toString( r ) );
    }

    if ( checkFeedback( getLinesRead() ) ) {
      logBasic( "Line number " + getLinesRead() );
    }
    return true;
  }

  /**
   * Initialize the transform.
   *
   * @return a <code>boolean</code> value
   */
  public boolean init() {

    if ( super.init() ) {

//      boolean remoteInput = getTransformMeta().getRemoteInputTransforms().size() > 0;
      List<TransformMeta> previous = getPipelineMeta().findPreviousTransforms( getTransformMeta() );
//      if ( !remoteInput && ( previous == null || previous.size() <= 0 ) ) {
//        data.setProcessingMode( PROC_MODE.DISABLED );
//      }
      return true;
    }
    return false;
  }

  /**
   * Run is where the action happens!
   */
  public void run() {
    logBasic( "Starting to run..." );
    try {
      // Wait
      while (processRow()) {
        if ( isStopped() ) {
          break;
        }
      }
    } catch ( Exception e ) {
      logError( "Unexpected error : " + e.toString() );
      logError( Const.getStackTracker( e ) );
      setErrors( 1 );
      stopAll();
    } finally {
      dispose();
      logBasic( "Finished, processing " + getLinesRead() + " rows" );
      markStop();
    }
  }
}
