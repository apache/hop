/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.reservoirsampling;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformDataInterface;
import org.apache.hop.pipeline.transform.TransformInterface;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInterface;
import org.apache.hop.pipeline.transforms.reservoirsampling.ReservoirSamplingData.PROC_MODE;

import java.util.Arrays;
import java.util.List;

public class ReservoirSampling extends BaseTransform implements TransformInterface {

  private ReservoirSamplingMeta m_meta;
  private ReservoirSamplingData m_data;

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
   * @param transformDataInterface holds the transform's temporary data
   * @param copyNr            the number assigned to the transform
   * @param pipelineMeta         meta data for the pipeline
   * @param pipeline             a <code>Pipeline</code> value
   */
  public ReservoirSampling( TransformMeta transformMeta, TransformDataInterface transformDataInterface, int copyNr,
                            PipelineMeta pipelineMeta, Pipeline pipeline ) {
    super( transformMeta, transformDataInterface, copyNr, pipelineMeta, pipeline );
  }

  /**
   * Process an incoming row of data.
   *
   * @param smi a <code>TransformMetaInterface</code> value
   * @param sdi a <code>TransformDataInterface</code> value
   * @return a <code>boolean</code> value
   * @throws HopException if an error occurs
   */
  public boolean processRow( TransformMetaInterface smi, TransformDataInterface sdi ) throws HopException {

    if ( m_data.getProcessingMode() == PROC_MODE.DISABLED ) {
      setOutputDone();
      m_data.cleanUp();
      return ( false );
    }

    m_meta = (ReservoirSamplingMeta) smi;
    m_data = (ReservoirSamplingData) sdi;

    Object[] r = getRow();

    // Handle the first row
    if ( first ) {
      first = false;
      if ( r == null ) { // no input to be expected...

        setOutputDone();
        return false;
      }

      // Initialize the data object
      m_data.setOutputRowMeta( getInputRowMeta().clone() );
      String sampleSize = getPipelineMeta().environmentSubstitute( m_meta.getSampleSize() );
      String seed = getPipelineMeta().environmentSubstitute( m_meta.getSeed() );
      m_data.initialize( Integer.valueOf( sampleSize ), Integer.valueOf( seed ) );

      // no real reason to determine the output fields here
      // as we don't add/delete any fields
    } // end (if first)

    if ( m_data.getProcessingMode() == PROC_MODE.PASSTHROUGH ) {
      if ( r == null ) {
        setOutputDone();
        m_data.cleanUp();
        return ( false );
      }
      putRow( m_data.getOutputRowMeta(), r );
    } else if ( m_data.getProcessingMode() == PROC_MODE.SAMPLING ) {
      if ( r == null ) {
        // Output the rows in the sample
        List<Object[]> samples = m_data.getSample();

        int numRows = ( samples != null ) ? samples.size() : 0;
        logBasic( this.getTransformName()
          + " Actual/Sample: " + numRows + "/" + m_data.m_k + " Seed:"
          + getPipelineMeta().environmentSubstitute( m_meta.m_randomSeed ) );
        if ( samples != null ) {
          for ( int i = 0; i < samples.size(); i++ ) {
            Object[] sample = samples.get( i );
            if ( sample != null ) {
              putRow( m_data.getOutputRowMeta(), sample );
            } else {
              // user probably requested more rows in
              // the sample than there were in total
              // in the end. Just break in this case
              break;
            }
          }
        }
        setOutputDone();
        m_data.cleanUp();
        return false;
      }

      // just pass the row to the data class for possible caching
      // in the sample
      m_data.processRow( r );
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
   * @param smi a <code>TransformMetaInterface</code> value
   * @param sdi a <code>TransformDataInterface</code> value
   * @return a <code>boolean</code> value
   */
  public boolean init( TransformMetaInterface smi, TransformDataInterface sdi ) {
    m_meta = (ReservoirSamplingMeta) smi;
    m_data = (ReservoirSamplingData) sdi;

    if ( super.init( smi, sdi ) ) {

      boolean remoteInput = getTransformMeta().getRemoteInputTransforms().size() > 0;
      List<TransformMeta> previous = getPipelineMeta().findPreviousTransforms( getTransformMeta() );
      if ( !remoteInput && ( previous == null || previous.size() <= 0 ) ) {
        m_data.setProcessingMode( PROC_MODE.DISABLED );
      }
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
      while ( processRow( m_meta, m_data ) ) {
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
      dispose( m_meta, m_data );
      logBasic( "Finished, processing " + getLinesRead() + " rows" );
      markStop();
    }
  }
}
