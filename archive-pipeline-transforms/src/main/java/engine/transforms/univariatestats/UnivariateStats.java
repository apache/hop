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

package org.apache.hop.pipeline.transforms.univariatestats;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInterface;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Calculate univariate statistics based on one column of the input data.
 * <p>
 * Calculates N, mean, standard deviation, minimum, maximum, median and arbitrary percentiles. Percentiles can be
 * calculated using interpolation or a simple method. See <a
 * href="http://www.itl.nist.gov/div898/handbook/prc/section2/prc252.htm"> The Engineering Statistics Handbook</a> for
 * details.
 *
 * @author Mark Hall (mhall{[at]}pentaho.org)
 * @version 1.0
 */
public class UnivariateStats extends BaseTransform implements ITransform {

  private UnivariateStatsMeta m_meta;
  private UnivariateStatsData m_data;

  /**
   * holds cached input values if median/percentiles are to be calculated
   */
  private ArrayList<Number>[] m_dataCache;

  /**
   * Creates a new <code>UnivariateStats</code> instance.
   *
   * @param transformMeta          holds the transform's meta data
   * @param iTransformData holds the transform's temporary data
   * @param copyNr            the number assigned to the transform
   * @param pipelineMeta         meta data for the pipeline
   * @param pipeline             a <code>Pipeline</code> value
   */
  public UnivariateStats( TransformMeta transformMeta, ITransformData iTransformData, int copyNr, PipelineMeta pipelineMeta,
                          Pipeline pipeline ) {
    super( transformMeta, iTransformData, copyNr, pipelineMeta, pipeline );
  }

  /**
   * Process an incoming row of data.
   *
   * @param smi a <code>TransformMetaInterface</code> value
   * @param sdi a <code>ITransformData</code> value
   * @return a <code>boolean</code> value
   * @throws HopException if an error occurs
   */
  @SuppressWarnings( { "unchecked" } )
  public boolean processRow( TransformMetaInterface smi, ITransformData sdi ) throws HopException {

    m_meta = (UnivariateStatsMeta) smi;
    m_data = (UnivariateStatsData) sdi;

    Object[] r = getRow(); // get row, set busy!
    if ( r == null ) { // no more input to be expected...

      // compute the derived stats and generate an output row
      Object[] outputRow = generateOutputRow();
      // emit the single output row
      putRow( m_data.getOutputRowMeta(), outputRow );
      setOutputDone();

      // save memory
      m_dataCache = null;

      return false;
    }

    // Handle the first row
    if ( first ) {
      first = false;
      // Don't want to clone and add to the input meta data - want
      // to create a new row meta data for derived calculations
      IRowMeta outputMeta = new RowMeta();

      m_data.setInputRowMeta( getInputRowMeta() );
      m_data.setOutputRowMeta( outputMeta );

      // Determine the output format
      m_meta.getFields( m_data.getOutputRowMeta(), getTransformName(), null, null, this, metaStore );

      // Set up data cache for calculating median/percentiles
      m_dataCache = new ArrayList[ m_meta.getNumFieldsToProcess() ];

      // Initialize the transform meta data
      FieldIndex[] fi = new FieldIndex[ m_meta.getNumFieldsToProcess() ];

      m_data.setFieldIndexes( fi );

      // allocate the field indexes in the data class and meta stats functions
      // in the transform meta
      for ( int i = 0; i < m_meta.getNumFieldsToProcess(); i++ ) {
        UnivariateStatsMetaFunction usmf = m_meta.getInputFieldMetaFunctions()[ i ];
        //CHECKSTYLE:Indentation:OFF
        m_data.getFieldIndexes()[ i ] = new FieldIndex();

        // check that this univariate stats computation has been
        // defined on an input field
        if ( !Utils.isEmpty( usmf.getSourceFieldName() ) ) {
          int fieldIndex = m_data.getInputRowMeta().indexOfValue( usmf.getSourceFieldName() );

          if ( fieldIndex < 0 ) {
            throw new HopTransformException( "Unable to find the specified fieldname '"
              + usmf.getSourceFieldName() + "' for stats calc #" + ( i + 1 ) );
          }

          FieldIndex tempData = m_data.getFieldIndexes()[ i ];

          tempData.m_columnIndex = fieldIndex;

          IValueMeta inputFieldMeta = m_data.getInputRowMeta().getValueMeta( fieldIndex );

          // check the type of the input field
          if ( !inputFieldMeta.isNumeric() ) {
            throw new HopException( "The input field for stats calc #" + ( i + 1 ) + "is not numeric." );
          }

          // finish initializing
          tempData.m_min = Double.MAX_VALUE;
          tempData.m_max = Double.MIN_VALUE;

          // set up caches if median/percentiles have been
          // requested

          if ( usmf.getCalcMedian() || usmf.getCalcPercentile() >= 0 ) {
            m_dataCache[ i ] = new ArrayList<Number>();
          }
        } else {
          throw new HopException( "There is no input field specified for stats calc #" + ( i + 1 ) );
        }
      }
    } // end (if first)

    for ( int i = 0; i < m_meta.getNumFieldsToProcess(); i++ ) {

      UnivariateStatsMetaFunction usmf = m_meta.getInputFieldMetaFunctions()[ i ];
      if ( !Utils.isEmpty( usmf.getSourceFieldName() ) ) {
        FieldIndex tempData = m_data.getFieldIndexes()[ i ];

        IValueMeta metaI = getInputRowMeta().getValueMeta( tempData.m_columnIndex );

        Number input = null;
        try {
          input = metaI.getNumber( r[ tempData.m_columnIndex ] );
        } catch ( Exception ex ) {
          // quietly ignore -- assume missing for anything not
          // parsable as a number
        }
        if ( input != null ) {

          // add to the cache?
          if ( usmf.getCalcMedian() || usmf.getCalcPercentile() >= 0 ) {
            m_dataCache[ i ].add( input );
          }

          // update stats
          double val = input.doubleValue();
          tempData.m_count++;
          tempData.m_sum += val;
          tempData.m_sumSq += ( val * val );
          if ( val < tempData.m_min ) {
            tempData.m_min = val;
          }
          if ( val > tempData.m_max ) {
            tempData.m_max = val;
          }

        } // otherwise, treat non-numeric values as missing
      }
    }

    if ( log.isRowLevel() ) {
      logRowlevel( "Read row #" + getLinesRead() + " : " + Arrays.toString( r ) );
    }

    if ( checkFeedback( getLinesRead() ) ) {
      logBasic( "Linenr " + getLinesRead() );
    }
    return true;
  }

  /**
   * Generates an output row
   *
   * @return an <code>Object[]</code> value
   */
  private Object[] generateOutputRow() {

    int totalNumOutputFields = 0;

    for ( int i = 0; i < m_meta.getNumFieldsToProcess(); i++ ) {
      UnivariateStatsMetaFunction usmf = m_meta.getInputFieldMetaFunctions()[ i ];

      if ( !Utils.isEmpty( usmf.getSourceFieldName() ) ) {
        totalNumOutputFields += usmf.numberOfMetricsRequested();
      }
    }

    Object[] result = new Object[ totalNumOutputFields ];
    int index = 0;
    for ( int i = 0; i < m_meta.getNumFieldsToProcess(); i++ ) {
      UnivariateStatsMetaFunction usmf = m_meta.getInputFieldMetaFunctions()[ i ];

      if ( !Utils.isEmpty( usmf.getSourceFieldName() ) ) {
        Object[] tempOut = m_data.getFieldIndexes()[ i ].generateOutputValues( usmf, m_dataCache[ i ] );

        for ( int j = 0; j < tempOut.length; j++ ) {
          result[ index++ ] = tempOut[ j ];
        }
      }
    }

    return result;
  }

  /**
   * Initialize the transform.
   *
   * @param smi a <code>TransformMetaInterface</code> value
   * @param sdi a <code>ITransformData</code> value
   * @return a <code>boolean</code> value
   */
  public boolean init( TransformMetaInterface smi, ITransformData sdi ) {
    m_meta = (UnivariateStatsMeta) smi;
    m_data = (UnivariateStatsData) sdi;

    if ( super.init( smi, sdi ) ) {
      return true;
    }
    return false;
  }
}
