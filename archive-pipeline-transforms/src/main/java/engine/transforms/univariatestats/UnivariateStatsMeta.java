/*! ******************************************************************************
 *

 * Hop : The Hop Orchestration Platform
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

package org.apache.hop.pipeline.transforms.univariatestats;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.w3c.dom.Node;

import java.text.NumberFormat;
import java.util.Arrays;
import java.util.List;

/**
 * Contains the meta-data for the UnivariateStats transform: calculates predefined univariate statistics
 *
 * @author Mark Hall (mhall{[at]}pentaho.org)
 * @version 1.0
 */
public class UnivariateStatsMeta extends BaseTransformMeta implements ITransform {

  // The stats to be computed for various input fields.
  // User may elect to omit some stats for particular fields.
  private UnivariateStatsMetaFunction[] m_stats;

  /**
   * Creates a new <code>UnivariateStatsMeta</code> instance.
   */
  public UnivariateStatsMeta() {
    super(); // allocate BaseTransformMeta
  }

  /**
   * Get the stats to be computed for the input fields
   *
   * @return an <code>UnivariateStatsMetaFunction[]</code> value
   */
  public UnivariateStatsMetaFunction[] getInputFieldMetaFunctions() {
    return m_stats;
  }

  /**
   * Returns how many UnivariateStatsMetaFunctions are currently being used. Each UnivariateStatsMetaFunction represents
   * an input field to be processed along with the user-requested stats to compute for it. The same input field may
   * occur in more than one UnivariateStatsMetaFunction as more than one percentile may be required.
   *
   * @return the number of non-unique input fields
   */
  public int getNumFieldsToProcess() {
    return m_stats.length;
  }

  /**
   * Set the stats to be computed for the input fields
   *
   * @param mf an array of <code>UnivariateStatsMetaFunction</code>s
   */
  public void setInputFieldMetaFunctions( UnivariateStatsMetaFunction[] mf ) {
    m_stats = mf;
  }

  /**
   * Allocate variables for stats to compute
   *
   * @param nrStats the number of UnivariateStatsMetaFunctions to allocate
   */
  public void allocate( int nrStats ) {
    m_stats = new UnivariateStatsMetaFunction[ nrStats ];
  }

  /**
   * Loads the meta data for this (configured) transform from XML.
   *
   * @param transformNode the transform to load
   * @throws HopXmlException if an error occurs
   */
  @Override
  public void loadXml( Node transformNode, IHopMetadataProvider metadataProvider ) throws HopXmlException {

    int nrStats = XmlHandler.countNodes( transformNode, UnivariateStatsMetaFunction.XML_TAG );

    allocate( nrStats );
    for ( int i = 0; i < nrStats; i++ ) {
      Node statnode = XmlHandler.getSubNodeByNr( transformNode, UnivariateStatsMetaFunction.XML_TAG, i );
      m_stats[ i ] = new UnivariateStatsMetaFunction( statnode );
    }
  }

  /**
   * Return the XML describing this (configured) transform
   *
   * @return a <code>String</code> containing the XML
   */
  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder( 300 );

    if ( m_stats != null ) {
      for ( int i = 0; i < m_stats.length; i++ ) {
        retval.append( "       " ).append( m_stats[ i ].getXml() ).append( Const.CR );
      }
    }
    return retval.toString();
  }

  /**
   * Check for equality
   *
   * @param obj an <code>Object</code> to compare with
   * @return true if equal to the supplied object
   */
  @Override
  public boolean equals( Object obj ) {
    if ( obj != null && ( obj.getClass().equals( this.getClass() ) ) ) {
      UnivariateStatsMeta m = (UnivariateStatsMeta) obj;
      return ( getXml().equals( m.getXml() ) );
    }

    return false;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode( m_stats );
  }

  /**
   * Clone this transform's meta data
   *
   * @return the cloned meta data
   */
  @Override
  public Object clone() {
    UnivariateStatsMeta retval = (UnivariateStatsMeta) super.clone();
    if ( m_stats != null ) {
      retval.allocate( m_stats.length );
      for ( int i = 0; i < m_stats.length; i++ ) {
        // CHECKSTYLE:Indentation:OFF
        retval.getInputFieldMetaFunctions()[ i ] = (UnivariateStatsMetaFunction) m_stats[ i ].clone();
      }
    } else {
      retval.allocate( 0 );
    }
    return retval;
  }

  /**
   * Set the default state of the meta data?
   */
  @Override
  public void setDefault() {
    m_stats = new UnivariateStatsMetaFunction[ 0 ];
  }

  @Override
  public void getFields( IRowMeta row, String origin, IRowMeta[] info, TransformMeta nextTransform,
                         IVariables variables, IHopMetadataProvider metadataProvider ) throws HopTransformException {

    row.clear();
    for ( int i = 0; i < m_stats.length; i++ ) {
      UnivariateStatsMetaFunction fn = m_stats[ i ];

      IValueMeta[] vmis = getValueMetas( fn, origin );

      for ( int j = 0; j < vmis.length; j++ ) {
        row.addValueMeta( vmis[ j ] );
      }
    }
  }

  /**
   * Returns an array of IValueMeta that contains the meta data for each value computed by the supplied
   * UnivariateStatsMetaFunction
   *
   * @param fn     the <code>UnivariateStatsMetaFunction</code> to construct meta data for
   * @param origin the origin
   * @return an array of meta data
   */
  private IValueMeta[] getValueMetas( UnivariateStatsMetaFunction fn, String origin ) {

    IValueMeta[] v = new IValueMeta[ fn.numberOfMetricsRequested() ];

    int index = 0;
    if ( fn.getCalcN() ) {
      v[ index ] = new ValueMetaNumber( fn.getSourceFieldName() + "(N)" );
      v[ index ].setOrigin( origin );
      index++;
    }

    if ( fn.getCalcMean() ) {
      v[ index ] = new ValueMetaNumber( fn.getSourceFieldName() + "(mean)" );
      v[ index ].setOrigin( origin );
      index++;
    }

    if ( fn.getCalcStdDev() ) {
      v[ index ] = new ValueMetaNumber( fn.getSourceFieldName() + "(stdDev)" );
      v[ index ].setOrigin( origin );
      index++;
    }

    if ( fn.getCalcMin() ) {
      v[ index ] = new ValueMetaNumber( fn.getSourceFieldName() + "(min)" );
      v[ index ].setOrigin( origin );
      index++;
    }

    if ( fn.getCalcMax() ) {
      v[ index ] = new ValueMetaNumber( fn.getSourceFieldName() + "(max)" );
      v[ index ].setOrigin( origin );
      index++;
    }

    if ( fn.getCalcMedian() ) {
      v[ index ] = new ValueMetaNumber( fn.getSourceFieldName() + "(median)" );
      v[ index ].setOrigin( origin );
      index++;
    }

    if ( fn.getCalcPercentile() >= 0 ) {
      double percent = fn.getCalcPercentile();
      // NumberFormat pF = NumberFormat.getPercentInstance();
      NumberFormat pF = NumberFormat.getInstance();
      pF.setMaximumFractionDigits( 2 );
      String res = pF.format( percent * 100 );
      v[ index ] = new ValueMetaNumber( fn.getSourceFieldName() + "(" + res + "th percentile)" );
      v[ index ].setOrigin( origin );
      index++;
    }
    return v;
  }

  /**
   * Check the settings of this transform and put findings in a remarks list.
   *
   * @param remarks   the list to put the remarks in. see <code>org.apache.hop.core.CheckResult</code>
   * @param pipelineMeta the transform meta data
   * @param transformMeta  the transform meta data
   * @param prev      the fields coming from a previous transform
   * @param input     the input transform names
   * @param output    the output transform names
   * @param info      the fields that are used as information by the transform
   */
  @Override
  public void check( List<ICheckResult> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta, IRowMeta prev,
                     String[] input, String[] output, IRowMeta info, IVariables variables,
                     IHopMetadataProvider metadataProvider ) {

    CheckResult cr;

    if ( ( prev == null ) || ( prev.size() == 0 ) ) {
      cr = new CheckResult( CheckResult.TYPE_RESULT_WARNING, "Not receiving any fields from previous transforms!", transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, "Transform is connected to previous one, receiving " + prev.size()
          + " fields", transformMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK, "Transform is receiving info from other transforms.", transformMeta );
      remarks.add( cr );
    } else {
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, "No input received from other transforms!", transformMeta );
      remarks.add( cr );
    }
  }

  /**
   * Get the executing transform, needed by Pipeline to launch a transform.
   *
   * @param transformMeta          the transform info
   * @param iTransformData the transform data interface linked to this transform. Here the transform can store temporary data, database connections,
   *                          etc.
   * @param cnr               the copy number to get.
   * @param tr                the pipeline info.
   * @param pipeline             the launching pipeline
   * @return a <code>ITransform</code> value
   */
  @Override
  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr, PipelineMeta tr,
                                Pipeline pipeline ) {
    return new UnivariateStats( transformMeta, this, data, cnr, tr, pipeline );
  }

  /**
   * Get a new instance of the appropriate data class. This data class implements the ITransformData. It basically
   * contains the persisting data that needs to live on, even if a worker thread is terminated.
   *
   * @return a <code>ITransformData</code> value
   */
  @Override
  public ITransformData getTransformData() {
    return new UnivariateStatsData();
  }
}
