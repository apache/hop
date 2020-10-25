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

package org.apache.hop.pipeline.transforms.univariatestats;

import org.apache.hop.core.xml.XmlHandler;
import org.w3c.dom.Node;

import java.util.Objects;

/**
 * Holds meta information about one univariate stats calculation: source field name and what derived values are to be
 * computed
 *
 * @author Mark Hall (mhall{[at]}pentaho.org
 * @version 1.0
 */
public class UnivariateStatsMetaFunction implements Cloneable {

  public static final String XML_TAG = "univariate_stats";

  private String m_sourceFieldName;
  private boolean m_n = true;
  private boolean m_mean = true;
  private boolean m_stdDev = true;
  private boolean m_min = true;
  private boolean m_max = true;
  private boolean m_median = true;
  private double m_arbitraryPercentile = -1;
  private boolean m_interpolatePercentile = true;

  /**
   * Creates a new <code>UnivariateStatsMetaFunction</code>
   *
   * @param sourceFieldName the name of the input field to compute stats for
   * @param n               output N
   * @param mean            compute and output the mean
   * @param stdDev          compute and output the standard deviation
   * @param min             output the minumum value
   * @param max             output the maximum value
   * @param median          compute and output the median (requires data caching and sorting)
   * @param arbPercentile   compute and output a percentile (0 <= arbPercentile <= 1)
   * @param interpolate     true if interpolation is to be used for percentiles (rather than a simple method). See <a
   *                        href="http://www.itl.nist.gov/div898/handbook/prc/section2/prc252.htm"> The Engineering Statistics
   *                        Handbook</a> for details.
   */
  public UnivariateStatsMetaFunction( String sourceFieldName, boolean n, boolean mean, boolean stdDev,
                                      boolean min, boolean max, boolean median, double arbPercentile, boolean interpolate ) {
    m_sourceFieldName = sourceFieldName;
    m_n = n;
    m_mean = mean;
    m_stdDev = stdDev;
    m_min = min;
    m_max = max;
    m_median = median;
    m_arbitraryPercentile = arbPercentile;
    m_interpolatePercentile = interpolate;
  }

  /**
   * Construct from an XML node
   *
   * @param uniNode a XML node
   */
  public UnivariateStatsMetaFunction( Node uniNode ) {
    String temp;
    m_sourceFieldName = XmlHandler.getTagValue( uniNode, "source_field_name" );

    temp = XmlHandler.getTagValue( uniNode, "N" );
    if ( temp.equalsIgnoreCase( "N" ) ) {
      m_n = false;
    }
    temp = XmlHandler.getTagValue( uniNode, "mean" );
    if ( temp.equalsIgnoreCase( "N" ) ) {
      m_mean = false;
    }

    temp = XmlHandler.getTagValue( uniNode, "stdDev" );
    if ( temp.equalsIgnoreCase( "N" ) ) {
      m_stdDev = false;
    }

    temp = XmlHandler.getTagValue( uniNode, "min" );
    if ( temp.equalsIgnoreCase( "N" ) ) {
      m_min = false;
    }

    temp = XmlHandler.getTagValue( uniNode, "max" );
    if ( temp.equalsIgnoreCase( "N" ) ) {
      m_max = false;
    }

    temp = XmlHandler.getTagValue( uniNode, "median" );
    if ( temp.equalsIgnoreCase( "N" ) ) {
      m_median = false;
    }

    temp = XmlHandler.getTagValue( uniNode, "percentile" );
    try {
      m_arbitraryPercentile = Double.parseDouble( temp );
    } catch ( Exception ex ) {
      m_arbitraryPercentile = -1;
    }

    temp = XmlHandler.getTagValue( uniNode, "interpolate" );
    if ( temp.equalsIgnoreCase( "N" ) ) {
      m_interpolatePercentile = false;
    }
  }

  /**
   * Check for equality
   *
   * @param obj an UnivarateStatsMetaFunction to compare against
   * @return true if this Object and the supplied one are the same
   */
  public boolean equals( Object obj ) {
    if ( ( obj != null ) && ( obj.getClass().equals( this.getClass() ) ) ) {
      UnivariateStatsMetaFunction mf = (UnivariateStatsMetaFunction) obj;

      return ( getXml().equals( mf.getXml() ) );
    }

    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash( m_sourceFieldName, m_n, m_mean, m_stdDev, m_min, m_max, m_median, m_arbitraryPercentile,
      m_interpolatePercentile );
  }

  /**
   * Return a String containing XML describing this UnivariateStatsMetaFunction
   *
   * @return an XML description of this UnivarateStatsMetaFunction
   */
  public String getXml() {
    String xml = ( "<" + XML_TAG + ">" );

    xml += XmlHandler.addTagValue( "source_field_name", m_sourceFieldName );
    xml += XmlHandler.addTagValue( "N", m_n );
    xml += XmlHandler.addTagValue( "mean", m_mean );
    xml += XmlHandler.addTagValue( "stdDev", m_stdDev );
    xml += XmlHandler.addTagValue( "min", m_min );
    xml += XmlHandler.addTagValue( "max", m_max );
    xml += XmlHandler.addTagValue( "median", m_median );
    xml += XmlHandler.addTagValue( "percentile", "" + m_arbitraryPercentile );
    xml += XmlHandler.addTagValue( "interpolate", m_interpolatePercentile );

    xml += ( "</" + XML_TAG + ">" );

    return xml;
  }

  /**
   * Make a copy
   *
   * @return a copy of this UnivariateStatsMetaFunction.
   */
  public Object clone() {
    try {
      UnivariateStatsMetaFunction retval = (UnivariateStatsMetaFunction) super.clone();

      return retval;
    } catch ( CloneNotSupportedException e ) {
      return null;
    }
  }

  /**
   * Set the name of the input field used by this UnivariateStatsMetaFunction.
   *
   * @param sn the name of the source field to use
   */
  public void setSourceFieldName( String sn ) {
    m_sourceFieldName = sn;
  }

  /**
   * Return the name of the input field used by this UnivariateStatsMetaFunction
   *
   * @return the name of the input field used
   */
  public String getSourceFieldName() {
    return m_sourceFieldName;
  }

  /**
   * Set whether to calculate N for this input field
   *
   * @param n true if N is to be calculated
   */
  public void setCalcN( boolean n ) {
    m_n = n;
  }

  /**
   * Get whether N is to be calculated for this input field
   *
   * @return true if N is to be calculated
   */
  public boolean getCalcN() {
    return m_n;
  }

  /**
   * Set whether to calculate the mean for this input field
   *
   * @param b true if the mean is to be calculated
   */
  public void setCalcMean( boolean b ) {
    m_mean = b;
  }

  /**
   * Get whether the mean is to be calculated for this input field
   *
   * @return true if the mean is to be calculated
   */
  public boolean getCalcMean() {
    return m_mean;
  }

  /**
   * Set whether the standard deviation is to be calculated for this input value
   *
   * @param b true if the standard deviation is to be calculated
   */
  public void setCalcStdDev( boolean b ) {
    m_stdDev = b;
  }

  /**
   * Get whether the standard deviation is to be calculated for this input value
   *
   * @return true if the standard deviation is to be calculated
   */
  public boolean getCalcStdDev() {
    return m_stdDev;
  }

  /**
   * Set whether the minimum is to be calculated for this input value
   *
   * @param b true if the minimum is to be calculated
   */
  public void setCalcMin( boolean b ) {
    m_min = b;
  }

  /**
   * Get whether the minimum is to be calculated for this input value
   *
   * @return true if the minimum is to be calculated
   */
  public boolean getCalcMin() {
    return m_min;
  }

  /**
   * Set whether the maximum is to be calculated for this input value
   *
   * @param b true if the maximum is to be calculated
   */
  public void setCalcMax( boolean b ) {
    m_max = b;
  }

  /**
   * Get whether the maximum is to be calculated for this input value
   *
   * @return true if the maximum is to be calculated
   */
  public boolean getCalcMax() {
    return m_max;
  }

  /**
   * Set whether the median is to be calculated for this input value
   *
   * @param b true if the median is to be calculated
   */
  public void setCalcMedian( boolean b ) {
    m_median = b;
  }

  /**
   * Get whether the median is to be calculated for this input value
   *
   * @return true if the median is to be calculated
   */
  public boolean getCalcMedian() {
    return m_median;
  }

  /**
   * Get whether interpolation is to be used in the computation of percentiles
   *
   * @return true if interpolation is to be used
   */
  public boolean getInterpolatePercentile() {
    return m_interpolatePercentile;
  }

  /**
   * Set whether interpolation is to be used in the computation of percentiles
   *
   * @param i true is interpolation is to be used
   */
  public void setInterpolatePercentile( boolean i ) {
    m_interpolatePercentile = i;
  }

  /**
   * Gets whether an arbitrary percentile is to be calculated for this input field
   *
   * @return true if a percentile is to be computed
   */
  public double getCalcPercentile() {
    return m_arbitraryPercentile;
  }

  /**
   * Sets whether an arbitrary percentile is to be calculated for this input field
   *
   * @param percentile the percentile to compute (0 <= percentile <= 100)
   */
  public void setCalcPercentile( double percentile ) {
    if ( percentile < 0 ) {
      m_arbitraryPercentile = -1; // not used
      return;
    }

    if ( percentile >= 0 && percentile <= 100 ) {
      m_arbitraryPercentile = percentile / 100.0;
      return;
    }

    m_arbitraryPercentile = -1; // not used
  }

  /**
   * Returns the number of metrics to compute
   *
   * @return the number of metrics to compute
   */
  public int numberOfMetricsRequested() {
    int num = 0;

    if ( getCalcN() ) {
      num++;
    }
    if ( getCalcMean() ) {
      num++;
    }
    if ( getCalcStdDev() ) {
      num++;
    }
    if ( getCalcMin() ) {
      num++;
    }
    if ( getCalcMax() ) {
      num++;
    }
    if ( getCalcMedian() ) {
      num++;
    }
    if ( getCalcPercentile() >= 0 ) {
      num++;
    }
    return num;
  }
}
