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

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Class used to hold operating field index, intermediate data and final results for a stats calculation.
 * <p>
 * Has functions to compute the mean, standard deviation and arbitrary percentiles. Percentiles can be computed using
 * interpolation or a simple method. See <a href="http://www.itl.nist.gov/div898/handbook/prc/section2/prc252.htm">
 * The Engineering Statistics Handbook</a> for details.
 */
public class FieldIndex {
  public int mColumnIndex;
  public double mCount;
  public double mMean;
  public double mStdDev;
  public double mMax;
  public double mMin;
  public double mMedian;
  public double mArbitraryPercentile;
  public double mSum;
  public double mSumSq;

  public void calculateDerived() {
    mMean = Double.NaN;
    mStdDev = Double.NaN;
    if ( mCount > 0 ) {
      mMean = mSum / mCount;
      mStdDev = Double.POSITIVE_INFINITY;
      if ( mCount > 1 ) {
        mStdDev = mSumSq - ( mSum * mSum ) / mCount;
        mStdDev /= ( mCount - 1 );
        if ( mStdDev < 0 ) {
          // round to zero
          mStdDev = 0;
        }
        mStdDev = Math.sqrt( mStdDev );
      }
    }
  }

  /**
   * Compute a percentile. Can compute percentiles using interpolation or a simple method (see <a
   * href="http://www.itl.nist.gov/div898/handbook/prc/section2/prc252.htm" The Engineering Statistics Handbook</a>
   * for details).
   *
   * @param p           the percentile to compute (0 <= p <= 1)
   * @param vals        a sorted array of values to compute the percentile from
   * @param interpolate true if interpolation is to be used
   * @return the percentile value
   */
  private double percentile( double p, double[] vals, boolean interpolate ) {
    double n = mCount;

    // interpolation
    if ( interpolate ) {
      double i = p * ( n + 1 );
      // special cases
      if ( i <= 1 ) {
        return mMin;
      }
      if ( i >= n ) {
        return mMax;
      }
      double lowObs = Math.floor( i );
      double highObs = lowObs + 1;

      double r1 = highObs - i;
      double r2 = 1.0 - r1;

      double x1 = vals[ (int) lowObs - 1 ];
      double x2 = vals[ (int) highObs - 1 ];

      return ( r1 * x1 ) + ( r2 * x2 );
    }

    // simple method
    double i = p * n;
    double res = 0;
    if ( i == 0 ) {
      return mMin;
    }
    if ( i == n ) {
      return mMax;
    }
    if ( i - Math.floor( i ) > 0 ) {
      i = Math.floor( i );
      res = vals[ (int) i ];
    } else {
      res = ( vals[ (int) ( i - 1 ) ] + vals[ (int) i ] ) / 2.0;
    }
    return res;
  }

  /**
   * Constructs an array of Objects containing the requested statistics for one univariate stats meta function using
   * this <code>FieldIndex</code>.
   *
   * @param usmf the<code>UnivariateStatsMetaFunction</code> to compute stats for. This contains the input field selected
   *             by the user along with which stats to compute for it.
   * @return an array of computed statistics
   */
  public Object[] generateOutputValues( UnivariateStatsMetaFunction usmf, ArrayList<Number> cache ) {
    calculateDerived();

    // process cache?
    if ( cache != null ) {
      double[] result = new double[ (int) mCount ];
      for ( int i = 0; i < cache.size(); i++ ) {
        result[ i ] = cache.get( i ).doubleValue();
      }
      Arrays.sort( result );

      if ( usmf.getCalcMedian() ) {
        mMedian = percentile( 0.5, result, usmf.getInterpolatePercentile() );
      }

      if ( usmf.getCalcPercentile() >= 0 ) {
        mArbitraryPercentile = percentile( usmf.getCalcPercentile(), result, usmf.getInterpolatePercentile() );
      }
    }

    Object[] result = new Object[ usmf.numberOfMetricsRequested() ];

    int index = 0;
    if ( usmf.getCalcN() ) {
      result[ index++ ] = new Double( mCount );
    }
    if ( usmf.getCalcMean() ) {
      result[ index++ ] = new Double( mMean );
    }
    if ( usmf.getCalcStdDev() ) {
      result[ index++ ] = new Double( mStdDev );
    }
    if ( usmf.getCalcMin() ) {
      result[ index++ ] = new Double( mMin );
    }
    if ( usmf.getCalcMax() ) {
      result[ index++ ] = new Double( mMax );
    }
    if ( usmf.getCalcMedian() ) {
      result[ index++ ] = new Double( mMedian );
    }
    if ( usmf.getCalcPercentile() >= 0 ) {
      result[ index++ ] = new Double( mArbitraryPercentile );
    }
    return result;
  }
}
