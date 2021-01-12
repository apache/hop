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

package org.apache.hop.pipeline.transforms.delay;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Delay input row.
 *
 * @author Samatar
 * @since 27-06-2008
 */
public class Delay extends BaseTransform<DelayMeta, DelayData> implements ITransform<DelayMeta, DelayData> {

  private static final Class<?> PKG = DelayMeta.class; // For Translator

  public Delay( TransformMeta transformMeta, DelayMeta meta, DelayData data, int copyNr, PipelineMeta pipelineMeta,
                Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  public boolean processRow() throws HopException {

    Object[] r = getRow(); // get row, set busy!

    if ( r == null ) { // no more input to be expected...

      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;

      String msgScale;
      switch ( meta.getScaleTimeCode() ) {
        case 0:
          msgScale = BaseMessages.getString( PKG, "DelayDialog.MSScaleTime.Label" );
          data.Multiple = 1;
          break;
        case 1:
          msgScale = BaseMessages.getString( PKG, "DelayDialog.SScaleTime.Label" );
          data.Multiple = 1000;
          break;
        case 2:
          msgScale = BaseMessages.getString( PKG, "DelayDialog.MnScaleTime.Label" );
          data.Multiple = 60000;
          break;
        case 3:
          msgScale = BaseMessages.getString( PKG, "DelayDialog.HrScaleTime.Label" );
          data.Multiple = 3600000;
          break;
        default:
          msgScale = "Unknown Scale";
          data.Multiple = 1;
      }

      String timeOut = resolve( meta.getTimeOut() );
      data.timeout = Const.toInt( timeOut, 0 );

      if ( log.isDebug() ) {
        logDebug( BaseMessages.getString( PKG, "Delay.Log.TimeOut", "" + data.timeout, msgScale ) );
      }
    }

    if ( ( data.Multiple < 1000 ) && ( data.timeout > 0 ) ) {
      // handle the milliseconds delays here
      try {
        Thread.sleep( data.timeout );
      } catch ( Exception e ) {
        // nothing
      }
    } else {
      // starttime (in seconds ,Minutes or Hours)
      long timeStart = System.currentTimeMillis();

      boolean continueLoop = true;

      while ( continueLoop && !isStopped() ) {
        // Update Time value
        long now = System.currentTimeMillis();

        // Let's check the limit time
        if ( now >= ( timeStart + ( data.timeout * data.Multiple ) ) ) {
          // We have reached the time limit
          continueLoop = false;
        } else {
          try {
            Thread.sleep( 1000 );
          } catch ( Exception e ) {
            // handling this exception would be kind of silly.
          }
        }
      }
    }
    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "Delay.WaitTimeIsElapsed.Label" ) );
    }

    putRow( getInputRowMeta(), r ); // copy row to possible alternate rowset(s).

    if ( checkFeedback( getLinesRead() ) ) {
      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "Delay.Log.LineNumber", "" + getLinesRead() ) );
      }
    }

    return true;
  }

}
