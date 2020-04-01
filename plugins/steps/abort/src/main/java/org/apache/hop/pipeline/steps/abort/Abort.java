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

package org.apache.hop.pipeline.steps.abort;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.step.BaseStep;
import org.apache.hop.pipeline.step.StepDataInterface;
import org.apache.hop.pipeline.step.StepInterface;
import org.apache.hop.pipeline.step.StepMeta;
import org.apache.hop.pipeline.step.StepMetaInterface;

/**
 * Step that will abort after having seen 'x' number of rows on its input.
 *
 * @author Sven Boden
 */
public class Abort extends BaseStep implements StepInterface {

  private static Class<?> PKG = Abort.class; // for i18n purposes, needed by Translator!!

  private AbortMeta meta;
  private int nrInputRows;
  private int nrThresholdRows;

  public Abort( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, PipelineMeta pipelineMeta,
                Pipeline pipeline ) {
    super( stepMeta, stepDataInterface, copyNr, pipelineMeta, pipeline );
  }

  @Override
  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (AbortMeta) smi;

    if ( super.init( smi, sdi ) ) {
      // Add init code here.
      nrInputRows = 0;
      String threshold = environmentSubstitute( meta.getRowThreshold() );
      nrThresholdRows = Const.toInt( threshold, -1 );
      if ( nrThresholdRows < 0 ) {
        logError( BaseMessages.getString( PKG, "Abort.Log.ThresholdInvalid", threshold ) );
      }

      return true;
    }
    return false;
  }
  
  @Override
  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws HopException {
    meta = (AbortMeta) smi;

    Object[] r = getRow(); // Get row from input rowset & set row busy!
    // no more input to be expected...
    if ( r == null ) {
      setOutputDone();
      return false;
    } else {
      putRow( getInputRowMeta(), r );
      nrInputRows++;
      if ( nrInputRows > nrThresholdRows ) {
        //
        // Here we abort!!
        //
        String abortOptionMessage = BaseMessages.getString( PKG, "AbortDialog.Options.Abort.Label");
        if ( meta.isAbortWithError() ) {
          abortOptionMessage = BaseMessages.getString( PKG, "AbortDialog.Options.AbortWithError.Label");
        } else if ( meta.isSafeStop() ) {
          abortOptionMessage = BaseMessages.getString( PKG, "AbortDialog.Options.SafeStop.Label");
        }
        logError( BaseMessages.getString( PKG, "Abort.Log.Wrote.AbortRow", Long.toString( nrInputRows ), abortOptionMessage, getInputRowMeta().getString( r ) ) );

        String message = environmentSubstitute( meta.getMessage() );
        if ( message == null || message.length() == 0 ) {
          logError( BaseMessages.getString( PKG, "Abort.Log.DefaultAbortMessage", "" + nrInputRows ) );
        } else {
          logError( message );
        }
        if ( meta.isSafeStop() ) {

          getPipeline().safeStop();
        } else {
          if ( meta.isAbortWithError() ) {
            setErrors( 1 );
          }

          stopAll();
        }
      } else {
        // seen a row but not yet reached the threshold
        if ( meta.isAlwaysLogRows() ) {
          logMinimal( BaseMessages.getString(
            PKG, "Abort.Log.Wrote.Row", Long.toString( nrInputRows ), getInputRowMeta().getString( r ) ) );
        } else {
          if ( log.isRowLevel() ) {
            logRowlevel( BaseMessages.getString(
              PKG, "Abort.Log.Wrote.Row", Long.toString( nrInputRows ), getInputRowMeta().getString( r ) ) );
          }
        }
      }
    }

    return true;
  }
}
