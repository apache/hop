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

package org.apache.hop.trans.steps.append;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopRowException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStep;
import org.apache.hop.trans.step.StepDataInterface;
import org.apache.hop.trans.step.StepInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.StepMetaInterface;
import org.apache.hop.trans.step.errorhandling.StreamInterface;

import java.util.List;

/**
 * Read all rows from a hop until the end, and then read the rows from another hop.
 *
 * @author Sven Boden
 * @since 3-june-2007
 */
public class Append extends BaseStep implements StepInterface {
  private static Class<?> PKG = Append.class; // for i18n purposes, needed by Translator2!!

  private AppendMeta meta;
  private AppendData data;

  public Append( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                 Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override
  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws HopException {
    meta = (AppendMeta) smi;
    data = (AppendData) sdi;

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

  /**
   * @see StepInterface#init(org.apache.hop.trans.step.StepMetaInterface, org.apache.hop.trans.step.StepDataInterface)
   */
  @Override
  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (AppendMeta) smi;
    data = (AppendData) sdi;

    if ( super.init( smi, sdi ) ) {
      data.processHead = true;
      data.processTail = false;
      data.firstTail = true;

      List<StreamInterface> infoStreams = meta.getStepIOMeta().getInfoStreams();
      StreamInterface headStream = infoStreams.get( 0 );
      StreamInterface tailStream = infoStreams.get( 1 );
      if ( meta.headStepname != null ) {
        headStream.setStepMeta( getTransMeta().findStep( meta.headStepname ) );
      }
      if ( meta.tailStepname != null ) {
        tailStream.setStepMeta( getTransMeta().findStep( meta.tailStepname ) );
      }

      if ( headStream.getStepname() == null || tailStream.getStepname() == null ) {
        logError( BaseMessages.getString( PKG, "Append.Log.BothHopsAreNeeded" ) );
      } else {
        try {
          data.headRowSet = findInputRowSet( headStream.getStepname() );
          data.tailRowSet = findInputRowSet( tailStream.getStepname() );
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
   * Checks whether 2 template rows are compatible for the mergestep.
   *
   * @param referenceRowMeta Reference row
   * @param compareRowMeta   Row to compare to
   * @return true when templates are compatible.
   * @throws HopRowException in case there is a compatibility error.
   */
  protected void checkInputLayoutValid( RowMetaInterface referenceRowMeta, RowMetaInterface compareRowMeta ) throws HopRowException {
    if ( referenceRowMeta != null && compareRowMeta != null ) {
      BaseStep.safeModeChecking( referenceRowMeta, compareRowMeta );
    }
  }

}
