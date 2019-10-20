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

package org.apache.hop.trans.steps.filterrows;

import java.util.List;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.exception.HopException;
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

/**
 * Filters input rows base on conditions.
 *
 * @author Matt
 * @since 16-apr-2003, 07-nov-2004 (rewrite)
 */
public class FilterRows extends BaseStep implements StepInterface {
  private static Class<?> PKG = FilterRowsMeta.class; // for i18n purposes, needed by Translator2!!

  private FilterRowsMeta meta;
  private FilterRowsData data;

  public FilterRows( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
    Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  private synchronized boolean keepRow( RowMetaInterface rowMeta, Object[] row ) throws HopException {
    try {
      return meta.getCondition().evaluate( rowMeta, row );
    } catch ( Exception e ) {
      String message =
        BaseMessages.getString( PKG, "FilterRows.Exception.UnexpectedErrorFoundInEvaluationFuction" );
      logError( message );
      logError( BaseMessages.getString( PKG, "FilterRows.Log.ErrorOccurredForRow" ) + rowMeta.getString( row ) );
      logError( Const.getStackTracker( e ) );
      throw new HopException( message, e );
    }
  }

  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws HopException {
    meta = (FilterRowsMeta) smi;
    data = (FilterRowsData) sdi;

    boolean keep;

    Object[] r = getRow(); // Get next usable row from input rowset(s)!
    if ( r == null ) { // no more input to be expected...

      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( getInputRowMeta(), getStepname(), null, null, this, repository, metaStore );

      // if filter refers to non-existing fields, throw exception
      checkNonExistingFields();

      // Cache the position of the RowSet for the output.
      //
      if ( data.chosesTargetSteps ) {
        List<StreamInterface> targetStreams = meta.getStepIOMeta().getTargetStreams();
        if ( !Utils.isEmpty( targetStreams.get( 0 ).getStepname() ) ) {
          data.trueRowSet = findOutputRowSet( getStepname(), getCopy(), targetStreams.get( 0 ).getStepname(), 0 );
          if ( data.trueRowSet == null ) {
            throw new HopException( BaseMessages.getString(
              PKG, "FilterRows.Log.TargetStepInvalid", targetStreams.get( 0 ).getStepname() ) );
          }
        } else {
          data.trueRowSet = null;
        }

        if ( !Utils.isEmpty( targetStreams.get( 1 ).getStepname() ) ) {
          data.falseRowSet = findOutputRowSet( getStepname(), getCopy(), targetStreams.get( 1 ).getStepname(), 0 );
          if ( data.falseRowSet == null ) {
            throw new HopException( BaseMessages.getString(
              PKG, "FilterRows.Log.TargetStepInvalid", targetStreams.get( 1 ).getStepname() ) );
          }
        } else {
          data.falseRowSet = null;
        }
      }
    }

    keep = keepRow( getInputRowMeta(), r ); // Keep this row?
    if ( !data.chosesTargetSteps ) {
      if ( keep ) {
        putRow( data.outputRowMeta, r ); // copy row to output rowset(s);
      }
    } else {
      if ( keep ) {
        if ( data.trueRowSet != null ) {
          if ( log.isRowLevel() ) {
            logRowlevel( "Sending row to true  :" + data.trueStepname + " : " + getInputRowMeta().getString( r ) );
          }
          putRowTo( data.outputRowMeta, r, data.trueRowSet );
        }
      } else {
        if ( data.falseRowSet != null ) {
          if ( log.isRowLevel() ) {
            logRowlevel( "Sending row to false :" + data.falseStepname + " : " + getInputRowMeta().getString( r ) );
          }
          putRowTo( data.outputRowMeta, r, data.falseRowSet );
        }
      }
    }

    if ( checkFeedback( getLinesRead() ) ) {
      if ( log.isBasic() ) {
        logBasic( BaseMessages.getString( PKG, "FilterRows.Log.LineNumber" ) + getLinesRead() );
      }
    }

    return true;
  }

  /**
   * @see StepInterface#init(org.apache.hop.trans.step.StepMetaInterface , org.apache.hop.trans.step.StepDataInterface)
   */
  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (FilterRowsMeta) smi;
    data = (FilterRowsData) sdi;

    if ( super.init( smi, sdi ) ) {
      // PDI-6785
      // could it be a better idea to have a clone on the condition in data and do this on the first row?
      meta.getCondition().clearFieldPositions();

      List<StreamInterface> targetStreams = meta.getStepIOMeta().getTargetStreams();
      data.trueStepname = targetStreams.get( 0 ).getStepname();
      data.falseStepname = targetStreams.get( 1 ).getStepname();

      data.chosesTargetSteps =
        targetStreams.get( 0 ).getStepMeta() != null || targetStreams.get( 1 ).getStepMeta() != null;
      return true;
    }
    return false;
  }

  protected void checkNonExistingFields() throws HopException {
    List<String> orphanFields = meta.getOrphanFields(
      meta.getCondition(), getInputRowMeta() );
    if ( orphanFields != null && orphanFields.size() > 0 ) {
      String fields = "";
      boolean first = true;
      for ( String field : orphanFields ) {
        if ( !first ) {
          fields += ", ";
        }
        fields += "'" + field + "'";
        first = false;
      }
      String errorMsg = BaseMessages.getString( PKG, "FilterRows.CheckResult.FieldsNotFoundFromPreviousStep", fields );
      throw new HopException( errorMsg );
    }
  }
}
