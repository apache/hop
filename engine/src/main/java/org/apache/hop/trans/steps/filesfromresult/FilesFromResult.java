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

package org.apache.hop.trans.steps.filesfromresult;

import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStep;
import org.apache.hop.trans.step.StepDataInterface;
import org.apache.hop.trans.step.StepInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.StepMetaInterface;

/**
 * Reads results from a previous transformation in a Job
 *
 * @author Matt
 * @since 2-jun-2003
 */
public class FilesFromResult extends BaseStep implements StepInterface {
  private static Class<?> PKG = FilesFromResult.class; // for i18n purposes, needed by Translator2!!

  private FilesFromResultData data;

  public FilesFromResult( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
    Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );

    data = (FilesFromResultData) stepDataInterface;
  }

  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws HopException {
    if ( data.resultFilesList == null || getLinesRead() >= data.resultFilesList.size() ) {
      setOutputDone();
      return false;
    }

    ResultFile resultFile = data.resultFilesList.get( (int) getLinesRead() );
    RowMetaAndData r = resultFile.getRow();

    if ( first ) {
      first = false;
      data.outputRowMeta = new RowMeta();
      smi.getFields( data.outputRowMeta, getStepname(), null, null, this, repository, metaStore );
    }
    incrementLinesRead();

    putRow( data.outputRowMeta, r.getData() ); // copy row to possible alternate
    // rowset(s).

    if ( checkFeedback( getLinesRead() ) ) {
      logBasic( BaseMessages.getString( PKG, "FilesFromResult.Log.LineNumber" ) + getLinesRead() );
    }

    return true;
  }

  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    data = (FilesFromResultData) sdi;

    if ( super.init( smi, sdi ) ) {
      Result result = getTrans().getPreviousResult();

      if ( result != null ) {
        data.resultFilesList = result.getResultFilesList();
      } else {
        data.resultFilesList = null;
      }

      // Add init code here.
      return true;
    }
    return false;
  }
}
