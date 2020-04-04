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

package org.apache.hop.pipeline.transforms.filesfromresult;

import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInterface;

/**
 * Reads results from a previous pipeline in a Job
 *
 * @author Matt
 * @since 2-jun-2003
 */
public class FilesFromResult extends BaseTransform implements ITransform {
  private static Class<?> PKG = FilesFromResult.class; // for i18n purposes, needed by Translator!!

  private FilesFromResultData data;

  public FilesFromResult( TransformMeta transformMeta, ITransformData data, int copyNr, PipelineMeta pipelineMeta,
                          Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );

    data = (FilesFromResultData) iTransformData;
  }

  public boolean processRow() throws HopException {
    if ( data.resultFilesList == null || getLinesRead() >= data.resultFilesList.size() ) {
      setOutputDone();
      return false;
    }

    ResultFile resultFile = data.resultFilesList.get( (int) getLinesRead() );
    RowMetaAndData r = resultFile.getRow();

    if ( first ) {
      first = false;
      data.outputRowMeta = new RowMeta();
      smi.getFields( data.outputRowMeta, getTransformName(), null, null, this, metaStore );
    }
    incrementLinesRead();

    putRow( data.outputRowMeta, r.getData() ); // copy row to possible alternate
    // rowset(s).

    if ( checkFeedback( getLinesRead() ) ) {
      logBasic( BaseMessages.getString( PKG, "FilesFromResult.Log.LineNumber" ) + getLinesRead() );
    }

    return true;
  }

  public boolean init() {
    data = (FilesFromResultData) sdi;

    if ( super.init() ) {
      Result result = getPipeline().getPreviousResult();

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
