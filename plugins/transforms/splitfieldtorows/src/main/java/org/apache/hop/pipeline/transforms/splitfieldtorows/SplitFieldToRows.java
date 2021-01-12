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

package org.apache.hop.pipeline.transforms.splitfieldtorows;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;


import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class SplitFieldToRows extends BaseTransform<SplitFieldToRowsMeta, SplitFieldToRowsData> implements ITransform<SplitFieldToRowsMeta, SplitFieldToRowsData> {
  private static final Class<?> PKG = SplitFieldToRowsMeta.class; // For Translator



  public SplitFieldToRows( TransformMeta transformMeta, SplitFieldToRowsMeta meta, SplitFieldToRowsData data, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  private boolean splitField( IRowMeta rowMeta, Object[] rowData ) throws HopException {
    if ( first ) {
      first = false;

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

      String realSplitFieldName = resolve( meta.getSplitField() );
      data.fieldnr = rowMeta.indexOfValue( realSplitFieldName );

      int numErrors = 0;
      if ( Utils.isEmpty( meta.getNewFieldname() ) ) {
        logError( BaseMessages.getString( PKG, "SplitFieldToRows.Log.NewFieldNameIsNull" ) );
        numErrors++;
      }

      if ( data.fieldnr < 0 ) {
        logError( BaseMessages
          .getString( PKG, "SplitFieldToRows.Log.CouldNotFindFieldToSplit", realSplitFieldName ) );
        numErrors++;
      }

      if ( !rowMeta.getValueMeta( data.fieldnr ).isString() ) {
        logError( BaseMessages.getString( PKG, "SplitFieldToRows.Log.SplitFieldNotValid", realSplitFieldName ) );
        numErrors++;
      }

      if ( meta.includeRowNumber() ) {
        String realRowNumberField = resolve( meta.getRowNumberField() );
        if ( Utils.isEmpty( realRowNumberField ) ) {
          logError( BaseMessages.getString( PKG, "SplitFieldToRows.Exception.RownrFieldMissing" ) );
          numErrors++;
        }
      }

      if ( numErrors > 0 ) {
        setErrors( numErrors );
        stopAll();
        return false;
      }

      data.splitMeta = rowMeta.getValueMeta( data.fieldnr );
    }

    String originalString = data.splitMeta.getString( rowData[ data.fieldnr ] );
    if ( originalString == null ) {
      originalString = "";
    }

    if ( meta.includeRowNumber() && meta.resetRowNumber() ) {
      data.rownr = 1L;
    }
    // use -1 for include all strings. see http://jira.pentaho.com/browse/PDI-11477
    String[] splitStrings = data.delimiterPattern.split( originalString, -1 );
    for ( String string : splitStrings ) {
      Object[] outputRow = RowDataUtil.createResizedCopy( rowData, data.outputRowMeta.size() );
      outputRow[ rowMeta.size() ] = string;
      // Include row number in output?
      if ( meta.includeRowNumber() ) {
        outputRow[ rowMeta.size() + 1 ] = data.rownr;
      }
      putRow( data.outputRowMeta, outputRow );
      data.rownr++;
    }

    return true;
  }

  public synchronized boolean processRow() throws HopException {

    Object[] r = getRow(); // get row from rowset, wait for our turn, indicate busy!
    if ( r == null ) { // no more input to be expected...

      setOutputDone();
      return false;
    }

    boolean ok = splitField( getInputRowMeta(), r );
    if ( !ok ) {
      setOutputDone();
      return false;
    }

    if ( checkFeedback( getLinesRead() ) ) {
      if ( log.isDetailed() ) {
        if ( log.isDetailed() ) {
          logBasic( BaseMessages.getString( PKG, "SplitFieldToRows.Log.LineNumber" ) + getLinesRead() );
        }
      }
    }

    return true;
  }

  public boolean init() {

    if ( super.init() ) {
      data.rownr = 1L;

      try {
        String delimiter = Const.nullToEmpty( meta.getDelimiter() );
        if ( meta.isDelimiterRegex() ) {
          data.delimiterPattern = Pattern.compile( resolve( delimiter ) );
        } else {
          data.delimiterPattern = Pattern.compile( Pattern.quote( resolve( delimiter ) ) );
        }
      } catch ( PatternSyntaxException pse ) {
        log.logError( pse.getMessage() );
        throw pse;
      }

      return true;
    }
    return false;
  }
}
