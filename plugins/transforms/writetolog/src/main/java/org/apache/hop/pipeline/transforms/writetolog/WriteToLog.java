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

package org.apache.hop.pipeline.transforms.writetolog;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Write data to log.
 *
 * @author Samatar
 * @since 30-06-2008
 */

public class WriteToLog extends BaseTransform<WriteToLogMeta, WriteToLogData> implements ITransform<WriteToLogMeta, WriteToLogData> {
  private static final Class<?> PKG = WriteToLogMeta.class; // For Translator

  private int rowCounter = 0;
  private boolean rowCounterLimitHit = false;

  public WriteToLog(TransformMeta transformMeta, WriteToLogMeta meta, WriteToLogData data, int copyNr, PipelineMeta pipelineMeta,
                    Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  public boolean processRow() throws HopException {

    Object[] r = getRow(); // get row, set busy!
    if ( r == null ) { // no more input to be expected...

      setOutputDone();
      return false;
    }

    // Limit hit? skip
    if ( rowCounterLimitHit ) {
      putRow( getInputRowMeta(), r ); // copy row to output
      return true;
    }

    if ( first ) {
      first = false;

      if ( meta.getFieldName() != null && meta.getFieldName().length > 0 ) {
        data.fieldnrs = new int[ meta.getFieldName().length ];

        for ( int i = 0; i < data.fieldnrs.length; i++ ) {
          data.fieldnrs[ i ] = getInputRowMeta().indexOfValue( meta.getFieldName()[ i ] );
          if ( data.fieldnrs[ i ] < 0 ) {
            logError( BaseMessages.getString( PKG, "WriteToLog.Log.CanNotFindField", meta.getFieldName()[ i ] ) );
            throw new HopException( BaseMessages.getString( PKG, "WriteToLog.Log.CanNotFindField", meta
              .getFieldName()[ i ] ) );
          }
        }
      } else {
        data.fieldnrs = new int[ getInputRowMeta().size() ];
        for ( int i = 0; i < data.fieldnrs.length; i++ ) {
          data.fieldnrs[ i ] = i;
        }
      }
      data.fieldnr = data.fieldnrs.length;
      data.loglevel = meta.getLogLevelByDesc();
      data.logmessage = Const.NVL( this.resolve( meta.getLogMessage() ), "" );
      if ( !Utils.isEmpty( data.logmessage ) ) {
        data.logmessage += Const.CR + Const.CR;
      }

    } // end if first

    StringBuilder out = new StringBuilder();
    out.append( Const.CR
      + "------------> " + BaseMessages.getString( PKG, "WriteToLog.Log.NLigne", "" + getLinesRead() )
      + "------------------------------" + Const.CR );

    out.append( getRealLogMessage() );

    // Loop through fields
    for ( int i = 0; i < data.fieldnr; i++ ) {
      String fieldvalue = getInputRowMeta().getString( r, data.fieldnrs[ i ] );

      if ( meta.isdisplayHeader() ) {
        String fieldname = getInputRowMeta().getFieldNames()[ data.fieldnrs[ i ] ];
        out.append( fieldname + " = " + fieldvalue + Const.CR );
      } else {
        out.append( fieldvalue + Const.CR );
      }
    }
    out.append( Const.CR + "====================" );

    setLog( data.loglevel, out );

    // Increment counter
    if ( meta.isLimitRows() && ++rowCounter >= meta.getLimitRowsNumber() ) {
      rowCounterLimitHit = true;
    }

    putRow( getInputRowMeta(), r ); // copy row to output

    return true;
  }

  private void setLog( LogLevel loglevel, StringBuilder msg ) {
    switch ( loglevel ) {
      case ERROR:
        // Output message to log
        // Log level = ERREUR
        logError( msg.toString() );
        break;
      case MINIMAL:
        // Output message to log
        // Log level = MINIMAL
        logMinimal( msg.toString() );
        break;
      case BASIC:
        // Output message to log
        // Log level = BASIC
        logBasic( msg.toString() );
        break;
      case DETAILED:
        // Output message to log
        // Log level = DETAILED
        logDetailed( msg.toString() );
        break;
      case DEBUG:
        // Output message to log
        // Log level = DEBUG
        logDebug( msg.toString() );
        break;
      case ROWLEVEL:
        // Output message to log
        // Log level = ROW LEVEL
        logRowlevel( msg.toString() );
        break;
      case NOTHING:
        // Output nothing to log
        // Log level = NOTHING
        break;
      default:
        break;
    }
  }

  public String getRealLogMessage() {
    return data.logmessage;
  }

  public boolean init() {

    if ( super.init() ) {
      // Add init code here.
      return true;
    }
    return false;
  }

}
