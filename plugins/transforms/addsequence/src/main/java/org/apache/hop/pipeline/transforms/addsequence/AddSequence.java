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

package org.apache.hop.pipeline.transforms.addsequence;

import org.apache.hop.core.Counter;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformDataInterface;
import org.apache.hop.pipeline.transform.TransformInterface;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInterface;

/**
 * Adds a sequential number to a stream of rows.
 *
 * @author Matt
 * @since 13-may-2003
 */
public class AddSequence extends BaseTransform implements TransformInterface {
  private static Class<?> PKG = AddSequence.class; // for i18n purposes, needed by Translator!!

  private AddSequenceMeta meta;

  private AddSequenceData data;

  public AddSequence( TransformMeta transformMeta, TransformDataInterface transformDataInterface, int copyNr, PipelineMeta pipelineMeta,
                      Pipeline pipeline ) {
    super( transformMeta, transformDataInterface, copyNr, pipelineMeta, pipeline );
  }

  public Object[] addSequence( RowMetaInterface inputRowMeta, Object[] inputRowData ) throws HopException {
    Object next = null;

    if ( meta.isCounterUsed() ) {
      synchronized ( data.counter ) {
        long prev = data.counter.getCounter();

        long nval = prev + data.increment;
        if ( data.increment > 0 && data.maximum > data.start && nval > data.maximum ) {
          nval = data.start;
        }
        if ( data.increment < 0 && data.maximum < data.start && nval < data.maximum ) {
          nval = data.start;
        }
        data.counter.setCounter( nval );

        next = prev;
      }
    } else if ( meta.isDatabaseUsed() ) {
      try {
        next = data.getDb().getNextSequenceValue( data.realSchemaName, data.realSequenceName, meta.getValuename() );
      } catch ( HopDatabaseException dbe ) {
        throw new HopTransformException( BaseMessages.getString(
          PKG, "AddSequence.Exception.ErrorReadingSequence", data.realSequenceName ), dbe );
      }
    } else {
      // This should never happen, but if it does, don't continue!!!
      throw new HopTransformException( BaseMessages.getString( PKG, "AddSequence.Exception.NoSpecifiedMethod" ) );
    }

    if ( next != null ) {
      Object[] outputRowData = inputRowData;
      if ( inputRowData.length < inputRowMeta.size() + 1 ) {
        outputRowData = RowDataUtil.resizeArray( inputRowData, inputRowMeta.size() + 1 );
      }
      outputRowData[ inputRowMeta.size() ] = next;
      return outputRowData;
    } else {
      throw new HopTransformException( BaseMessages.getString(
        PKG, "AddSequence.Exception.CouldNotFindNextValueForSequence" )
        + meta.getValuename() );
    }
  }

  public boolean processRow( TransformMetaInterface smi, TransformDataInterface sdi ) throws HopException {
    meta = (AddSequenceMeta) smi;
    data = (AddSequenceData) sdi;

    Object[] r = getRow(); // Get row from input rowset & set row busy!
    if ( r == null ) {
      // no more input to be expected...
      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metaStore );
    }

    if ( log.isRowLevel() ) {
      logRowlevel( BaseMessages.getString( PKG, "AddSequence.Log.ReadRow" )
        + getLinesRead() + " : " + getInputRowMeta().getString( r ) );
    }

    try {
      putRow( data.outputRowMeta, addSequence( getInputRowMeta(), r ) );

      if ( log.isRowLevel() ) {
        logRowlevel( BaseMessages.getString( PKG, "AddSequence.Log.WriteRow" )
          + getLinesWritten() + " : " + getInputRowMeta().getString( r ) );
      }
      if ( checkFeedback( getLinesRead() ) ) {
        if ( log.isBasic() ) {
          logBasic( BaseMessages.getString( PKG, "AddSequence.Log.LineNumber" ) + getLinesRead() );
        }
      }
    } catch ( HopException e ) {
      logError( BaseMessages.getString( PKG, "AddSequence.Log.ErrorInTransform" ) + e.getMessage() );
      setErrors( 1 );
      stopAll();
      setOutputDone(); // signal end to receiver(s)
      return false;
    }

    return true;
  }

  public boolean init( TransformMetaInterface smi, TransformDataInterface sdi ) {
    meta = (AddSequenceMeta) smi;
    data = (AddSequenceData) sdi;

    if ( super.init( smi, sdi ) ) {
      data.realSchemaName = environmentSubstitute( meta.getSchemaName() );
      data.realSequenceName = environmentSubstitute( meta.getSequenceName() );
      if ( meta.isDatabaseUsed() ) {
        Database db = new Database( this, meta.getDatabaseMeta() );
        db.shareVariablesWith( this );
        data.setDb( db );
        try {
          if ( getPipelineMeta().isUsingUniqueConnections() ) {
            synchronized ( getPipeline() ) {
              data.getDb().connect( getPipeline().getTransactionId(), getPartitionId() );
            }
          } else {
            data.getDb().connect( getPartitionId() );
          }
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "AddSequence.Log.ConnectedDB" ) );
          }
          return true;
        } catch ( HopDatabaseException dbe ) {
          logError( BaseMessages.getString( PKG, "AddSequence.Log.CouldNotConnectToDB" ) + dbe.getMessage() );
        }
      } else if ( meta.isCounterUsed() ) {
        // Do the environment translations of the counter values.
        boolean doAbort = false;
        try {
          data.start = Long.parseLong( environmentSubstitute( meta.getStartAt() ) );
        } catch ( NumberFormatException ex ) {
          logError( BaseMessages.getString( PKG, "AddSequence.Log.CouldNotParseCounterValue", "start", meta
            .getStartAt(), environmentSubstitute( meta.getStartAt() ), ex.getMessage() ) );
          doAbort = true;
        }

        try {
          data.increment = Long.parseLong( environmentSubstitute( meta.getIncrementBy() ) );
        } catch ( NumberFormatException ex ) {
          logError( BaseMessages.getString( PKG, "AddSequence.Log.CouldNotParseCounterValue", "increment", meta
            .getIncrementBy(), environmentSubstitute( meta.getIncrementBy() ), ex.getMessage() ) );
          doAbort = true;
        }

        try {
          data.maximum = Long.parseLong( environmentSubstitute( meta.getMaxValue() ) );
        } catch ( NumberFormatException ex ) {
          logError( BaseMessages.getString( PKG, "AddSequence.Log.CouldNotParseCounterValue", "increment", meta
            .getMaxValue(), environmentSubstitute( meta.getMaxValue() ), ex.getMessage() ) );
          doAbort = true;
        }

        if ( doAbort ) {
          return false;
        }

        String realCounterName = environmentSubstitute( meta.getCounterName() );
        if ( !Utils.isEmpty( realCounterName ) ) {
          data.setLookup( "@@sequence:" + meta.getCounterName() );
        } else {
          data.setLookup( "@@sequence:" + meta.getValuename() );
        }

        if ( getPipeline().getCounters() != null ) {
          // check if counter exists
          synchronized ( getPipeline().getCounters() ) {
            data.counter = getPipeline().getCounters().get( data.getLookup() );
            if ( data.counter == null ) {
              // create a new one
              data.counter = new Counter( data.start, data.increment, data.maximum );
              getPipeline().getCounters().put( data.getLookup(), data.counter );
            } else {
              // Check whether counter characteristics are the same as a previously
              // defined counter with the same name.
              if ( ( data.counter.getStart() != data.start )
                || ( data.counter.getIncrement() != data.increment )
                || ( data.counter.getMaximum() != data.maximum ) ) {
                logError( BaseMessages.getString(
                  PKG, "AddSequence.Log.CountersWithDifferentCharacteristics", data.getLookup() ) );
                return false;
              }
            }
          }
          return true;
        } else {
          logError( BaseMessages.getString( PKG, "AddSequence.Log.PipelineCountersHashtableNotAllocated" ) );
        }
      } else {
        logError( BaseMessages.getString( PKG, "AddSequence.Log.NeedToSelectSequence" ) );
      }
    }
    return false;
  }

  public void dispose( TransformMetaInterface smi, TransformDataInterface sdi ) {
    meta = (AddSequenceMeta) smi;
    data = (AddSequenceData) sdi;

    if ( meta.isCounterUsed() ) {
      if ( data.getLookup() != null ) {
        getPipeline().getCounters().remove( data.getLookup() );
      }
      data.counter = null;
    }

    if ( meta.isDatabaseUsed() ) {
      if ( data.getDb() != null ) {
        data.getDb().disconnect();
      }
    }

    super.dispose( smi, sdi );
  }

}
