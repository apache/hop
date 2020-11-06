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

package org.apache.hop.pipeline.transforms.addsequence;

import org.apache.hop.core.Counter;
import org.apache.hop.core.Counters;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.Map;

/**
 * Adds a sequential number to a stream of rows.
 *
 * @author Matt
 * @since 13-may-2003
 */
public class AddSequence extends BaseTransform<AddSequenceMeta, AddSequenceData> implements ITransform<AddSequenceMeta, AddSequenceData> {

  private static final Class<?> PKG = AddSequence.class; // Needed by Translator

  public AddSequence( TransformMeta transformMeta, AddSequenceMeta meta, AddSequenceData data, int copyNr, PipelineMeta pipelineMeta,
                      Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  public Object[] addSequence( IRowMeta inputRowMeta, Object[] inputRowData ) throws HopException {
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

  @Override
  public boolean processRow() throws HopException {

    Object[] r = getRow(); // Get row from input rowset & set row busy!
    if ( r == null ) {
      // no more input to be expected...
      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );
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

  public boolean init() {

    if ( super.init() ) {
      data.realSchemaName = environmentSubstitute( meta.getSchemaName() );
      data.realSequenceName = environmentSubstitute( meta.getSequenceName() );
      if ( meta.isDatabaseUsed() ) {
        Database db = new Database( this, meta.getDatabaseMeta() );
        db.shareVariablesWith( this );
        data.setDb( db );
        try {
          data.getDb().connect( getPartitionId() );
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
        // check if counter exists
        Map<String, Counter> counterMap = Counters.getInstance().getCounterMap();
        synchronized ( counterMap ) {
          data.counter = counterMap.get( data.getLookup() );
          if ( data.counter == null ) {
            // create a new one
            data.counter = new Counter( data.start, data.increment, data.maximum );
            counterMap.put( data.getLookup(), data.counter );
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

    return false;
  }

  @Override
  public void dispose() {

    if ( meta.isCounterUsed() ) {
      if ( data.getLookup() != null ) {
        Counters.getInstance().getCounterMap().remove( data.getLookup() );
      }
      data.counter = null;
    }

    if ( meta.isDatabaseUsed() ) {
      if ( data.getDb() != null ) {
        data.getDb().disconnect();
      }
    }

    super.dispose();
  }

}
