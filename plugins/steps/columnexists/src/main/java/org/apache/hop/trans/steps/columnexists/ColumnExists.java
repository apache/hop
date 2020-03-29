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

package org.apache.hop.trans.steps.columnexists;

import org.apache.hop.core.database.Database;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStep;
import org.apache.hop.trans.step.StepDataInterface;
import org.apache.hop.trans.step.StepInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.StepMetaInterface;

/**
 * Check if a column exists in table on a specified connection *
 *
 * @author Samatar
 * @since 03-Juin-2008
 */

public class ColumnExists extends BaseStep implements StepInterface {
  private static final Class<?> PKG = ColumnExistsMeta.class; // for i18n purposes, needed by Translator2!!

  private ColumnExistsMeta meta;
  private ColumnExistsData data;

  public ColumnExists( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                       Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override
  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws HopException {
    meta = (ColumnExistsMeta) smi;
    data = (ColumnExistsData) sdi;

    boolean sendToErrorRow = false;
    String errorMessage = null;

    Object[] r = getRow(); // Get row from input rowset & set row busy!
    // if no more input to be expected set done
    if ( r == null ) {
      setOutputDone();
      return false;
    }

    boolean columnexists = false;

    if ( first ) {
      first = false;

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getStepname(), null, null, this, metaStore );

      // Check is columnname field is provided
      if ( Utils.isEmpty( meta.getDynamicColumnnameField() ) ) {
        logError( BaseMessages.getString( PKG, "ColumnExists.Error.ColumnnameFieldMissing" ) );
        throw new HopException( BaseMessages.getString( PKG, "ColumnExists.Error.ColumnnameFieldMissing" ) );
      }
      if ( meta.isTablenameInField() ) {
        // Check is tablename field is provided
        if ( Utils.isEmpty( meta.getDynamicTablenameField() ) ) {
          logError( BaseMessages.getString( PKG, "ColumnExists.Error.TablenameFieldMissing" ) );
          throw new HopException( BaseMessages.getString( PKG, "ColumnExists.Error.TablenameFieldMissing" ) );
        }

        // cache the position of the field
        if ( data.indexOfTablename < 0 ) {
          data.indexOfTablename = getInputRowMeta().indexOfValue( meta.getDynamicTablenameField() );
          if ( data.indexOfTablename < 0 ) {
            // The field is unreachable !
            logError( BaseMessages.getString( PKG, "ColumnExists.Exception.CouldnotFindField" ) + "["
              + meta.getDynamicTablenameField() + "]" );
            throw new HopException( BaseMessages.getString( PKG, "ColumnExists.Exception.CouldnotFindField",
              meta.getDynamicTablenameField() ) );
          }
        }
      } else {
        if ( !Utils.isEmpty( data.schemaname ) ) {
          data.tablename = data.db.getDatabaseMeta().getQuotedSchemaTableCombination( data.schemaname, data.tablename );
        } else {
          data.tablename = data.db.getDatabaseMeta().quoteField( data.tablename );
        }
      }

      // cache the position of the column field
      if ( data.indexOfColumnname < 0 ) {
        data.indexOfColumnname = getInputRowMeta().indexOfValue( meta.getDynamicColumnnameField() );
        if ( data.indexOfColumnname < 0 ) {
          // The field is unreachable !
          logError( BaseMessages.getString( PKG, "ColumnExists.Exception.CouldnotFindField" ) + "["
            + meta.getDynamicColumnnameField() + "]" );
          throw new HopException( BaseMessages.getString( PKG, "ColumnExists.Exception.CouldnotFindField",
            meta.getDynamicColumnnameField() ) );
        }
      }

      // End If first
    }

    try {
      // get tablename
      if ( meta.isTablenameInField() ) {
        data.tablename = getInputRowMeta().getString( r, data.indexOfTablename );
        if ( !Utils.isEmpty( data.schemaname ) ) {
          data.tablename = data.db.getDatabaseMeta().getQuotedSchemaTableCombination( data.schemaname, data.tablename );
        } else {
          data.tablename = data.db.getDatabaseMeta().quoteField( data.tablename );
        }
      }
      // get columnname
      String columnname = getInputRowMeta().getString( r, data.indexOfColumnname );
      columnname = data.db.getDatabaseMeta().quoteField( columnname );

      // Check if table exists on the specified connection
      columnexists = data.db.checkColumnExists( columnname, data.tablename );

      Object[] outputRowData = RowDataUtil.addValueData( r, getInputRowMeta().size(), columnexists );

      // add new values to the row.
      putRow( data.outputRowMeta, outputRowData ); // copy row to output rowset(s);

      if ( log.isRowLevel() ) {
        logRowlevel( BaseMessages.getString( PKG, "ColumnExists.LineNumber",
          getLinesRead() + " : " + getInputRowMeta().getString( r ) ) );
      }
    } catch ( HopException e ) {
      if ( getStepMeta().isDoingErrorHandling() ) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError( BaseMessages.getString( PKG, "ColumnExists.ErrorInStepRunning" + " : " + e.getMessage() ) );
        throw new HopStepException( BaseMessages.getString( PKG, "ColumnExists.Log.ErrorInStep" ), e );
      }
      if ( sendToErrorRow ) {
        // Simply add this row to the error row
        putError( getInputRowMeta(), r, 1, errorMessage, meta.getResultFieldName(), "ColumnExists001" );
      }
    }

    return true;
  }

  @Override
  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (ColumnExistsMeta) smi;
    data = (ColumnExistsData) sdi;

    if ( super.init( smi, sdi ) ) {
      if ( !meta.isTablenameInField() ) {
        if ( Utils.isEmpty( meta.getTablename() ) ) {
          logError( BaseMessages.getString( PKG, "ColumnExists.Error.TablenameMissing" ) );
          return false;
        }
        data.tablename = environmentSubstitute( meta.getTablename() );
      }
      data.schemaname = meta.getSchemaname();
      if ( !Utils.isEmpty( data.schemaname ) ) {
        data.schemaname = environmentSubstitute( data.schemaname );
      }

      if ( Utils.isEmpty( meta.getResultFieldName() ) ) {
        logError( BaseMessages.getString( PKG, "ColumnExists.Error.ResultFieldMissing" ) );
        return false;
      }
      data.db = new Database( this, meta.getDatabase() );
      data.db.shareVariablesWith( this );
      try {
        if ( getTransMeta().isUsingUniqueConnections() ) {
          synchronized ( getTrans() ) {
            data.db.connect( getTrans().getTransactionId(), getPartitionID() );
          }
        } else {
          data.db.connect( getPartitionID() );
        }

        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "ColumnExists.Log.ConnectedToDB" ) );
        }

        return true;
      } catch ( HopException e ) {
        logError( BaseMessages.getString( PKG, "ColumnExists.Log.DBException" ) + e.getMessage() );
        if ( data.db != null ) {
          data.db.disconnect();
        }
      }
    }
    return false;
  }

  @Override
  public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (ColumnExistsMeta) smi;
    data = (ColumnExistsData) sdi;
    if ( data.db != null ) {
      data.db.disconnect();
    }
    super.dispose( smi, sdi );
  }
}
