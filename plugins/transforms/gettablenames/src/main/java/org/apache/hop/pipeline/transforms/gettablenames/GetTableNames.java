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

package org.apache.hop.pipeline.transforms.gettablenames;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Return tables name list from Database connection *
 *
 * @author Samatar
 * @since 03-Juin-2008
 */

public class GetTableNames extends BaseTransform<GetTableNamesMeta, GetTableNamesData> implements ITransform<GetTableNamesMeta, GetTableNamesData> {

  private static final Class<?> PKG = GetTableNamesMeta.class; // For Translator

  public GetTableNames( TransformMeta transformMeta, GetTableNamesMeta meta, GetTableNamesData data, int copyNr, PipelineMeta pipelineMeta,
                        Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  /**
   * Build an empty row based on the meta-data...
   *
   * @return
   */

  private Object[] buildEmptyRow() {
    Object[] rowData = RowDataUtil.allocateRowData( data.outputRowMeta.size() );

    return rowData;
  }

  public boolean processRow() throws HopException {
    if ( meta.isDynamicSchema() ) {
      // Grab one row from previous transform ...
      data.readrow = getRow();

      if ( data.readrow == null ) {
        setOutputDone();
        return false;
      }
    }

    if ( first ) {
      first = false;

      if ( meta.isDynamicSchema() ) {
        data.inputRowMeta = getInputRowMeta();
        data.outputRowMeta = data.inputRowMeta.clone();
        // Get total previous fields
        data.totalpreviousfields = data.inputRowMeta.size();

        // Check is filename field is provided
        if ( Utils.isEmpty( meta.getSchemaFieldName() ) ) {
          logError( BaseMessages.getString( PKG, "GetTableNames.Log.NoSchemaField" ) );
          throw new HopException( BaseMessages.getString( PKG, "GetTableNames.Log.NoSchemaField" ) );
        }

        // cache the position of the field
        if ( data.indexOfSchemaField < 0 ) {
          data.indexOfSchemaField = data.inputRowMeta.indexOfValue( meta.getSchemaFieldName() );
          if ( data.indexOfSchemaField < 0 ) {
            // The field is unreachable !
            logError( BaseMessages.getString( PKG, "GetTableNames.Log.ErrorFindingField" )
              + "[" + meta.getSchemaFieldName() + "]" );
            throw new HopException( BaseMessages.getString(
              PKG, "GetTableNames.Exception.CouldnotFindField", meta.getSchemaFieldName() ) );
          }
        }

      } else {
        data.outputRowMeta = new RowMeta();
      }

      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

    }

    if ( meta.isDynamicSchema() ) {
      // Get value of dynamic schema ...
      data.realSchemaName = data.inputRowMeta.getString( data.readrow, data.indexOfSchemaField );
    }

    Object[] outputRow = buildEmptyRow();
    if ( meta.isDynamicSchema() ) {
      System.arraycopy( data.readrow, 0, outputRow, 0, data.readrow.length );
    }
    processIncludeCatalog( outputRow );
    processIncludeSchema( outputRow );
    processIncludeTable( outputRow );
    processIncludeView( outputRow );
    processIncludeProcedure( outputRow );
    processIncludeSynonym( outputRow );

    if ( !meta.isDynamicSchema() ) {
      setOutputDone();
      return false;
    } else {
      return true;
    }
  }

  private void processIncludeSynonym( Object[] outputRow )
    throws HopDatabaseException, HopTransformException, HopValueException {
    if ( meta.isIncludeSynonym() ) {
      String[] synonyms = data.db.getSynonyms( data.realSchemaName, meta.isAddSchemaInOut() );
      String ObjectType = BaseMessages.getString( PKG, "GetTableNamesDialog.ObjectType.Synonym" );

      for ( int i = 0; i < synonyms.length && !isStopped(); i++ ) {
        Object[] outputRowSyn = outputRow.clone();
        int outputIndex = data.totalpreviousfields;

        String synonym = synonyms[ i ];

        outputRowSyn[ outputIndex++ ] = synonym;

        if ( !Utils.isEmpty( data.realObjectTypeFieldName ) ) {
          outputRowSyn[ outputIndex++ ] = ObjectType;
        }
        if ( !Utils.isEmpty( data.realIsSystemObjectFieldName ) ) {
          outputRowSyn[ outputIndex++ ] = Boolean.valueOf( data.db.isSystemTable( synonym ) );
        }
        if ( !Utils.isEmpty( data.realSqlCreationFieldName) ) {
          outputRowSyn[ outputIndex++ ] = null;
        }
        data.rownr++;
        putRow( data.outputRowMeta, outputRowSyn ); // copy row to output rowset(s);

        logInfo( outputRowSyn );
      }
    }
  }

  private void processIncludeProcedure( Object[] outputRow )
    throws HopDatabaseException, HopTransformException, HopValueException {
    if ( meta.isIncludeProcedure() ) {
      String[] procNames = data.db.getProcedures();
      String ObjectType = BaseMessages.getString( PKG, "GetTableNamesDialog.ObjectType.Procedure" );
      for ( int i = 0; i < procNames.length && !isStopped(); i++ ) {
        Object[] outputRowProc = outputRow.clone();
        int outputIndex = data.totalpreviousfields;

        String procName = procNames[ i ];
        outputRowProc[ outputIndex++ ] = procName;

        if ( !Utils.isEmpty( data.realObjectTypeFieldName ) ) {
          outputRowProc[ outputIndex++ ] = ObjectType;
        }
        if ( !Utils.isEmpty( data.realIsSystemObjectFieldName ) ) {
          outputRowProc[ outputIndex++ ] = Boolean.valueOf( data.db.isSystemTable( procName ) );
        }
        if ( !Utils.isEmpty( data.realSqlCreationFieldName) ) {
          outputRowProc[ outputIndex++ ] = null;
        }
        data.rownr++;
        putRow( data.outputRowMeta, outputRowProc ); // copy row to output rowset(s);

        logInfo( outputRowProc );
      }
    }
  }

  @VisibleForTesting
  void processIncludeView( Object[] outputRow ) {
    // Views
    if ( meta.isIncludeView() ) {
      try {
        String[] viewNames = data.db.getViews( data.realSchemaName, meta.isAddSchemaInOut() );
        String[] viewNamesWithoutSchema = data.db.getViews( data.realSchemaName, false );
        String ObjectType = BaseMessages.getString( PKG, "GetTableNamesDialog.ObjectType.View" );
        for ( int i = 0; i < viewNames.length && !isStopped(); i++ ) {
          Object[] outputRowView = outputRow.clone();
          int outputIndex = data.totalpreviousfields;

          String viewName = viewNames[ i ];
          String viewNameWithoutSchema = viewNamesWithoutSchema[ i ];
          outputRowView[ outputIndex++ ] = viewName;

          if ( !Utils.isEmpty( data.realObjectTypeFieldName ) ) {
            outputRowView[ outputIndex++ ] = ObjectType;
          }
          if ( !Utils.isEmpty( data.realIsSystemObjectFieldName ) ) {
            outputRowView[ outputIndex++ ] = Boolean.valueOf( data.db.isSystemTable( viewNameWithoutSchema ) );
          }

          if ( !Utils.isEmpty( data.realSqlCreationFieldName) ) {
            outputRowView[ outputIndex++ ] = null;
          }
          data.rownr++;
          putRow( data.outputRowMeta, outputRowView ); // copy row to output rowset(s);

          logInfo( outputRowView );
        }
      } catch ( Exception e ) {
        // Ignore
      }
    }
  }

  @VisibleForTesting
  void processIncludeTable( Object[] outputRow )
    throws HopDatabaseException, HopTransformException, HopValueException {
    if ( meta.isIncludeTable() ) {
      // Tables

      String[] tableNames = data.db.getTablenames( data.realSchemaName, meta.isAddSchemaInOut() );
      String[] tableNamesWithoutSchema = data.db.getTablenames( data.realSchemaName, false );

      String ObjectType = BaseMessages.getString( PKG, "GetTableNamesDialog.ObjectType.Table" );

      for ( int i = 0; i < tableNames.length && !isStopped(); i++ ) {
        Object[] outputRowTable = outputRow.clone();

        int outputIndex = data.totalpreviousfields;

        String tableName = tableNames[ i ];
        String tableNameWithoutSchema = tableNamesWithoutSchema[ i ];
        outputRowTable[ outputIndex++ ] = tableName;

        if ( !Utils.isEmpty( data.realObjectTypeFieldName ) ) {
          outputRowTable[ outputIndex++ ] = ObjectType;
        }
        if ( !Utils.isEmpty( data.realIsSystemObjectFieldName ) ) {
          outputRowTable[ outputIndex++ ] = Boolean.valueOf( data.db.isSystemTable( tableNameWithoutSchema ) );
        }
        // Get primary key
        String pk = null;
        String[] pkc = data.db.getPrimaryKeyColumnNames( tableNameWithoutSchema );
        if ( pkc != null && pkc.length == 1 ) {
          pk = pkc[ 0 ];
        }
        // return sql creation
        // handle simple primary key (one field)
        String sql =
          data.db
            .getCreateTableStatement(
              tableName,
              data.db.getTableFieldsMeta( data.realSchemaName, tableNameWithoutSchema ),
              null, false, pk, true );

        if ( pkc != null ) {
          // add composite primary key (several fields in primary key)
          int IndexOfLastClosedBracket = sql.lastIndexOf( ")" );
          if ( IndexOfLastClosedBracket > -1 ) {
            sql = sql.substring( 0, IndexOfLastClosedBracket );
            sql += ", PRIMARY KEY (";
            for ( int k = 0; k < pkc.length; k++ ) {
              if ( k > 0 ) {
                sql += ", ";
              }
              sql += pkc[ k ];
            }
            sql += ")" + Const.CR + ")" + Const.CR + ";";
          }
        }
        if ( !Utils.isEmpty( data.realSqlCreationFieldName) ) {
          outputRowTable[ outputIndex++ ] = sql;
        }

        data.rownr++;
        putRow( data.outputRowMeta, outputRowTable ); // copy row to output rowset(s);

        logInfo( outputRowTable );
      }
    }
  }

  private void processIncludeSchema( Object[] outputRow )
    throws HopDatabaseException, HopTransformException, HopValueException {
    // Schemas
    if ( meta.isIncludeSchema() ) {
      String ObjectType = BaseMessages.getString( PKG, "GetTableNamesDialog.ObjectType.Schema" );
      // Views
      String[] schemaNames = new String[] {};
      if ( !Utils.isEmpty( data.realSchemaName ) ) {
        schemaNames = new String[] { data.realSchemaName };
      } else {
        schemaNames = data.db.getSchemas();
      }
      for ( int i = 0; i < schemaNames.length && !isStopped(); i++ ) {

        // Clone current input row
        Object[] outputRowSchema = outputRow.clone();

        int outputIndex = data.totalpreviousfields;

        String schemaName = schemaNames[ i ];
        outputRowSchema[ outputIndex++ ] = schemaName;

        if ( !Utils.isEmpty( data.realObjectTypeFieldName ) ) {
          outputRowSchema[ outputIndex++ ] = ObjectType;
        }
        if ( !Utils.isEmpty( data.realIsSystemObjectFieldName ) ) {
          outputRowSchema[ outputIndex++ ] = Boolean.valueOf( data.db.isSystemTable( schemaName ) );
        }
        if ( !Utils.isEmpty( data.realSqlCreationFieldName) ) {
          outputRowSchema[ outputIndex++ ] = null;
        }
        data.rownr++;
        putRow( data.outputRowMeta, outputRowSchema ); // copy row to output rowset(s);

        logInfo( outputRowSchema );
      }
    }
  }

  private void processIncludeCatalog( Object[] outputRow )
    throws HopDatabaseException, HopTransformException, HopValueException {
    // Catalogs
    if ( meta.isIncludeCatalog() ) {
      String ObjectType = BaseMessages.getString( PKG, "GetTableNames.ObjectType.Catalog" );
      // Views
      String[] catalogsNames = data.db.getCatalogs();

      for ( int i = 0; i < catalogsNames.length && !isStopped(); i++ ) {

        // Clone current input row
        Object[] outputRowCatalog = outputRow.clone();

        int outputIndex = data.totalpreviousfields;

        String catalogName = catalogsNames[ i ];
        outputRowCatalog[ outputIndex++ ] = catalogName;

        if ( !Utils.isEmpty( data.realObjectTypeFieldName ) ) {
          outputRowCatalog[ outputIndex++ ] = ObjectType;
        }
        if ( !Utils.isEmpty( data.realIsSystemObjectFieldName ) ) {
          outputRowCatalog[ outputIndex++ ] = Boolean.valueOf( data.db.isSystemTable( catalogName ) );
        }
        if ( !Utils.isEmpty( data.realSqlCreationFieldName) ) {
          outputRowCatalog[ outputIndex++ ] = null;
        }
        data.rownr++;
        putRow( data.outputRowMeta, outputRowCatalog ); // copy row to output rowset(s);

        logInfo( outputRowCatalog );
      }
    }
  }

  private void logInfo( Object[] outputRow ) throws HopValueException {
    if ( checkFeedback( getLinesRead() ) ) {
      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "GetTableNames.LineNumber", "" + getLinesRead() ) );
      }
    }
    if ( log.isRowLevel() ) {
      logRowlevel( BaseMessages.getString( PKG, "GetTableNames.Log.PutoutRow", data.outputRowMeta
        .getString( outputRow ) ) );
    }
  }

  public boolean init(){

    if ( super.init() ) {
      if ( Utils.isEmpty( meta.getTablenameFieldName() ) ) {
        logError( BaseMessages.getString( PKG, "GetTableNames.Error.TablenameFieldNameMissing" ) );
        return false;
      }
      String realSchemaName = resolve( meta.getSchemaName() );
      if ( !Utils.isEmpty( realSchemaName ) ) {
        data.realSchemaName = realSchemaName;
      }
      data.realTableNameFieldName = resolve( meta.getTablenameFieldName() );
      data.realObjectTypeFieldName = resolve( meta.getObjectTypeFieldName() );
      data.realIsSystemObjectFieldName = resolve( meta.isSystemObjectFieldName() );
      data.realSqlCreationFieldName = resolve( meta.getSqlCreationFieldName() );
      if ( !meta.isIncludeCatalog()
        && !meta.isIncludeSchema() && !meta.isIncludeTable() && !meta.isIncludeView()
        && !meta.isIncludeProcedure() && !meta.isIncludeSynonym() ) {
        logError( BaseMessages.getString( PKG, "GetTableNames.Error.includeAtLeastOneType" ) );
        return false;
      }

      try {
        // Create the output row meta-data
        data.outputRowMeta = new RowMeta();
        meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider ); // get the
        // metadata
        // populated
      } catch ( Exception e ) {
        logError( "Error initializing transform: " + e.toString() );
        logError( Const.getStackTracker( e ) );
        return false;
      }

      data.db = new Database( this, this, meta.getDatabase() );
      try {
        data.db.connect( getPartitionId() );

        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "GetTableNames.Log.ConnectedToDB" ) );
        }

        return true;
      } catch ( HopException e ) {
        logError( BaseMessages.getString( PKG, "GetTableNames.Log.DBException" ) + e.getMessage() );
        if ( data.db != null ) {
          data.db.disconnect();
        }
      }
    }
    return false;
  }

  public void dispose(){
    if ( data.db != null ) {
      data.db.disconnect();
      data.db = null;
    }
    super.dispose();
  }

}
