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

package org.apache.hop.core.database;

import org.apache.hop.core.IProgressMonitor;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Contains the schema's, catalogs, tables, views, synonyms, etc we can find in the databases...
 *
 * @author Matt
 * @since 7-apr-2005
 */
public class DatabaseMetaInformation {
  private static final Class<?> PKG = Database.class; // For Translator

  private String[] tables;
  private Map<String, Collection<String>> tableMap;
  private String[] views;
  private Map<String, Collection<String>> viewMap;
  private String[] synonyms;
  private Map<String, Collection<String>> synonymMap;
  private Catalog[] catalogs;
  private Schema[] schemas;
  private String[] procedures;

  private IVariables variables;
  private DatabaseMeta databaseMeta;

  public static final String FILTER_CATALOG_LIST = "FILTER_CATALOG_LIST";
  public static final String FILTER_SCHEMA_LIST = "FILTER_SCHEMA_LIST";

  /**
   * Create a new DatabaseMetaData object for the given database connection
   */
  public DatabaseMetaInformation( IVariables variables, DatabaseMeta databaseMeta ) {
    this.variables = variables;
    this.databaseMeta = databaseMeta;
  }

  public void getData( ILoggingObject parentLoggingObject, IProgressMonitor monitor ) throws HopDatabaseException {
    if ( monitor != null ) {
      monitor.beginTask( BaseMessages.getString( PKG, "DatabaseMeta.Info.GettingInfoFromDb" ), 8 );
    }

    Database db = new Database( parentLoggingObject, variables, databaseMeta );

    /*
     * ResultSet tableResultSet = null;
     *
     * ResultSet schemaTablesResultSet = null; ResultSet schemaResultSet = null;
     *
     * ResultSet catalogResultSet = null; ResultSet catalogTablesResultSet = null;
     */

    try {
      if ( monitor != null ) {
        monitor.subTask( BaseMessages.getString( PKG, "DatabaseMeta.Info.ConnectingDb" ) );
      }
      db.connect();
      if ( monitor != null ) {
        monitor.worked( 1 );
      }

      if ( monitor != null && monitor.isCanceled() ) {
        return;
      }
      if ( monitor != null ) {
        monitor.subTask( BaseMessages.getString( PKG, "DatabaseMeta.Info.GettingMetaData" ) );
      }
      DatabaseMetaData dbmd = db.getDatabaseMetaData();
      if ( monitor != null ) {
        monitor.worked( 1 );
      }

      if ( monitor != null && monitor.isCanceled() ) {
        return;
      }
      if ( monitor != null ) {
        monitor.subTask( BaseMessages.getString( PKG, "DatabaseMeta.Info.GettingInfo" ) );
      }
      Map<String, String> connectionExtraOptions = databaseMeta.getExtraOptions();
      if ( databaseMeta.supportsCatalogs() && dbmd.supportsCatalogsInTableDefinitions() ) {
        ArrayList<Catalog> catalogList = new ArrayList<>();

        String catalogFilterKey = databaseMeta.getPluginId() + "." + FILTER_CATALOG_LIST;
        if ( ( connectionExtraOptions != null ) && connectionExtraOptions.containsKey( catalogFilterKey ) ) {
          String catsFilterCommaList = connectionExtraOptions.get( catalogFilterKey );
          String[] catsFilterArray = catsFilterCommaList.split( "," );
          for ( int i = 0; i < catsFilterArray.length; i++ ) {
            catalogList.add( new Catalog( catsFilterArray[ i ].trim() ) );
          }
        }
        if ( catalogList.isEmpty() ) {
          ResultSet catalogResultSet = dbmd.getCatalogs();

          // Grab all the catalog names and put them in an array list
          // Then we can close the resultset as soon as possible.
          // This is the safest route to take for a lot of databases
          //
          while ( catalogResultSet != null && catalogResultSet.next() ) {
            String catalogName = catalogResultSet.getString( 1 );
            catalogList.add( new Catalog( catalogName ) );
          }

          // Close the catalogs resultset immediately
          //
          catalogResultSet.close();
        }

        // Now loop over the catalogs...
        //
        for ( Catalog catalog : catalogList ) {
          ArrayList<String> catalogTables = new ArrayList<>();

          try {
            ResultSet catalogTablesResultSet = dbmd.getTables( catalog.getCatalogName(), null, null, null );
            while ( catalogTablesResultSet.next() ) {
              String tableName = catalogTablesResultSet.getString( 3 );

              if ( !db.isSystemTable( tableName ) ) {
                catalogTables.add( tableName );
              }
            }
            // Immediately close the catalog tables ResultSet
            //
            catalogTablesResultSet.close();

            // Sort the tables by names
            Collections.sort( catalogTables );
          } catch ( Exception e ) {
            // Obviously, we're not allowed to snoop around in this catalog.
            // Just ignore it!
          }

          // Save the list of tables in the catalog (can be empty)
          //
          catalog.setItems( catalogTables.toArray( new String[ catalogTables.size() ] ) );
        }

        // Save for later...
        setCatalogs( catalogList.toArray( new Catalog[ catalogList.size() ] ) );
      }
      if ( monitor != null ) {
        monitor.worked( 1 );
      }

      if ( monitor != null && monitor.isCanceled() ) {
        return;
      }
      if ( monitor != null ) {
        monitor.subTask( BaseMessages.getString( PKG, "DatabaseMeta.Info.GettingSchemaInfo" ) );
      }
      if ( databaseMeta.supportsSchemas() && dbmd.supportsSchemasInTableDefinitions() ) {
        ArrayList<Schema> schemaList = new ArrayList<>();
        try {
          String schemaFilterKey = databaseMeta.getPluginId() + "." + FILTER_SCHEMA_LIST;
          if ( ( connectionExtraOptions != null ) && connectionExtraOptions.containsKey( schemaFilterKey ) ) {
            String schemasFilterCommaList = connectionExtraOptions.get( schemaFilterKey );
            String[] schemasFilterArray = schemasFilterCommaList.split( "," );
            for ( int i = 0; i < schemasFilterArray.length; i++ ) {
              schemaList.add( new Schema( schemasFilterArray[ i ].trim() ) );
            }
          }
          if ( schemaList.isEmpty() ) {
            // Support schemas for MS SQL server due to PDI-1531
            //
            String sql = databaseMeta.getSqlListOfSchemas();
            if ( !Utils.isEmpty( sql ) ) {
              Statement schemaStatement = db.getConnection().createStatement();
              ResultSet schemaResultSet = schemaStatement.executeQuery( sql );
              while ( schemaResultSet != null && schemaResultSet.next() ) {
                String schemaName = schemaResultSet.getString( "name" );
                schemaList.add( new Schema( schemaName ) );
              }
              schemaResultSet.close();
              schemaStatement.close();
            } else {
              ResultSet schemaResultSet = dbmd.getSchemas();
              while ( schemaResultSet != null && schemaResultSet.next() ) {
                String schemaName = schemaResultSet.getString( 1 );
                schemaList.add( new Schema( schemaName ) );
              }
              // Close the schema ResultSet immediately
              //
              schemaResultSet.close();
            }
          }
          for ( Schema schema : schemaList ) {
            ArrayList<String> schemaTables = new ArrayList<>();

            try {
              ResultSet schemaTablesResultSet = dbmd.getTables( null, schema.getSchemaName(), null, null );
              while ( schemaTablesResultSet.next() ) {
                String tableName = schemaTablesResultSet.getString( 3 );
                if ( !db.isSystemTable( tableName ) ) {
                  schemaTables.add( tableName );
                }
              }
              // Immediately close the schema tables ResultSet
              //
              schemaTablesResultSet.close();

              // Sort the tables by names
              Collections.sort( schemaTables );
            } catch ( Exception e ) {
              // Obviously, we're not allowed to snoop around in this catalog.
              // Just ignore it!
            }

            schema.setItems( schemaTables.toArray( new String[ schemaTables.size() ] ) );
          }
        } catch ( Exception e ) {
          // Typically an unsupported feature, security issue etc.
          // Ignore it to avoid excessive spamming
        }

        // Save for later...
        setSchemas( schemaList.toArray( new Schema[ schemaList.size() ] ) );
      }
      if ( monitor != null ) {
        monitor.worked( 1 );
      }

      if ( monitor != null && monitor.isCanceled() ) {
        return;
      }
      if ( monitor != null ) {
        monitor.subTask( BaseMessages.getString( PKG, "DatabaseMeta.Info.GettingTables" ) );
      }
      setTables( db.getTablenames( databaseMeta.supportsSchemas() ) ); // legacy call
      setTableMap( db.getTableMap() );
      if ( monitor != null ) {
        monitor.worked( 1 );
      }

      if ( monitor != null && monitor.isCanceled() ) {
        return;
      }
      if ( monitor != null ) {
        monitor.subTask( BaseMessages.getString( PKG, "DatabaseMeta.Info.GettingViews" ) );
      }
      if ( databaseMeta.supportsViews() ) {
        setViews( db.getViews( databaseMeta.supportsSchemas() ) ); // legacy call
        setViewMap( db.getViewMap() );
      }
      if ( monitor != null ) {
        monitor.worked( 1 );
      }

      if ( monitor != null && monitor.isCanceled() ) {
        return;
      }
      if ( monitor != null ) {
        monitor.subTask( BaseMessages.getString( PKG, "DatabaseMeta.Info.GettingSynonyms" ) );
      }
      if ( databaseMeta.supportsSynonyms() ) {
        setSynonyms( db.getSynonyms( databaseMeta.supportsSchemas() ) ); // legacy call
        setSynonymMap( db.getSynonymMap() );
      }
      if ( monitor != null ) {
        monitor.worked( 1 );
      }

      if ( monitor != null && monitor.isCanceled() ) {
        return;
      }
      if ( monitor != null ) {
        monitor.subTask( BaseMessages.getString( PKG, "DatabaseMeta.Info.GettingProcedures" ) );
      }
      setProcedures( db.getProcedures() );
      if ( monitor != null ) {
        monitor.worked( 1 );
      }

    } catch ( Exception e ) {
      throw new HopDatabaseException(
        BaseMessages.getString( PKG, "DatabaseMeta.Error.UnableRetrieveDbInfo" ), e );
    } finally {
      if ( monitor != null ) {
        monitor.subTask( BaseMessages.getString( PKG, "DatabaseMeta.Info.ClosingDbConnection" ) );
      }

      db.disconnect();
      if ( monitor != null ) {
        monitor.worked( 1 );
      }
    }
    if ( monitor != null ) {
      monitor.done();
    }
  }

  /**
   * Gets tables
   *
   * @return value of tables
   */
  public String[] getTables() {
    return tables;
  }

  /**
   * @param tables The tables to set
   */
  public void setTables( String[] tables ) {
    this.tables = tables;
  }

  /**
   * Gets tableMap
   *
   * @return value of tableMap
   */
  public Map<String, Collection<String>> getTableMap() {
    return tableMap;
  }

  /**
   * @param tableMap The tableMap to set
   */
  public void setTableMap( Map<String, Collection<String>> tableMap ) {
    this.tableMap = tableMap;
  }

  /**
   * Gets views
   *
   * @return value of views
   */
  public String[] getViews() {
    return views;
  }

  /**
   * @param views The views to set
   */
  public void setViews( String[] views ) {
    this.views = views;
  }

  /**
   * Gets viewMap
   *
   * @return value of viewMap
   */
  public Map<String, Collection<String>> getViewMap() {
    return viewMap;
  }

  /**
   * @param viewMap The viewMap to set
   */
  public void setViewMap( Map<String, Collection<String>> viewMap ) {
    this.viewMap = viewMap;
  }

  /**
   * Gets synonyms
   *
   * @return value of synonyms
   */
  public String[] getSynonyms() {
    return synonyms;
  }

  /**
   * @param synonyms The synonyms to set
   */
  public void setSynonyms( String[] synonyms ) {
    this.synonyms = synonyms;
  }

  /**
   * Gets synonymMap
   *
   * @return value of synonymMap
   */
  public Map<String, Collection<String>> getSynonymMap() {
    return synonymMap;
  }

  /**
   * @param synonymMap The synonymMap to set
   */
  public void setSynonymMap( Map<String, Collection<String>> synonymMap ) {
    this.synonymMap = synonymMap;
  }

  /**
   * Gets catalogs
   *
   * @return value of catalogs
   */
  public Catalog[] getCatalogs() {
    return catalogs;
  }

  /**
   * @param catalogs The catalogs to set
   */
  public void setCatalogs( Catalog[] catalogs ) {
    this.catalogs = catalogs;
  }

  /**
   * Gets schemas
   *
   * @return value of schemas
   */
  public Schema[] getSchemas() {
    return schemas;
  }

  /**
   * @param schemas The schemas to set
   */
  public void setSchemas( Schema[] schemas ) {
    this.schemas = schemas;
  }

  /**
   * Gets procedures
   *
   * @return value of procedures
   */
  public String[] getProcedures() {
    return procedures;
  }

  /**
   * @param procedures The procedures to set
   */
  public void setProcedures( String[] procedures ) {
    this.procedures = procedures;
  }

  /**
   * Gets variables
   *
   * @return value of variables
   */
  public IVariables getVariables() {
    return variables;
  }

  /**
   * @param variables The variables to set
   */
  public void setVariables( IVariables variables ) {
    this.variables = variables;
  }

  /**
   * Gets databaseMeta
   *
   * @return value of databaseMeta
   */
  public DatabaseMeta getDatabaseMeta() {
    return databaseMeta;
  }

  /**
   * @param databaseMeta The databaseMeta to set
   */
  public void setDatabaseMeta( DatabaseMeta databaseMeta ) {
    this.databaseMeta = databaseMeta;
  }
}
