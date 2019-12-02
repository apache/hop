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

package org.apache.hop.repository.kdr.delegates;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

import org.apache.hop.core.Const;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopDependencyException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.repository.LongObjectId;
import org.apache.hop.repository.ObjectId;
import org.apache.hop.repository.RepositoryOperation;
import org.apache.hop.repository.kdr.HopDatabaseRepository;

public class HopDatabaseRepositoryDatabaseDelegate extends HopDatabaseRepositoryBaseDelegate {

  private static final Class<?> PKG = DatabaseMeta.class; // for i18n purposes, needed by Translator2!!

  public HopDatabaseRepositoryDatabaseDelegate( HopDatabaseRepository repository ) {
    super( repository );
  }

  public synchronized ObjectId getDatabaseID( String name ) throws HopException {
    return repository.connectionDelegate.getIDWithValue(
      quoteTable( HopDatabaseRepository.TABLE_R_DATABASE ),
      quote( HopDatabaseRepository.FIELD_DATABASE_ID_DATABASE ),
      quote( HopDatabaseRepository.FIELD_DATABASE_NAME ), name );
  }

  public synchronized String getDatabaseTypeCode( ObjectId id_database_type ) throws HopException {
    return repository.connectionDelegate.getStringWithID(
      quoteTable( HopDatabaseRepository.TABLE_R_DATABASE_TYPE ),
      quote( HopDatabaseRepository.FIELD_DATABASE_TYPE_ID_DATABASE_TYPE ), id_database_type,
      quote( HopDatabaseRepository.FIELD_DATABASE_TYPE_CODE ) );
  }

  public synchronized String getDatabaseConTypeCode( ObjectId id_database_contype ) throws HopException {
    return repository.connectionDelegate.getStringWithID(
      quoteTable( HopDatabaseRepository.TABLE_R_DATABASE_CONTYPE ),
      quote( HopDatabaseRepository.FIELD_DATABASE_CONTYPE_ID_DATABASE_CONTYPE ), id_database_contype,
      quote( HopDatabaseRepository.FIELD_DATABASE_CONTYPE_CODE ) );
  }

  public RowMetaAndData getDatabase( ObjectId id_database ) throws HopException {
    return repository.connectionDelegate.getOneRow(
      quoteTable( HopDatabaseRepository.TABLE_R_DATABASE ),
      quote( HopDatabaseRepository.FIELD_DATABASE_ID_DATABASE ), id_database );
  }

  public RowMetaAndData getDatabaseAttribute( ObjectId id_database_attribute ) throws HopException {
    return repository.connectionDelegate.getOneRow(
      quoteTable( HopDatabaseRepository.TABLE_R_DATABASE_ATTRIBUTE ),
      quote( HopDatabaseRepository.FIELD_DATABASE_ATTRIBUTE_ID_DATABASE_ATTRIBUTE ), id_database_attribute );
  }

  public Collection<RowMetaAndData> getDatabaseAttributes() throws HopDatabaseException, HopValueException {
    List<RowMetaAndData> attrs = new ArrayList<RowMetaAndData>();
    List<Object[]> rows =
      repository.connectionDelegate.getRows( "SELECT * FROM "
        + quoteTable( HopDatabaseRepository.TABLE_R_DATABASE_ATTRIBUTE ), 0 );
    for ( Object[] row : rows ) {
      RowMetaAndData rowWithMeta = new RowMetaAndData( repository.connectionDelegate.getReturnRowMeta(), row );
      long id =
        rowWithMeta.getInteger(
          quote( HopDatabaseRepository.FIELD_DATABASE_ATTRIBUTE_ID_DATABASE_ATTRIBUTE ), 0 );
      if ( id > 0 ) {
        attrs.add( rowWithMeta );
      }
    }
    return attrs;
  }

  /**
   *
   * Load the Database Info
   */
  public DatabaseMeta loadDatabaseMeta( ObjectId id_database ) throws HopException {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    try {
      RowMetaAndData r = getDatabase( id_database );

      if ( r != null ) {
        ObjectId id_database_type =
          new LongObjectId( r.getInteger( HopDatabaseRepository.FIELD_DATABASE_ID_DATABASE_TYPE, 0 ) ); // con_type
        String dbTypeDesc = getDatabaseTypeCode( id_database_type );
        if ( dbTypeDesc != null ) {
          databaseMeta.setDatabaseInterface( DatabaseMeta.getDatabaseInterface( dbTypeDesc ) );
          databaseMeta.setAttributes( new Properties() ); // new attributes
        }

        databaseMeta.setObjectId( id_database );
        databaseMeta.setName( r.getString( HopDatabaseRepository.FIELD_DATABASE_NAME, "" ) );

        ObjectId id_database_contype = new LongObjectId(
          r.getInteger( HopDatabaseRepository.FIELD_DATABASE_ID_DATABASE_CONTYPE, 0 ) ); // con_access
        databaseMeta.setAccessType( DatabaseMeta.getAccessType( getDatabaseConTypeCode( id_database_contype ) ) );

        databaseMeta.setHostname( r.getString( HopDatabaseRepository.FIELD_DATABASE_HOST_NAME, "" ) );
        databaseMeta.setDBName( r.getString( HopDatabaseRepository.FIELD_DATABASE_DATABASE_NAME, "" ) );
        databaseMeta.setPort( r.getString( HopDatabaseRepository.FIELD_DATABASE_PORT, "" ) );
        databaseMeta.setUsername( r.getString( HopDatabaseRepository.FIELD_DATABASE_USERNAME, "" ) );
        databaseMeta.setPassword( Encr.decryptPasswordOptionallyEncrypted( r.getString(
          HopDatabaseRepository.FIELD_DATABASE_PASSWORD, "" ) ) );
        databaseMeta.setServername( r.getString( HopDatabaseRepository.FIELD_DATABASE_SERVERNAME, "" ) );
        databaseMeta.setDataTablespace( r.getString( HopDatabaseRepository.FIELD_DATABASE_DATA_TBS, "" ) );
        databaseMeta.setIndexTablespace( r.getString( HopDatabaseRepository.FIELD_DATABASE_INDEX_TBS, "" ) );

        // Also, load all the properties we can find...
        final Collection<RowMetaAndData> attrs = repository.connectionDelegate.getDatabaseAttributes( id_database );
        for ( RowMetaAndData row : attrs ) {
          String code = row.getString( HopDatabaseRepository.FIELD_DATABASE_ATTRIBUTE_CODE, "" );
          String attribute = row.getString( HopDatabaseRepository.FIELD_DATABASE_ATTRIBUTE_VALUE_STR, "" );
          databaseMeta.getAttributes().put( code, Const.NVL( attribute, "" ) );
        }
      }

      return databaseMeta;
    } catch ( HopDatabaseException dbe ) {
      throw new HopException( "Error loading database connection from repository (id_database="
        + id_database + ")", dbe );
    }
  }

  /**
   * Saves the database information into a given repository.
   *
   * @param databaseMeta
   *          The database metadata object to store
   *
   * @throws HopException
   *           if an error occurs.
   */
  public void saveDatabaseMeta( DatabaseMeta databaseMeta ) throws HopException {
    try {
      // If we don't have an ID, we don't know which entry in the database we need to update.
      // See if a database with the same name is already available...
      if ( databaseMeta.getObjectId() == null ) {
        databaseMeta.setObjectId( getDatabaseID( databaseMeta.getName() ) );
      }

      // Still not found? --> Insert
      if ( databaseMeta.getObjectId() == null ) {
        // Insert new Note in repository
        //
        databaseMeta.setObjectId( insertDatabase(
          databaseMeta.getName(), databaseMeta.getPluginId(), DatabaseMeta.getAccessTypeDesc( databaseMeta
            .getAccessType() ), databaseMeta.getHostname(), databaseMeta.getDatabaseName(), databaseMeta
            .getPort(), databaseMeta.getUsername(), databaseMeta.getPassword(),
          databaseMeta.getServername(), databaseMeta.getDataTablespace(), databaseMeta.getIndexTablespace() ) );
      } else {
        // --> found entry with the same name...

        // Update the note...
        updateDatabase(
          databaseMeta.getObjectId(), databaseMeta.getName(), databaseMeta.getPluginId(), DatabaseMeta
            .getAccessTypeDesc( databaseMeta.getAccessType() ), databaseMeta.getHostname(), databaseMeta
            .getDatabaseName(), databaseMeta.getPort(), databaseMeta.getUsername(),
          databaseMeta.getPassword(), databaseMeta.getServername(), databaseMeta.getDataTablespace(),
          databaseMeta.getIndexTablespace() );
      }

      // For the extra attributes, just delete them and re-add them.
      delDatabaseAttributes( databaseMeta.getObjectId() );

      // OK, now get a list of all the attributes set on the database connection...
      insertDatabaseAttributes( databaseMeta.getObjectId(), databaseMeta.getAttributes() );
    } catch ( HopDatabaseException dbe ) {
      throw new HopException(
        "Error saving database connection or one of its attributes to the repository.", dbe );
    }
  }

  public synchronized ObjectId insertDatabase( String name, String type, String access, String host,
    String dbname, String port, String user, String pass, String servername, String data_tablespace,
    String index_tablespace ) throws HopException {

    ObjectId id = repository.connectionDelegate.getNextDatabaseID();

    ObjectId id_database_type = getDatabaseTypeID( type );
    if ( id_database_type == null ) {
      // New support database type: add it!

      id_database_type = repository.connectionDelegate.getNextDatabaseTypeID();

      String tablename = HopDatabaseRepository.TABLE_R_DATABASE_TYPE;
      RowMetaInterface tableMeta = new RowMeta();

      tableMeta.addValueMeta( new ValueMetaInteger(
        HopDatabaseRepository.FIELD_DATABASE_TYPE_ID_DATABASE_TYPE, 5, 0 ) );
      tableMeta.addValueMeta( new ValueMetaString( HopDatabaseRepository.FIELD_DATABASE_TYPE_CODE,
        HopDatabaseRepository.REP_STRING_CODE_LENGTH, 0 ) );
      tableMeta.addValueMeta( new ValueMetaString( HopDatabaseRepository.FIELD_DATABASE_TYPE_DESCRIPTION,
        HopDatabaseRepository.REP_STRING_LENGTH, 0 ) );

      repository.connectionDelegate.getDatabase().prepareInsert( tableMeta, tablename );

      Object[] tableData = new Object[3];
      int tableIndex = 0;

      tableData[tableIndex++] = new LongObjectId( id_database_type ).longValue();
      tableData[tableIndex++] = type;
      tableData[tableIndex++] = type;

      repository.connectionDelegate.getDatabase().setValuesInsert( tableMeta, tableData );
      repository.connectionDelegate.getDatabase().insertRow();
      repository.connectionDelegate.getDatabase().closeInsert();
    }

    ObjectId id_database_contype = getDatabaseConTypeID( access );

    RowMetaAndData table = new RowMetaAndData();
    table.addValue( new ValueMetaInteger(
      HopDatabaseRepository.FIELD_DATABASE_ID_DATABASE ), id );
    table.addValue(
      new ValueMetaString( HopDatabaseRepository.FIELD_DATABASE_NAME ), name );
    table
      .addValue(
        new ValueMetaInteger(
          HopDatabaseRepository.FIELD_DATABASE_ID_DATABASE_TYPE ),
        id_database_type );
    table.addValue(
      new ValueMetaInteger(
        HopDatabaseRepository.FIELD_DATABASE_ID_DATABASE_CONTYPE ),
      id_database_contype );
    table.addValue( new ValueMetaString(
      HopDatabaseRepository.FIELD_DATABASE_HOST_NAME ), host );
    table.addValue( new ValueMetaString(
      HopDatabaseRepository.FIELD_DATABASE_DATABASE_NAME ), dbname );
    table.addValue(
      new ValueMetaInteger( HopDatabaseRepository.FIELD_DATABASE_PORT ), Long.valueOf(
        Const.toLong( port, -1 ) ) );
    table.addValue( new ValueMetaString(
      HopDatabaseRepository.FIELD_DATABASE_USERNAME ), user );
    table.addValue( new ValueMetaString(
      HopDatabaseRepository.FIELD_DATABASE_PASSWORD ), Encr
      .encryptPasswordIfNotUsingVariables( pass ) );
    table.addValue( new ValueMetaString(
      HopDatabaseRepository.FIELD_DATABASE_SERVERNAME ), servername );
    table.addValue( new ValueMetaString(
      HopDatabaseRepository.FIELD_DATABASE_DATA_TBS ), data_tablespace );
    table.addValue( new ValueMetaString(
      HopDatabaseRepository.FIELD_DATABASE_INDEX_TBS ), index_tablespace );

    repository.connectionDelegate.getDatabase().prepareInsert(
      table.getRowMeta(), HopDatabaseRepository.TABLE_R_DATABASE );
    repository.connectionDelegate.getDatabase().setValuesInsert( table );
    repository.connectionDelegate.getDatabase().insertRow();
    repository.connectionDelegate.getDatabase().closeInsert();

    return id;
  }

  public synchronized void updateDatabase( ObjectId id_database, String name, String type, String access,
    String host, String dbname, String port, String user, String pass, String servername,
    String data_tablespace, String index_tablespace ) throws HopException {
    ObjectId id_database_type = getDatabaseTypeID( type );
    ObjectId id_database_contype = getDatabaseConTypeID( access );

    RowMetaAndData table = new RowMetaAndData();
    table.addValue(
      new ValueMetaString( HopDatabaseRepository.FIELD_DATABASE_NAME ), name );
    table
      .addValue(
        new ValueMetaInteger(
          HopDatabaseRepository.FIELD_DATABASE_ID_DATABASE_TYPE ),
        id_database_type );
    table.addValue(
      new ValueMetaInteger(
        HopDatabaseRepository.FIELD_DATABASE_ID_DATABASE_CONTYPE ),
      id_database_contype );
    table.addValue( new ValueMetaString(
      HopDatabaseRepository.FIELD_DATABASE_HOST_NAME ), host );
    table.addValue( new ValueMetaString(
      HopDatabaseRepository.FIELD_DATABASE_DATABASE_NAME ), dbname );
    table.addValue(
      new ValueMetaInteger( HopDatabaseRepository.FIELD_DATABASE_PORT ), Long.valueOf(
        Const.toLong( port, -1 ) ) );
    table.addValue( new ValueMetaString(
      HopDatabaseRepository.FIELD_DATABASE_USERNAME ), user );
    table.addValue( new ValueMetaString(
      HopDatabaseRepository.FIELD_DATABASE_PASSWORD ), Encr
      .encryptPasswordIfNotUsingVariables( pass ) );
    table.addValue( new ValueMetaString(
      HopDatabaseRepository.FIELD_DATABASE_SERVERNAME ), servername );
    table.addValue( new ValueMetaString(
      HopDatabaseRepository.FIELD_DATABASE_DATA_TBS ), data_tablespace );
    table.addValue( new ValueMetaString(
      HopDatabaseRepository.FIELD_DATABASE_INDEX_TBS ), index_tablespace );

    repository.connectionDelegate.updateTableRow(
      HopDatabaseRepository.TABLE_R_DATABASE, HopDatabaseRepository.FIELD_DATABASE_ID_DATABASE, table,
      id_database );
  }

  public synchronized ObjectId getDatabaseTypeID( String code ) throws HopException {
    return repository.connectionDelegate.getIDWithValue(
      quoteTable( HopDatabaseRepository.TABLE_R_DATABASE_TYPE ),
      quote( HopDatabaseRepository.FIELD_DATABASE_TYPE_ID_DATABASE_TYPE ),
      quote( HopDatabaseRepository.FIELD_DATABASE_TYPE_CODE ), code );
  }

  public synchronized ObjectId getDatabaseConTypeID( String code ) throws HopException {
    return repository.connectionDelegate.getIDWithValue(
      quoteTable( HopDatabaseRepository.TABLE_R_DATABASE_CONTYPE ),
      quote( HopDatabaseRepository.FIELD_DATABASE_CONTYPE_ID_DATABASE_CONTYPE ),
      quote( HopDatabaseRepository.FIELD_DATABASE_CONTYPE_CODE ), code );
  }

  /**
   * Remove a database connection from the repository
   *
   * @param databaseName
   *          The name of the connection to remove
   * @throws HopException
   *           In case something went wrong: database error, insufficient permissions, depending objects, etc.
   */
  public void deleteDatabaseMeta( String databaseName ) throws HopException {

    repository.getSecurityProvider().validateAction( RepositoryOperation.DELETE_DATABASE );

    try {
      ObjectId id_database = getDatabaseID( databaseName );
      delDatabase( id_database );

    } catch ( HopException dbe ) {
      throw new HopException( BaseMessages.getString(
        PKG, "HopDatabaseRepository.Exception.ErrorDeletingConnection.Message", databaseName ), dbe );
    }
  }

  public synchronized void delDatabase( ObjectId id_database ) throws HopException {
    repository.getSecurityProvider().validateAction( RepositoryOperation.DELETE_DATABASE );

    // First, see if the database connection is still used by other connections...
    // If so, generate an error!!
    // We look in table R_STEP_DATABASE to see if there are any steps using this database.
    //
    String[] transList = repository.getTransformationsUsingDatabase( id_database );
    String[] jobList = repository.getJobsUsingDatabase( id_database );

    if ( jobList.length == 0 && transList.length == 0 ) {
      repository.connectionDelegate.performDelete( "DELETE FROM "
        + quoteTable( HopDatabaseRepository.TABLE_R_DATABASE ) + " WHERE "
        + quote( HopDatabaseRepository.FIELD_DATABASE_ID_DATABASE ) + " = ? ", id_database );
    } else {
      String message = " Database used by the following " + Const.CR;
      if ( jobList.length > 0 ) {
        message = "jobs :" + Const.CR;
        for ( String job : jobList ) {
          message += "\t " + job + Const.CR;
        }
      }

      message += "transformations:" + Const.CR;
      for ( String trans : transList ) {
        message += "\t " + trans + Const.CR;
      }
      HopDependencyException e = new HopDependencyException( message );
      throw new HopDependencyException( "This database is still in use by "
        + jobList.length + " jobs and " + transList.length + " transformations references", e );
    }
  }

  public synchronized void delDatabaseAttributes( ObjectId id_database ) throws HopException {
    repository.connectionDelegate.performDelete( "DELETE FROM "
      + quoteTable( HopDatabaseRepository.TABLE_R_DATABASE_ATTRIBUTE ) + " WHERE "
      + quote( HopDatabaseRepository.FIELD_DATABASE_ATTRIBUTE_ID_DATABASE ) + " = ? ", id_database );
  }

  public synchronized int getNrDatabases() throws HopException {
    int retval = 0;

    String sql = "SELECT COUNT(*) FROM " + quoteTable( HopDatabaseRepository.TABLE_R_DATABASE );
    RowMetaAndData r = repository.connectionDelegate.getOneRow( sql );
    if ( r != null ) {
      retval = (int) r.getInteger( 0, 0L );
    }

    return retval;
  }

  public synchronized int getNrDatabases( ObjectId id_transformation ) throws HopException {
    int retval = 0;

    RowMetaAndData transIdRow = repository.connectionDelegate.getParameterMetaData( id_transformation );
    String sql =
      "SELECT COUNT(*) FROM "
        + quoteTable( HopDatabaseRepository.TABLE_R_STEP_DATABASE ) + " WHERE "
        + quote( HopDatabaseRepository.FIELD_STEP_DATABASE_ID_TRANSFORMATION ) + " = ? ";
    RowMetaAndData r =
      repository.connectionDelegate.getOneRow( sql, transIdRow.getRowMeta(), transIdRow.getData() );
    if ( r != null ) {
      retval = (int) r.getInteger( 0, 0L );
    }

    return retval;
  }

  public synchronized int getNrDatabaseAttributes( ObjectId id_database ) throws HopException {
    int retval = 0;

    String sql =
      "SELECT COUNT(*) FROM "
        + quoteTable( HopDatabaseRepository.TABLE_R_DATABASE_ATTRIBUTE ) + " WHERE "
        + quote( HopDatabaseRepository.FIELD_DATABASE_ATTRIBUTE_ID_DATABASE ) + " = " + id_database;
    RowMetaAndData r = repository.connectionDelegate.getOneRow( sql );
    if ( r != null ) {
      retval = (int) r.getInteger( 0, 0L );
    }

    return retval;
  }

  private RowMetaAndData createAttributeRow( ObjectId idDatabase, String code, String strValue )
    throws HopException {
    ObjectId id = repository.connectionDelegate.getNextDatabaseAttributeID();

    RowMetaAndData table = new RowMetaAndData();

    table.addValue(
      new ValueMetaInteger(
        HopDatabaseRepository.FIELD_DATABASE_ATTRIBUTE_ID_DATABASE_ATTRIBUTE ), id );
    table.addValue(
      new ValueMetaInteger(
        HopDatabaseRepository.FIELD_DATABASE_ATTRIBUTE_ID_DATABASE ),
      idDatabase );
    table.addValue( new ValueMetaString(
      HopDatabaseRepository.FIELD_DATABASE_ATTRIBUTE_CODE ), code );
    table.addValue( new ValueMetaString(
      HopDatabaseRepository.FIELD_DATABASE_ATTRIBUTE_VALUE_STR ), strValue );

    return table;
  }

  private void insertDatabaseAttributes( ObjectId idDatabase, Properties properties ) throws HopException {
    if ( properties.isEmpty() ) {
      return;
    }

    Database db = repository.connectionDelegate.getDatabase();
    boolean firstAttribute = true;
    Enumeration<Object> keys = properties.keys();
    while ( keys.hasMoreElements() ) {
      String code = (String) keys.nextElement();
      String attribute = (String) properties.get( code );

      RowMetaAndData attributeData = createAttributeRow( idDatabase, code, attribute );
      if ( firstAttribute ) {
        db.prepareInsert( attributeData.getRowMeta(), HopDatabaseRepository.TABLE_R_DATABASE_ATTRIBUTE );
        firstAttribute = false;
      }
      db.setValuesInsert( attributeData );
      db.insertRow( db.getPrepStatementInsert(), true, false );
    }
    db.executeAndClearBatch( db.getPrepStatementInsert() );
    db.closeInsert();
  }
}
