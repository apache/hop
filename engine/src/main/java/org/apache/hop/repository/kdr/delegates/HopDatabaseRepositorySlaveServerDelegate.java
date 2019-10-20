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

import org.apache.hop.cluster.SlaveServer;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopObjectExistsException;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.repository.ObjectId;
import org.apache.hop.repository.kdr.HopDatabaseRepository;

public class HopDatabaseRepositorySlaveServerDelegate extends HopDatabaseRepositoryBaseDelegate {

  private static Class<?> PKG = SlaveServer.class; // for i18n purposes, needed by Translator2!!

  public HopDatabaseRepositorySlaveServerDelegate( HopDatabaseRepository repository ) {
    super( repository );
  }

  public RowMetaAndData getSlaveServer( ObjectId id_slave ) throws HopException {
    return repository.connectionDelegate.getOneRow(
      quoteTable( HopDatabaseRepository.TABLE_R_SLAVE ),
      quote( HopDatabaseRepository.FIELD_SLAVE_ID_SLAVE ), id_slave );
  }

  public synchronized ObjectId getSlaveID( String name ) throws HopException {
    return repository.connectionDelegate.getIDWithValue(
      quoteTable( HopDatabaseRepository.TABLE_R_SLAVE ),
      quote( HopDatabaseRepository.FIELD_SLAVE_ID_SLAVE ),
      quote( HopDatabaseRepository.FIELD_SLAVE_NAME ), name );
  }

  public void saveSlaveServer( SlaveServer slaveServer ) throws HopException {
    saveSlaveServer( slaveServer, null, false );
  }

  public void saveSlaveServer( SlaveServer slaveServer, ObjectId id_transformation, boolean isUsedByTransformation ) throws HopException {

    try {
      saveSlaveServer( slaveServer, id_transformation, isUsedByTransformation, false );
    } catch ( HopObjectExistsException e ) {
      // This is an expected possibility here. Common objects are not going to overwrite database objects
      log.logBasic( e.getMessage() );
    }
  }

  public void saveSlaveServer( SlaveServer slaveServer, ObjectId id_transformation,
    boolean isUsedByTransformation, boolean overwrite ) throws HopException {

    if ( slaveServer.getObjectId() == null ) {
      // See if the slave doesn't exist already (just for safety and PDI-5102
      //
      slaveServer.setObjectId( getSlaveID( slaveServer.getName() ) );
    }

    if ( slaveServer.getObjectId() == null ) {
      // New Slave Server
      slaveServer.setObjectId( insertSlave( slaveServer ) );
    } else {
      ObjectId existingSlaveId = slaveServer.getObjectId();
      if ( existingSlaveId != null
        && slaveServer.getObjectId() != null && !slaveServer.getObjectId().equals( existingSlaveId ) ) {
        // A slave with this name already exists
        if ( overwrite ) {
          // Proceed with save, removing the original version from the repository first
          repository.deleteSlave( existingSlaveId );
          updateSlave( slaveServer );
        } else {
          throw new HopObjectExistsException( "Failed to save object to repository. Object ["
            + slaveServer.getName() + "] already exists." );
        }
      } else {
        // There are no naming collisions (either it is the same object or the name is unique)
        updateSlave( slaveServer );
      }
    }

    // Save the trans-slave relationship too.
    if ( id_transformation != null && isUsedByTransformation ) {
      repository.insertTransformationSlave( id_transformation, slaveServer.getObjectId() );
    }
  }

  public SlaveServer loadSlaveServer( ObjectId id_slave_server ) throws HopException {
    SlaveServer slaveServer = new SlaveServer();

    slaveServer.setObjectId( id_slave_server );

    RowMetaAndData row = getSlaveServer( id_slave_server );
    if ( row == null ) {
      throw new HopDatabaseException( BaseMessages.getString(
        PKG, "SlaveServer.SlaveCouldNotBeFound", id_slave_server.toString() ) );
    }

    slaveServer.setName( row.getString( HopDatabaseRepository.FIELD_SLAVE_NAME, null ) );
    slaveServer.setHostname( row.getString( HopDatabaseRepository.FIELD_SLAVE_HOST_NAME, null ) );
    slaveServer.setPort( row.getString( HopDatabaseRepository.FIELD_SLAVE_PORT, null ) );
    slaveServer.setWebAppName( row.getString( HopDatabaseRepository.FIELD_SLAVE_WEB_APP_NAME, null ) );
    slaveServer.setUsername( row.getString( HopDatabaseRepository.FIELD_SLAVE_USERNAME, null ) );
    slaveServer.setPassword( Encr.decryptPasswordOptionallyEncrypted( row.getString(
      HopDatabaseRepository.FIELD_SLAVE_PASSWORD, null ) ) );
    slaveServer.setProxyHostname( row.getString( HopDatabaseRepository.FIELD_SLAVE_PROXY_HOST_NAME, null ) );
    slaveServer.setProxyPort( row.getString( HopDatabaseRepository.FIELD_SLAVE_PROXY_PORT, null ) );
    slaveServer.setNonProxyHosts( row.getString( HopDatabaseRepository.FIELD_SLAVE_NON_PROXY_HOSTS, null ) );
    slaveServer.setMaster( row.getBoolean( HopDatabaseRepository.FIELD_SLAVE_MASTER, false ) );

    return slaveServer;
  }

  public synchronized ObjectId insertSlave( SlaveServer slaveServer ) throws HopException {
    if ( getSlaveID( slaveServer.getName() ) != null ) {
      // This slave server name is already in use. Throw an exception.
      throw new HopObjectExistsException( "Failed to create object in repository. Object ["
        + slaveServer.getName() + "] already exists." );
    }

    ObjectId id = repository.connectionDelegate.getNextSlaveServerID();

    RowMetaAndData table = new RowMetaAndData();

    table.addValue(
      new ValueMetaInteger( HopDatabaseRepository.FIELD_SLAVE_ID_SLAVE ), id );
    table.addValue(
      new ValueMetaString( HopDatabaseRepository.FIELD_SLAVE_NAME ), slaveServer
        .getName() );
    table.addValue(
      new ValueMetaString( HopDatabaseRepository.FIELD_SLAVE_HOST_NAME ),
      slaveServer.getHostname() );
    table.addValue(
      new ValueMetaString( HopDatabaseRepository.FIELD_SLAVE_PORT ), slaveServer
        .getPort() );
    table.addValue( new ValueMetaString(
      HopDatabaseRepository.FIELD_SLAVE_WEB_APP_NAME ), slaveServer
      .getWebAppName() );
    table.addValue(
      new ValueMetaString( HopDatabaseRepository.FIELD_SLAVE_USERNAME ),
      slaveServer.getUsername() );
    table.addValue(
      new ValueMetaString( HopDatabaseRepository.FIELD_SLAVE_PASSWORD ), Encr
        .encryptPasswordIfNotUsingVariables( slaveServer.getPassword() ) );
    table.addValue( new ValueMetaString(
      HopDatabaseRepository.FIELD_SLAVE_PROXY_HOST_NAME ), slaveServer
      .getProxyHostname() );
    table.addValue(
      new ValueMetaString( HopDatabaseRepository.FIELD_SLAVE_PROXY_PORT ),
      slaveServer.getProxyPort() );
    table.addValue( new ValueMetaString(
      HopDatabaseRepository.FIELD_SLAVE_NON_PROXY_HOSTS ), slaveServer
      .getNonProxyHosts() );
    table.addValue(
      new ValueMetaBoolean( HopDatabaseRepository.FIELD_SLAVE_MASTER ), Boolean
        .valueOf( slaveServer.isMaster() ) );

    repository.connectionDelegate.getDatabase().prepareInsert(
      table.getRowMeta(), HopDatabaseRepository.TABLE_R_SLAVE );
    repository.connectionDelegate.getDatabase().setValuesInsert( table );
    repository.connectionDelegate.getDatabase().insertRow();
    repository.connectionDelegate.getDatabase().closeInsert();

    return id;
  }

  public synchronized void updateSlave( SlaveServer slaveServer ) throws HopException {
    RowMetaAndData table = new RowMetaAndData();
    table.addValue(
      new ValueMetaString( HopDatabaseRepository.FIELD_SLAVE_NAME ), slaveServer
        .getName() );
    table.addValue(
      new ValueMetaString( HopDatabaseRepository.FIELD_SLAVE_HOST_NAME ),
      slaveServer.getHostname() );
    table.addValue(
      new ValueMetaString( HopDatabaseRepository.FIELD_SLAVE_PORT ), slaveServer
        .getPort() );
    table.addValue( new ValueMetaString(
      HopDatabaseRepository.FIELD_SLAVE_WEB_APP_NAME ), slaveServer
      .getWebAppName() );
    table.addValue(
      new ValueMetaString( HopDatabaseRepository.FIELD_SLAVE_USERNAME ),
      slaveServer.getUsername() );
    table.addValue(
      new ValueMetaString( HopDatabaseRepository.FIELD_SLAVE_PASSWORD ), Encr
        .encryptPasswordIfNotUsingVariables( slaveServer.getPassword() ) );
    table.addValue( new ValueMetaString(
      HopDatabaseRepository.FIELD_SLAVE_PROXY_HOST_NAME ), slaveServer
      .getProxyHostname() );
    table.addValue(
      new ValueMetaString( HopDatabaseRepository.FIELD_SLAVE_PROXY_PORT ),
      slaveServer.getProxyPort() );
    table.addValue( new ValueMetaString(
      HopDatabaseRepository.FIELD_SLAVE_NON_PROXY_HOSTS ), slaveServer
      .getNonProxyHosts() );
    table.addValue(
      new ValueMetaBoolean( HopDatabaseRepository.FIELD_SLAVE_MASTER ), Boolean
        .valueOf( slaveServer.isMaster() ) );

    repository.connectionDelegate.updateTableRow(
      HopDatabaseRepository.TABLE_R_SLAVE, HopDatabaseRepository.FIELD_SLAVE_ID_SLAVE, table, slaveServer
        .getObjectId() );
  }
}
