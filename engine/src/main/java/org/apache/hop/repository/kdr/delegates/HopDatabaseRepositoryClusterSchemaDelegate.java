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

import java.util.List;

import org.apache.hop.cluster.ClusterSchema;
import org.apache.hop.cluster.SlaveServer;
import org.apache.hop.core.Const;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopDependencyException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopObjectExistsException;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.repository.ObjectId;
import org.apache.hop.repository.kdr.HopDatabaseRepository;

public class HopDatabaseRepositoryClusterSchemaDelegate extends HopDatabaseRepositoryBaseDelegate {

  // private static Class<?> PKG = ClusterSchema.class; // for i18n purposes, needed by Translator2!!

  public HopDatabaseRepositoryClusterSchemaDelegate( HopDatabaseRepository repository ) {
    super( repository );
  }

  public RowMetaAndData getClusterSchema( ObjectId id_cluster_schema ) throws HopException {
    return repository.connectionDelegate.getOneRow(
      quoteTable( HopDatabaseRepository.TABLE_R_CLUSTER ),
      quote( HopDatabaseRepository.FIELD_CLUSTER_ID_CLUSTER ), id_cluster_schema );
  }

  public synchronized ObjectId getClusterID( String name ) throws HopException {
    return repository.connectionDelegate.getIDWithValue(
      quoteTable( HopDatabaseRepository.TABLE_R_CLUSTER ),
      quote( HopDatabaseRepository.FIELD_CLUSTER_ID_CLUSTER ),
      quote( HopDatabaseRepository.FIELD_CLUSTER_NAME ), name );
  }

  public ClusterSchema loadClusterSchema( ObjectId id_cluster_schema, List<SlaveServer> slaveServers ) throws HopException {
    ClusterSchema clusterSchema = new ClusterSchema();
    RowMetaAndData row = getClusterSchema( id_cluster_schema );

    clusterSchema.setObjectId( id_cluster_schema );
    clusterSchema.setName( row.getString( HopDatabaseRepository.FIELD_CLUSTER_NAME, null ) );
    clusterSchema.setBasePort( row.getString( HopDatabaseRepository.FIELD_CLUSTER_BASE_PORT, null ) );
    clusterSchema.setSocketsBufferSize( row.getString(
      HopDatabaseRepository.FIELD_CLUSTER_SOCKETS_BUFFER_SIZE, null ) );
    clusterSchema.setSocketsFlushInterval( row.getString(
      HopDatabaseRepository.FIELD_CLUSTER_SOCKETS_FLUSH_INTERVAL, null ) );
    clusterSchema.setSocketsCompressed( row.getBoolean(
      HopDatabaseRepository.FIELD_CLUSTER_SOCKETS_COMPRESSED, true ) );
    clusterSchema.setDynamic( row.getBoolean( HopDatabaseRepository.FIELD_CLUSTER_DYNAMIC, true ) );

    ObjectId[] pids = repository.getClusterSlaveIDs( id_cluster_schema );
    for ( int i = 0; i < pids.length; i++ ) {
      SlaveServer slaveServer = repository.loadSlaveServer( pids[i], null ); // Load last version
      SlaveServer reference = SlaveServer.findSlaveServer( slaveServers, slaveServer.getName() );
      if ( reference != null ) {
        clusterSchema.getSlaveServers().add( reference );
      } else {
        clusterSchema.getSlaveServers().add( slaveServer );
      }
    }

    return clusterSchema;
  }

  public void saveClusterSchema( ClusterSchema clusterSchema, String versionComment ) throws HopException {
    saveClusterSchema( clusterSchema, versionComment, null, false );
  }

  public void saveClusterSchema( ClusterSchema clusterSchema, String versionComment, ObjectId id_transformation,
    boolean isUsedByTransformation ) throws HopException {
    try {
      saveClusterSchema( clusterSchema, versionComment, id_transformation, isUsedByTransformation, false );
    } catch ( HopObjectExistsException e ) {
      // This is an expected possibility here. Common objects are not going to overwrite database objects
      log.logBasic( e.getMessage() );
    }
  }

  public void saveClusterSchema( ClusterSchema clusterSchema, String versionComment, ObjectId id_transformation,
    boolean isUsedByTransformation, boolean overwrite ) throws HopException {
    ObjectId existingClusterSchemaId = getClusterID( clusterSchema.getName() );
    if ( existingClusterSchemaId != null ) {
      clusterSchema.setObjectId( existingClusterSchemaId );
    }

    if ( clusterSchema.getObjectId() == null ) {
      // New Slave Server
      clusterSchema.setObjectId( insertCluster( clusterSchema ) );
    } else {

      // If we received a clusterSchemaId and it is different from the cluster schema we are working with...
      if ( existingClusterSchemaId != null && !clusterSchema.getObjectId().equals( existingClusterSchemaId ) ) {
        // A cluster with this name already exists
        if ( overwrite ) {
          // Proceed with save, removing the original version from the repository first
          repository.deleteClusterSchema( existingClusterSchemaId );
          updateCluster( clusterSchema );
        } else {
          throw new HopObjectExistsException( "Failed to save object to repository. Object ["
            + clusterSchema.getName() + "] already exists." );
        }
      } else {
        // There are no naming collisions (either it is the same object or the name is unique)
        updateCluster( clusterSchema );
      }
    }

    repository.delClusterSlaves( clusterSchema.getObjectId() );

    // Also save the used slave server references.
    for ( int i = 0; i < clusterSchema.getSlaveServers().size(); i++ ) {
      SlaveServer slaveServer = clusterSchema.getSlaveServers().get( i );
      if ( slaveServer.getObjectId() == null ) {
        // oops, not yet saved!

        repository.save( slaveServer, versionComment, null, id_transformation, isUsedByTransformation, overwrite );
      }
      repository.insertClusterSlave( clusterSchema, slaveServer );
    }

    // Save a link to the transformation to keep track of the use of this cluster schema
    // Only save it if it's really used by the transformation
    if ( isUsedByTransformation ) {
      repository.insertTransformationCluster( id_transformation, clusterSchema.getObjectId() );
    }
  }

  private synchronized ObjectId insertCluster( ClusterSchema clusterSchema ) throws HopException {
    if ( getClusterID( clusterSchema.getName() ) != null ) {
      // This cluster schema name is already in use. Throw an exception.
      throw new HopObjectExistsException( "Failed to create object in repository. Object ["
        + clusterSchema.getName() + "] already exists." );
    }

    ObjectId id = repository.connectionDelegate.getNextClusterID();

    RowMetaAndData table = new RowMetaAndData();

    table.addValue( new ValueMetaInteger(
      HopDatabaseRepository.FIELD_CLUSTER_ID_CLUSTER ), id );
    table.addValue(
      new ValueMetaString( HopDatabaseRepository.FIELD_CLUSTER_NAME ),
      clusterSchema.getName() );
    table.addValue( new ValueMetaString(
      HopDatabaseRepository.FIELD_CLUSTER_BASE_PORT ), clusterSchema
      .getBasePort() );
    table
      .addValue(
        new ValueMetaString(
          HopDatabaseRepository.FIELD_CLUSTER_SOCKETS_BUFFER_SIZE ),
        clusterSchema.getSocketsBufferSize() );
    table.addValue(
      new ValueMetaString(
        HopDatabaseRepository.FIELD_CLUSTER_SOCKETS_FLUSH_INTERVAL ),
      clusterSchema.getSocketsFlushInterval() );
    table.addValue( new ValueMetaBoolean(
      HopDatabaseRepository.FIELD_CLUSTER_SOCKETS_COMPRESSED ), Boolean
      .valueOf( clusterSchema.isSocketsCompressed() ) );
    table.addValue(
      new ValueMetaBoolean( HopDatabaseRepository.FIELD_CLUSTER_DYNAMIC ), Boolean
        .valueOf( clusterSchema.isDynamic() ) );

    repository.connectionDelegate.getDatabase().prepareInsert(
      table.getRowMeta(), HopDatabaseRepository.TABLE_R_CLUSTER );
    repository.connectionDelegate.getDatabase().setValuesInsert( table );
    repository.connectionDelegate.getDatabase().insertRow();
    repository.connectionDelegate.getDatabase().closeInsert();

    return id;
  }

  public synchronized void updateCluster( ClusterSchema clusterSchema ) throws HopException {
    RowMetaAndData table = new RowMetaAndData();

    table.addValue( new ValueMetaInteger(
      HopDatabaseRepository.FIELD_CLUSTER_ID_CLUSTER ), clusterSchema
      .getObjectId() );
    table.addValue(
      new ValueMetaString( HopDatabaseRepository.FIELD_CLUSTER_NAME ),
      clusterSchema.getName() );
    table.addValue( new ValueMetaString(
      HopDatabaseRepository.FIELD_CLUSTER_BASE_PORT ), clusterSchema
      .getBasePort() );
    table
      .addValue(
        new ValueMetaString(
          HopDatabaseRepository.FIELD_CLUSTER_SOCKETS_BUFFER_SIZE ),
        clusterSchema.getSocketsBufferSize() );
    table.addValue(
      new ValueMetaString(
        HopDatabaseRepository.FIELD_CLUSTER_SOCKETS_FLUSH_INTERVAL ),
      clusterSchema.getSocketsFlushInterval() );
    table.addValue( new ValueMetaBoolean(
      HopDatabaseRepository.FIELD_CLUSTER_SOCKETS_COMPRESSED ), Boolean
      .valueOf( clusterSchema.isSocketsCompressed() ) );
    table.addValue(
      new ValueMetaBoolean( HopDatabaseRepository.FIELD_CLUSTER_DYNAMIC ), Boolean
        .valueOf( clusterSchema.isDynamic() ) );

    repository.connectionDelegate.updateTableRow(
      HopDatabaseRepository.TABLE_R_CLUSTER, HopDatabaseRepository.FIELD_CLUSTER_ID_CLUSTER, table,
      clusterSchema.getObjectId() );
  }

  public synchronized void delClusterSchema( ObjectId id_cluster ) throws HopException {
    // First, see if the schema is still used by other objects...
    // If so, generate an error!!
    //
    // We look in table R_TRANS_CLUSTER to see if there are any transformations using this schema.
    String[] transList = repository.getTransformationsUsingCluster( id_cluster );

    if ( transList.length == 0 ) {
      repository.connectionDelegate.performDelete( "DELETE FROM "
        + quoteTable( HopDatabaseRepository.TABLE_R_CLUSTER ) + " WHERE "
        + quote( HopDatabaseRepository.FIELD_CLUSTER_ID_CLUSTER ) + " = ? ", id_cluster );
    } else {
      StringBuilder message = new StringBuilder();

      message.append( "The cluster schema is used by the following transformations:" ).append( Const.CR );
      for ( int i = 0; i < transList.length; i++ ) {
        message.append( "  " ).append( transList[i] ).append( Const.CR );
      }
      message.append( Const.CR );

      HopDependencyException e = new HopDependencyException( message.toString() );
      throw new HopDependencyException( "This cluster schema is still in use by one or more transformations ("
        + transList.length + ") :", e );
    }
  }

  public synchronized void renameClusterSchema( ObjectId id_cluster, String new_name ) throws HopException {

  }

}
