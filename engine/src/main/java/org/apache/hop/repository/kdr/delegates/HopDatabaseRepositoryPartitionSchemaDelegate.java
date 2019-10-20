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

import org.apache.hop.core.Const;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopDependencyException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopObjectExistsException;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.partition.PartitionSchema;
import org.apache.hop.repository.ObjectId;
import org.apache.hop.repository.kdr.HopDatabaseRepository;

public class HopDatabaseRepositoryPartitionSchemaDelegate extends HopDatabaseRepositoryBaseDelegate {

  // private static Class<?> PKG = PartitionSchema.class; // for i18n purposes, needed by Translator2!!

  public HopDatabaseRepositoryPartitionSchemaDelegate( HopDatabaseRepository repository ) {
    super( repository );
  }

  public RowMetaAndData getPartitionSchema( ObjectId id_partition_schema ) throws HopException {
    return repository.connectionDelegate.getOneRow(
      quoteTable( HopDatabaseRepository.TABLE_R_PARTITION_SCHEMA ),
      quote( HopDatabaseRepository.FIELD_PARTITION_SCHEMA_ID_PARTITION_SCHEMA ), id_partition_schema );
  }

  public RowMetaAndData getPartition( ObjectId id_partition ) throws HopException {
    return repository.connectionDelegate.getOneRow(
      quoteTable( HopDatabaseRepository.TABLE_R_PARTITION ),
      quote( HopDatabaseRepository.FIELD_PARTITION_ID_PARTITION ), id_partition );
  }

  public synchronized ObjectId getPartitionSchemaID( String name ) throws HopException {
    return repository.connectionDelegate.getIDWithValue(
      quoteTable( HopDatabaseRepository.TABLE_R_PARTITION_SCHEMA ),
      quote( HopDatabaseRepository.FIELD_PARTITION_SCHEMA_ID_PARTITION_SCHEMA ),
      quote( HopDatabaseRepository.FIELD_PARTITION_SCHEMA_NAME ), name );
  }

  public void savePartitionSchema( PartitionSchema partitionSchema ) throws HopException {
    savePartitionSchema( partitionSchema, null, false );
  }

  public void savePartitionSchema( PartitionSchema partitionSchema, ObjectId id_transformation,
    boolean isUsedByTransformation ) throws HopException {
    try {
      savePartitionSchema( partitionSchema, id_transformation, isUsedByTransformation, false );
    } catch ( HopObjectExistsException e ) {
      // This is an expected possibility here. Common objects are not going to overwrite database objects
      log.logBasic( e.getMessage() );
    }
  }

  public void savePartitionSchema( PartitionSchema partitionSchema, ObjectId id_transformation,
    boolean isUsedByTransformation, boolean overwrite ) throws HopException {
    // Look up the ID again (import scenario!)
    //
    if ( partitionSchema.getObjectId() == null ) {
      partitionSchema.setObjectId( getPartitionSchemaID( partitionSchema.getName() ) );
    }

    if ( partitionSchema.getObjectId() == null ) {
      // New Slave Server
      partitionSchema.setObjectId( insertPartitionSchema( partitionSchema ) );
    } else {
      ObjectId existingPartitionSchemaId = partitionSchema.getObjectId();

      // If we received a partitionSchemaId and it is different from the partition schema we are working with...
      if ( existingPartitionSchemaId != null && !partitionSchema.getObjectId().equals( existingPartitionSchemaId ) ) {
        // A partition with this name already exists
        if ( overwrite ) {
          // Proceed with save, removing the original version from the repository first
          repository.deletePartitionSchema( existingPartitionSchemaId );
          updatePartitionSchema( partitionSchema );
          repository.delPartitions( partitionSchema.getObjectId() );
        } else {
          throw new HopObjectExistsException( "Failed to save object to repository. Object ["
            + partitionSchema.getName() + "] already exists." );
        }
      } else {
        // There are no naming collisions (either it is the same object or the name is unique)
        updatePartitionSchema( partitionSchema );
        repository.delPartitions( partitionSchema.getObjectId() );
      }
    }

    // Save the partitionschema-partition relationships
    //
    for ( int i = 0; i < partitionSchema.getPartitionIDs().size(); i++ ) {
      insertPartition( partitionSchema.getObjectId(), partitionSchema.getPartitionIDs().get( i ) );
    }

    // Save a link to the transformation to keep track of the use of this partition schema
    // Otherwise, we shouldn't bother with this
    //
    if ( isUsedByTransformation ) {
      repository.insertTransformationPartitionSchema( id_transformation, partitionSchema.getObjectId() );
    }
  }

  public PartitionSchema loadPartitionSchema( ObjectId id_partition_schema ) throws HopException {
    PartitionSchema partitionSchema = new PartitionSchema();

    partitionSchema.setObjectId( id_partition_schema );

    RowMetaAndData row = getPartitionSchema( id_partition_schema );

    partitionSchema.setName( row.getString( "NAME", null ) );

    ObjectId[] pids = repository.getPartitionIDs( id_partition_schema );
    for ( int i = 0; i < pids.length; i++ ) {
      partitionSchema.getPartitionIDs().add( getPartition( pids[i] ).getString( "PARTITION_ID", null ) );
    }

    partitionSchema.setDynamicallyDefined( row.getBoolean( "DYNAMIC_DEFINITION", false ) );
    partitionSchema.setNumberOfPartitionsPerSlave( row.getString( "PARTITIONS_PER_SLAVE", null ) );

    return partitionSchema;
  }

  public synchronized ObjectId insertPartitionSchema( PartitionSchema partitionSchema ) throws HopException {
    if ( getPartitionSchemaID( partitionSchema.getName() ) != null ) {
      // This partition schema name is already in use. Throw an exception.
      throw new HopObjectExistsException( "Failed to create object in repository. Object ["
        + partitionSchema.getName() + "] already exists." );
    }

    ObjectId id = repository.connectionDelegate.getNextPartitionSchemaID();

    RowMetaAndData table = new RowMetaAndData();

    table.addValue( new ValueMetaInteger( HopDatabaseRepository.FIELD_PARTITION_SCHEMA_ID_PARTITION_SCHEMA ), id );
    table.addValue( new ValueMetaString( HopDatabaseRepository.FIELD_PARTITION_SCHEMA_NAME ),
      partitionSchema.getName() );
    table.addValue( new ValueMetaBoolean( HopDatabaseRepository.FIELD_PARTITION_SCHEMA_DYNAMIC_DEFINITION ),
      partitionSchema.isDynamicallyDefined() );
    table.addValue( new ValueMetaString( HopDatabaseRepository.FIELD_PARTITION_SCHEMA_PARTITIONS_PER_SLAVE ),
      partitionSchema.getNumberOfPartitionsPerSlave() );

    repository.connectionDelegate.getDatabase().prepareInsert(
      table.getRowMeta(), HopDatabaseRepository.TABLE_R_PARTITION_SCHEMA );
    repository.connectionDelegate.getDatabase().setValuesInsert( table );
    repository.connectionDelegate.getDatabase().insertRow();
    repository.connectionDelegate.getDatabase().closeInsert();

    return id;
  }

  public synchronized void updatePartitionSchema( PartitionSchema partitionSchema ) throws HopException {
    RowMetaAndData table = new RowMetaAndData();
    table.addValue( new ValueMetaString( HopDatabaseRepository.FIELD_PARTITION_SCHEMA_NAME ),
      partitionSchema.getName() );
    table.addValue( new ValueMetaBoolean( HopDatabaseRepository.FIELD_PARTITION_SCHEMA_DYNAMIC_DEFINITION ),
      partitionSchema.isDynamicallyDefined() );
    table.addValue( new ValueMetaString( HopDatabaseRepository.FIELD_PARTITION_SCHEMA_PARTITIONS_PER_SLAVE ),
      partitionSchema.getNumberOfPartitionsPerSlave() );

    repository.connectionDelegate.updateTableRow(
      HopDatabaseRepository.TABLE_R_PARTITION_SCHEMA,
      HopDatabaseRepository.FIELD_PARTITION_SCHEMA_ID_PARTITION_SCHEMA, table, partitionSchema.getObjectId() );
  }

  public synchronized ObjectId insertPartition( ObjectId id_partition_schema, String partition_id ) throws HopException {
    ObjectId id = repository.connectionDelegate.getNextPartitionID();

    RowMetaAndData table = new RowMetaAndData();

    table.addValue( new ValueMetaInteger(
      HopDatabaseRepository.FIELD_PARTITION_ID_PARTITION ), id );
    table.addValue(
      new ValueMetaInteger(
        HopDatabaseRepository.FIELD_PARTITION_ID_PARTITION_SCHEMA ),
      id_partition_schema );
    table.addValue( new ValueMetaString(
      HopDatabaseRepository.FIELD_PARTITION_PARTITION_ID ), partition_id );

    repository.connectionDelegate.getDatabase().prepareInsert(
      table.getRowMeta(), HopDatabaseRepository.TABLE_R_PARTITION );
    repository.connectionDelegate.getDatabase().setValuesInsert( table );
    repository.connectionDelegate.getDatabase().insertRow();
    repository.connectionDelegate.getDatabase().closeInsert();

    return id;
  }

  public synchronized void delPartitionSchema( ObjectId id_partition_schema ) throws HopException {
    // First, see if the schema is still used by other objects...
    // If so, generate an error!!
    //
    // We look in table R_TRANS_PARTITION_SCHEMA to see if there are any transformations using this schema.
    String[] transList = repository.getTransformationsUsingPartitionSchema( id_partition_schema );

    if ( transList.length == 0 ) {
      repository.connectionDelegate.performDelete( "DELETE FROM "
        + quoteTable( HopDatabaseRepository.TABLE_R_PARTITION ) + " WHERE "
        + quote( HopDatabaseRepository.FIELD_PARTITION_ID_PARTITION_SCHEMA ) + " = ? ", id_partition_schema );
      repository.connectionDelegate.performDelete(
        "DELETE FROM "
          + quoteTable( HopDatabaseRepository.TABLE_R_PARTITION_SCHEMA ) + " WHERE "
          + quote( HopDatabaseRepository.FIELD_PARTITION_SCHEMA_ID_PARTITION_SCHEMA ) + " = ? ",
        id_partition_schema );
    } else {
      StringBuilder message = new StringBuilder( 100 );

      message.append( "The partition schema is used by the following transformations:" ).append( Const.CR );
      for ( int i = 0; i < transList.length; i++ ) {
        message.append( "  " ).append( transList[i] ).append( Const.CR );
      }
      message.append( Const.CR );

      HopDependencyException e = new HopDependencyException( message.toString() );
      throw new HopDependencyException(
        "This partition schema is still in use by one or more transformations (" + transList.length + ") :", e );
    }
  }
}
