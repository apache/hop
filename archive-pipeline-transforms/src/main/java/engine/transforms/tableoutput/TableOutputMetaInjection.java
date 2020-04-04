/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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

package org.apache.hop.pipeline.transforms.tableoutput;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.pipeline.transform.TransformInjectionMetaEntry;
import org.apache.hop.pipeline.transform.TransformInjectionUtil;
import org.apache.hop.pipeline.transform.TransformMetaInjectionEntryInterface;
import org.apache.hop.pipeline.transform.TransformMetaInjectionInterface;

import java.util.ArrayList;
import java.util.List;

/**
 * This takes care of the external metadata injection into the TableOutputMeta class
 *
 * @author Chris
 */
public class TableOutputMetaInjection implements TransformMetaInjectionInterface {

  public enum Entry implements TransformMetaInjectionEntryInterface {

    TARGET_SCHEMA( IValueMeta.TYPE_STRING, "The target schema" ),
    TARGET_TABLE( IValueMeta.TYPE_STRING, "The target table" ),
    COMMIT_SIZE( IValueMeta.TYPE_STRING, "The commit size" ),
    TRUNCATE_TABLE( IValueMeta.TYPE_STRING, "Truncate table? (Y/N)" ),
    SPECIFY_DATABASE_FIELDS( IValueMeta.TYPE_STRING, "Specify database fields? (Y/N)" ),
    IGNORE_INSERT_ERRORS( IValueMeta.TYPE_STRING, "Ignore insert errors? (Y/N)" ),
    USE_BATCH_UPDATE( IValueMeta.TYPE_STRING, "Use batch update for inserts? (Y/N)" ),

    PARTITION_OVER_TABLES( IValueMeta.TYPE_STRING, "Partition data over tables? (Y/N)" ),
    PARTITIONING_FIELD( IValueMeta.TYPE_STRING, "Partitioning field" ),
    PARTITION_DATA_PER( IValueMeta.TYPE_STRING, "Partition data per (month/day)" ),

    TABLE_NAME_DEFINED_IN_FIELD( IValueMeta.TYPE_STRING,
      "Is the name of the table defined in a field? (Y/N)" ),
    TABLE_NAME_FIELD( IValueMeta.TYPE_STRING, "Field that contains the name of table" ),
    STORE_TABLE_NAME( IValueMeta.TYPE_STRING, "Store the tablename field? (Y/N)" ),

    RETURN_AUTO_GENERATED_KEY( IValueMeta.TYPE_STRING, "Return auto-generated key? (Y/N)" ),
    AUTO_GENERATED_KEY_FIELD( IValueMeta.TYPE_STRING, "Name of auto-generated key field" ),

    DATABASE_FIELDS( IValueMeta.TYPE_NONE, "The database fields" ),
    DATABASE_FIELD( IValueMeta.TYPE_NONE, "One database field" ),
    DATABASE_FIELDNAME( IValueMeta.TYPE_STRING, "Table field" ),
    STREAM_FIELDNAME( IValueMeta.TYPE_STRING, "Stream field" );

    private int valueType;
    private String description;

    private Entry( int valueType, String description ) {
      this.valueType = valueType;
      this.description = description;
    }

    /**
     * @return the valueType
     */
    public int getValueType() {
      return valueType;
    }

    /**
     * @return the description
     */
    public String getDescription() {
      return description;
    }

    public static Entry findEntry( String key ) {
      return Entry.valueOf( key );
    }
  }

  private TableOutputMeta meta;

  public TableOutputMetaInjection( TableOutputMeta meta ) {
    this.meta = meta;
  }

  @Override
  public List<TransformInjectionMetaEntry> getTransformInjectionMetadataEntries() throws HopException {
    List<TransformInjectionMetaEntry> all = new ArrayList<TransformInjectionMetaEntry>();

    Entry[] topEntries =
      new Entry[] {
        Entry.TARGET_SCHEMA, Entry.TARGET_TABLE, Entry.COMMIT_SIZE, Entry.TRUNCATE_TABLE,
        Entry.SPECIFY_DATABASE_FIELDS, Entry.IGNORE_INSERT_ERRORS, Entry.USE_BATCH_UPDATE,
        Entry.PARTITION_OVER_TABLES, Entry.PARTITIONING_FIELD, Entry.PARTITION_DATA_PER,
        Entry.TABLE_NAME_DEFINED_IN_FIELD, Entry.TABLE_NAME_FIELD, Entry.STORE_TABLE_NAME,
        Entry.RETURN_AUTO_GENERATED_KEY, Entry.AUTO_GENERATED_KEY_FIELD, };
    for ( Entry topEntry : topEntries ) {
      all.add( new TransformInjectionMetaEntry( topEntry.name(), topEntry.getValueType(), topEntry.getDescription() ) );
    }

    // The fields
    //
    TransformInjectionMetaEntry fieldsEntry =
      new TransformInjectionMetaEntry(
        Entry.DATABASE_FIELDS.name(), IValueMeta.TYPE_NONE, Entry.DATABASE_FIELDS.description );
    all.add( fieldsEntry );
    TransformInjectionMetaEntry fieldEntry =
      new TransformInjectionMetaEntry(
        Entry.DATABASE_FIELD.name(), IValueMeta.TYPE_NONE, Entry.DATABASE_FIELD.description );
    fieldsEntry.getDetails().add( fieldEntry );

    Entry[] fieldsEntries = new Entry[] { Entry.DATABASE_FIELDNAME, Entry.STREAM_FIELDNAME, };
    for ( Entry entry : fieldsEntries ) {
      TransformInjectionMetaEntry metaEntry =
        new TransformInjectionMetaEntry( entry.name(), entry.getValueType(), entry.getDescription() );
      fieldEntry.getDetails().add( metaEntry );
    }

    return all;
  }

  @Override
  public void injectTransformMetadataEntries( List<TransformInjectionMetaEntry> all ) throws HopException {

    List<String> databaseFields = new ArrayList<>();
    List<String> streamFields = new ArrayList<>();

    // Parse the fields, inject into the meta class..
    //
    for ( TransformInjectionMetaEntry lookFields : all ) {
      Entry fieldsEntry = Entry.findEntry( lookFields.getKey() );
      if ( fieldsEntry == null ) {
        continue;
      }

      String lookValue = (String) lookFields.getValue();
      switch ( fieldsEntry ) {
        case DATABASE_FIELDS:
          for ( TransformInjectionMetaEntry lookField : lookFields.getDetails() ) {
            Entry fieldEntry = Entry.findEntry( lookField.getKey() );
            if ( fieldEntry == Entry.DATABASE_FIELD ) {

              String databaseFieldname = null;
              String streamFieldname = null;

              List<TransformInjectionMetaEntry> entries = lookField.getDetails();
              for ( TransformInjectionMetaEntry entry : entries ) {
                Entry metaEntry = Entry.findEntry( entry.getKey() );
                if ( metaEntry != null ) {
                  String value = (String) entry.getValue();
                  switch ( metaEntry ) {
                    case DATABASE_FIELDNAME:
                      databaseFieldname = value;
                      break;
                    case STREAM_FIELDNAME:
                      streamFieldname = value;
                      break;
                    default:
                      break;
                  }
                }
              }
              databaseFields.add( databaseFieldname );
              streamFields.add( streamFieldname );
            }
          }
          break;

        case TARGET_SCHEMA:
          meta.setSchemaName( lookValue );
          break;
        case TARGET_TABLE:
          meta.setTableName( lookValue );
          break;
        case COMMIT_SIZE:
          meta.setCommitSize( lookValue );
          break;
        case TRUNCATE_TABLE:
          meta.setTruncateTable( "Y".equalsIgnoreCase( lookValue ) );
          break;
        case SPECIFY_DATABASE_FIELDS:
          meta.setSpecifyFields( "Y".equalsIgnoreCase( lookValue ) );
          break;
        case IGNORE_INSERT_ERRORS:
          meta.setIgnoreErrors( "Y".equalsIgnoreCase( lookValue ) );
          break;
        case USE_BATCH_UPDATE:
          meta.setUseBatchUpdate( "Y".equalsIgnoreCase( lookValue ) );
          break;
        case PARTITION_OVER_TABLES:
          meta.setPartitioningEnabled( "Y".equalsIgnoreCase( lookValue ) );
          break;
        case PARTITIONING_FIELD:
          meta.setPartitioningField( lookValue );
          break;
        case PARTITION_DATA_PER:
          meta.setPartitioningDaily( "DAY".equalsIgnoreCase( lookValue ) );
          meta.setPartitioningMonthly( "MONTH".equalsIgnoreCase( lookValue ) );
          break;
        case TABLE_NAME_DEFINED_IN_FIELD:
          meta.setTableNameInField( "Y".equalsIgnoreCase( lookValue ) );
          break;
        case TABLE_NAME_FIELD:
          meta.setTableNameField( lookValue );
          break;
        case STORE_TABLE_NAME:
          meta.setTableNameInTable( "Y".equalsIgnoreCase( lookValue ) );
          break;
        case RETURN_AUTO_GENERATED_KEY:
          meta.setReturningGeneratedKeys( "Y".equalsIgnoreCase( lookValue ) );
          break;
        case AUTO_GENERATED_KEY_FIELD:
          meta.setGeneratedKeyField( lookValue );
          break;
        default:
          break;
      }
    }

    // Pass the grid to the transform metadata
    //
    if ( databaseFields.size() > 0 ) {
      meta.setFieldDatabase( databaseFields.toArray( new String[ databaseFields.size() ] ) );
      meta.setFieldStream( streamFields.toArray( new String[ streamFields.size() ] ) );
    }
  }

  public List<TransformInjectionMetaEntry> extractTransformMetadataEntries() throws HopException {
    List<TransformInjectionMetaEntry> list = new ArrayList<TransformInjectionMetaEntry>();

    list.add( TransformInjectionUtil.getEntry( Entry.TARGET_SCHEMA, meta.getSchemaName() ) );
    list.add( TransformInjectionUtil.getEntry( Entry.TARGET_TABLE, meta.getTableName() ) );
    list.add( TransformInjectionUtil.getEntry( Entry.COMMIT_SIZE, meta.getCommitSize() ) );
    list.add( TransformInjectionUtil.getEntry( Entry.TRUNCATE_TABLE, meta.truncateTable() ) );
    list.add( TransformInjectionUtil.getEntry( Entry.SPECIFY_DATABASE_FIELDS, meta.specifyFields() ) );
    list.add( TransformInjectionUtil.getEntry( Entry.IGNORE_INSERT_ERRORS, meta.ignoreErrors() ) );
    list.add( TransformInjectionUtil.getEntry( Entry.USE_BATCH_UPDATE, meta.useBatchUpdate() ) );

    list.add( TransformInjectionUtil.getEntry( Entry.PARTITION_OVER_TABLES, meta.isPartitioningEnabled() ) );
    list.add( TransformInjectionUtil.getEntry( Entry.PARTITIONING_FIELD, meta.getPartitioningField() ) );
    list.add( TransformInjectionUtil.getEntry( Entry.PARTITION_DATA_PER, meta.isPartitioningDaily()
      ? "DAY"
      : meta.isPartitioningMonthly() ? "MONTH" : "" ) );

    list.add( TransformInjectionUtil.getEntry( Entry.TABLE_NAME_DEFINED_IN_FIELD, meta.isTableNameInField() ) );
    list.add( TransformInjectionUtil.getEntry( Entry.TABLE_NAME_FIELD, meta.getTableNameField() ) );
    list.add( TransformInjectionUtil.getEntry( Entry.STORE_TABLE_NAME, meta.isTableNameInTable() ) );

    list.add( TransformInjectionUtil.getEntry( Entry.RETURN_AUTO_GENERATED_KEY, meta.isReturningGeneratedKeys() ) );
    list.add( TransformInjectionUtil.getEntry( Entry.AUTO_GENERATED_KEY_FIELD, meta.getGeneratedKeyField() ) );

    TransformInjectionMetaEntry fieldsEntry = TransformInjectionUtil.getEntry( Entry.DATABASE_FIELDS );
    list.add( fieldsEntry );
    for ( int i = 0; i < meta.getFieldDatabase().length; i++ ) {
      TransformInjectionMetaEntry fieldEntry = TransformInjectionUtil.getEntry( Entry.DATABASE_FIELD );
      List<TransformInjectionMetaEntry> details = fieldEntry.getDetails();
      details.add( TransformInjectionUtil.getEntry( Entry.DATABASE_FIELDNAME, meta.getFieldDatabase()[ i ] ) );
      details.add( TransformInjectionUtil.getEntry( Entry.STREAM_FIELDNAME, meta.getFieldStream()[ i ] ) );
      fieldsEntry.getDetails().add( fieldEntry );
    }

    return list;
  }

  public TableOutputMeta getMeta() {
    return meta;
  }
}
