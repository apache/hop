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

package org.apache.hop.pipeline.transforms.parallelgzipcsv;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transform.TransformInjectionMetaEntry;
import org.apache.hop.pipeline.transform.TransformMetaInjectionInterface;
import org.apache.hop.pipeline.transforms.fileinput.TextFileInputField;

import java.util.ArrayList;
import java.util.List;

/**
 * This takes care of the external metadata injection into the ParGzipCsvInputMeta class
 *
 * @author Matt
 */
public class ParGzipCsvInputMetaInjection implements TransformMetaInjectionInterface {

  private enum Entry {

    FILENAME( IValueMeta.TYPE_STRING, "The file name to read" ), FILENAME_FIELD(
      IValueMeta.TYPE_STRING, "The filename field (if the transform reads file names)" ),
    INCLUDING_FILENAMES( IValueMeta.TYPE_STRING, "Include file name in output? (Y/N)" ),
    ROW_NUMBER_FIELD(
      IValueMeta.TYPE_STRING, "The row number field" ), HEADER_PRESENT(
      IValueMeta.TYPE_STRING, "Is there a header row? (Y/N)" ), DELIMITER(
      IValueMeta.TYPE_STRING, "The field delimiter" ), ENCLOSURE(
      IValueMeta.TYPE_STRING, "The field enclosure" ), BUFFER_SIZE(
      IValueMeta.TYPE_STRING, "I/O buffer size" ), LAZY_CONVERSION(
      IValueMeta.TYPE_STRING, "Use lazy conversion? (Y/N)" ), ADD_FILES_TO_RESULT(
      IValueMeta.TYPE_STRING, "Add files to result? (Y/N)" ), RUN_IN_PARALLEL(
      IValueMeta.TYPE_STRING, "Run in parallel? (Y/N)" ), ENCODING(
      IValueMeta.TYPE_STRING, "The file encoding" ),

    FIELDS( IValueMeta.TYPE_NONE, "The fields" ), FIELD( IValueMeta.TYPE_NONE, "One field" ),
    FIELD_NAME( IValueMeta.TYPE_STRING, "Name" ), FIELD_POSITION(
      IValueMeta.TYPE_STRING, "Position" ), FIELD_LENGTH( IValueMeta.TYPE_STRING, "Length" ),
    FIELD_TYPE( IValueMeta.TYPE_STRING, "Data type (String, Number, ...)" ), FIELD_IGNORE(
      IValueMeta.TYPE_STRING, "Ignore? (Y/N)" ),
    FIELD_FORMAT( IValueMeta.TYPE_STRING, "Format" ), FIELD_TRIM_TYPE(
      IValueMeta.TYPE_STRING, "Trim type (none, left, right, both)" ), FIELD_PRECISION(
      IValueMeta.TYPE_STRING, "Precision" ), FIELD_DECIMAL(
      IValueMeta.TYPE_STRING, "Decimal symbol" ), FIELD_GROUP(
      IValueMeta.TYPE_STRING, "Grouping symbol" ), FIELD_CURRENCY(
      IValueMeta.TYPE_STRING, "Currency symbol" ), FIELD_REPEAT(
      IValueMeta.TYPE_STRING, "Repeat values? (Y/N)" ), FIELD_NULL_STRING(
      IValueMeta.TYPE_STRING, "The null string" ), FIELD_IF_NULL(
      IValueMeta.TYPE_STRING, "The default value if null" );

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

  private ParGzipCsvInputMeta meta;

  public ParGzipCsvInputMetaInjection( ParGzipCsvInputMeta meta ) {
    this.meta = meta;
  }

  @Override
  public List<TransformInjectionMetaEntry> getTransformInjectionMetadataEntries() throws HopException {
    List<TransformInjectionMetaEntry> all = new ArrayList<TransformInjectionMetaEntry>();

    Entry[] topEntries =
      new Entry[] {
        Entry.FILENAME, Entry.FILENAME_FIELD, Entry.INCLUDING_FILENAMES, Entry.ROW_NUMBER_FIELD,
        Entry.HEADER_PRESENT, Entry.DELIMITER, Entry.ENCLOSURE, Entry.BUFFER_SIZE, Entry.LAZY_CONVERSION,
        Entry.ADD_FILES_TO_RESULT, Entry.RUN_IN_PARALLEL, Entry.ENCODING, };
    for ( Entry topEntry : topEntries ) {
      all.add( new TransformInjectionMetaEntry( topEntry.name(), topEntry.getValueType(), topEntry.getDescription() ) );
    }

    // The fields...
    //
    TransformInjectionMetaEntry fieldsEntry =
      new TransformInjectionMetaEntry( Entry.FIELDS.name(), IValueMeta.TYPE_NONE, Entry.FIELDS.description );
    all.add( fieldsEntry );
    TransformInjectionMetaEntry fieldEntry =
      new TransformInjectionMetaEntry( Entry.FIELD.name(), IValueMeta.TYPE_NONE, Entry.FIELD.description );
    fieldsEntry.getDetails().add( fieldEntry );

    Entry[] aggEntries =
      new Entry[] {
        Entry.FIELD_NAME, Entry.FIELD_POSITION, Entry.FIELD_LENGTH, Entry.FIELD_TYPE, Entry.FIELD_IGNORE,
        Entry.FIELD_FORMAT, Entry.FIELD_TRIM_TYPE, Entry.FIELD_PRECISION, Entry.FIELD_DECIMAL,
        Entry.FIELD_GROUP, Entry.FIELD_CURRENCY, Entry.FIELD_REPEAT, Entry.FIELD_NULL_STRING,
        Entry.FIELD_IF_NULL, };
    for ( Entry entry : aggEntries ) {
      TransformInjectionMetaEntry metaEntry =
        new TransformInjectionMetaEntry( entry.name(), entry.getValueType(), entry.getDescription() );
      fieldEntry.getDetails().add( metaEntry );
    }

    return all;
  }

  @Override
  public void injectTransformMetadataEntries( List<TransformInjectionMetaEntry> all ) throws HopException {

    List<TextFileInputField> fields = new ArrayList<TextFileInputField>();

    // Parse the fields, inject into the meta class..
    //
    for ( TransformInjectionMetaEntry lookFields : all ) {
      Entry fieldsEntry = Entry.findEntry( lookFields.getKey() );
      if ( fieldsEntry == null ) {
        continue;
      }

      String lookValue = (String) lookFields.getValue();
      switch ( fieldsEntry ) {
        case FIELDS:
          for ( TransformInjectionMetaEntry lookField : lookFields.getDetails() ) {
            Entry fieldEntry = Entry.findEntry( lookField.getKey() );
            if ( fieldEntry == Entry.FIELD ) {

              TextFileInputField field = new TextFileInputField();

              List<TransformInjectionMetaEntry> entries = lookField.getDetails();
              for ( TransformInjectionMetaEntry entry : entries ) {
                Entry metaEntry = Entry.findEntry( entry.getKey() );
                if ( metaEntry != null ) {
                  String value = (String) entry.getValue();
                  switch ( metaEntry ) {
                    case FIELD_NAME:
                      field.setName( value );
                      break;
                    case FIELD_POSITION:
                      field.setPosition( Const.toInt( value, -1 ) );
                      break;
                    case FIELD_LENGTH:
                      field.setLength( Const.toInt( value, -1 ) );
                      break;
                    case FIELD_TYPE:
                      field.setType( ValueMetaFactory.getIdForValueMeta( value ) );
                      break;
                    case FIELD_IGNORE:
                      field.setIgnored( "Y".equalsIgnoreCase( value ) );
                      break;
                    case FIELD_FORMAT:
                      field.setFormat( value );
                      break;
                    case FIELD_TRIM_TYPE:
                      field.setTrimType( ValueMetaString.getTrimTypeByCode( value ) );
                      break;
                    case FIELD_PRECISION:
                      field.setPrecision( Const.toInt( value, -1 ) );
                      break;
                    case FIELD_DECIMAL:
                      field.setDecimalSymbol( value );
                      break;
                    case FIELD_GROUP:
                      field.setGroupSymbol( value );
                      break;
                    case FIELD_CURRENCY:
                      field.setCurrencySymbol( value );
                      break;
                    case FIELD_REPEAT:
                      field.setRepeated( "Y".equalsIgnoreCase( value ) );
                      break;
                    case FIELD_NULL_STRING:
                      field.setNullString( value );
                      break;
                    case FIELD_IF_NULL:
                      field.setIfNullValue( value );
                      break;
                    default:
                      break;
                  }
                }
              }
              fields.add( field );
            }
          }
          break;

        case FILENAME:
          meta.setFilename( lookValue );
          break;
        case FILENAME_FIELD:
          meta.setFilenameField( lookValue );
          break;
        case ROW_NUMBER_FIELD:
          meta.setRowNumField( lookValue );
          break;
        case INCLUDING_FILENAMES:
          meta.setIncludingFilename( "Y".equalsIgnoreCase( lookValue ) );
          break;
        case DELIMITER:
          meta.setDelimiter( lookValue );
          break;
        case ENCLOSURE:
          meta.setEnclosure( lookValue );
          break;
        case HEADER_PRESENT:
          meta.setHeaderPresent( "Y".equalsIgnoreCase( lookValue ) );
          break;
        case BUFFER_SIZE:
          meta.setBufferSize( lookValue );
          break;
        case LAZY_CONVERSION:
          meta.setLazyConversionActive( "Y".equalsIgnoreCase( lookValue ) );
          break;
        case ADD_FILES_TO_RESULT:
          meta.setAddResultFile( "Y".equalsIgnoreCase( lookValue ) );
          break;
        case RUN_IN_PARALLEL:
          meta.setRunningInParallel( "Y".equalsIgnoreCase( lookValue ) );
          break;
        case ENCODING:
          meta.setEncoding( lookValue );
          break;
        default:
          break;
      }
    }

    // If we got fields, use them, otherwise leave the defaults alone.
    //
    if ( fields.size() > 0 ) {
      meta.setInputFields( fields.toArray( new TextFileInputField[ fields.size() ] ) );
    }
  }

  public List<TransformInjectionMetaEntry> extractTransformMetadataEntries() throws HopException {
    return null;
  }

  public ParGzipCsvInputMeta getMeta() {
    return meta;
  }
}
