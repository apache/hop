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

package org.apache.hop.pipeline.transforms.fixedinput;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transform.TransformInjectionMetaEntry;
import org.apache.hop.pipeline.transform.TransformMetaInjectionInterface;

import java.util.ArrayList;
import java.util.List;

/**
 * Metadata injection interface for the Fixed File Input transform.
 *
 * @author Matt
 */
public class FixedInputMetaInjection implements TransformMetaInjectionInterface {

  private FixedInputMeta meta;

  public FixedInputMetaInjection( FixedInputMeta meta ) {
    this.meta = meta;
  }

  @Override
  public List<TransformInjectionMetaEntry> getTransformInjectionMetadataEntries() throws HopException {
    List<TransformInjectionMetaEntry> all = new ArrayList<TransformInjectionMetaEntry>();

    // Add the fields...
    //
    TransformInjectionMetaEntry fieldsEntry =
      new TransformInjectionMetaEntry( Entry.FIELDS.name(), Entry.FIELDS.getValueType(), Entry.FIELDS
        .getDescription() );
    all.add( fieldsEntry );

    TransformInjectionMetaEntry fieldEntry =
      new TransformInjectionMetaEntry( Entry.FIELD.name(), Entry.FIELD.getValueType(), Entry.FIELD.getDescription() );
    fieldsEntry.getDetails().add( fieldEntry );

    for ( Entry entry : Entry.values() ) {
      if ( entry.getParent() == Entry.FIELD ) {
        TransformInjectionMetaEntry metaEntry =
          new TransformInjectionMetaEntry( entry.name(), entry.getValueType(), entry.getDescription() );
        fieldEntry.getDetails().add( metaEntry );
      } else {
        if ( entry.getParent() == null && entry != Entry.FIELDS && entry != Entry.FIELD ) {
          TransformInjectionMetaEntry metaEntry =
            new TransformInjectionMetaEntry( entry.name(), entry.getValueType(), entry.getDescription() );
          all.add( metaEntry );
        }
      }
    }

    return all;
  }

  @Override
  public void injectTransformMetadataEntries( List<TransformInjectionMetaEntry> all ) throws HopException {

    List<FixedFileInputField> fixedInputFields = new ArrayList<FixedFileInputField>();

    // Parse the fields, inject into the meta class..
    //
    for ( TransformInjectionMetaEntry lookFields : all ) {
      String lookValue = (String) lookFields.getValue();
      Entry fieldsEntry = Entry.findEntry( lookFields.getKey() );
      if ( fieldsEntry != null ) {
        switch ( fieldsEntry ) {
          case FIELDS:
            for ( TransformInjectionMetaEntry lookField : lookFields.getDetails() ) {
              Entry fieldEntry = Entry.findEntry( lookField.getKey() );
              if ( fieldEntry != null ) {
                if ( fieldEntry == Entry.FIELD ) {

                  FixedFileInputField inputField = new FixedFileInputField();

                  List<TransformInjectionMetaEntry> entries = lookField.getDetails();
                  for ( TransformInjectionMetaEntry entry : entries ) {
                    Entry metaEntry = Entry.findEntry( entry.getKey() );
                    if ( metaEntry != null ) {
                      String value = (String) entry.getValue();
                      switch ( metaEntry ) {
                        case NAME:
                          inputField.setName( value );
                          break;
                        case TYPE:
                          inputField.setType( ValueMetaFactory.getIdForValueMeta( value ) );
                          break;
                        case WIDTH:
                          inputField.setWidth( Const.toInt( value, -1 ) );
                          break;
                        case LENGTH:
                          inputField.setLength( Const.toInt( value, -1 ) );
                          break;
                        case PRECISION:
                          inputField.setPrecision( Const.toInt( value, -1 ) );
                          break;
                        case CURRENCY:
                          inputField.setCurrency( value );
                          break;
                        case GROUP:
                          inputField.setGrouping( value );
                          break;
                        case DECIMAL:
                          inputField.setDecimal( value );
                          break;
                        case FORMAT:
                          inputField.setFormat( value );
                          break;
                        case TRIM_TYPE:
                          inputField.setTrimType( ValueMetaString.getTrimTypeByCode( value ) );
                          break;
                        default:
                          break;
                      }
                    }
                  }

                  fixedInputFields.add( inputField );
                }
              }
            }
            break;

          case FILENAME:
            meta.setFilename( lookValue );
            break;
          case HEADER_PRESENT:
            meta.setHeaderPresent( "Y".equalsIgnoreCase( lookValue ) );
            break;
          case LINE_WIDTH:
            meta.setLineWidth( lookValue );
            break;
          case BUFFER_SIZE:
            meta.setBufferSize( lookValue );
            break;
          case LAZY_CONVERSION_ACTIVE:
            meta.setLazyConversionActive( "Y".equalsIgnoreCase( lookValue ) );
            break;
          case LINE_FEED_PRESENT:
            meta.setLineFeedPresent( "Y".equalsIgnoreCase( lookValue ) );
            break;
          case RUNNING_IN_PARALLEL:
            meta.setRunningInParallel( "Y".equalsIgnoreCase( lookValue ) );
            break;
          case FILE_TYPE_CODE:
            meta.setFileType( FixedInputMeta.getFileType( lookValue ) );
            break;
          case ADD_TO_RESULT:
            meta.setAddResultFile( "Y".equalsIgnoreCase( lookValue ) );
            break;
          default:
            break;
        }
      }
    }

    // Pass the grid to the transform metadata
    //
    meta.setFieldDefinition( fixedInputFields.toArray( new FixedFileInputField[ fixedInputFields.size() ] ) );
  }

  @Override
  public List<TransformInjectionMetaEntry> extractTransformMetadataEntries() throws HopException {
    return null;
  }

  public FixedInputMeta getMeta() {
    return meta;
  }

  private enum Entry {

    FIELDS( IValueMeta.TYPE_NONE, "All the data fields in the fixed width file" ), FIELD(
      IValueMeta.TYPE_NONE, "One data field" ),

    NAME( FIELD, IValueMeta.TYPE_STRING, "Field name" ), TYPE(
      FIELD, IValueMeta.TYPE_STRING, "Field data type" ), WIDTH(
      FIELD, IValueMeta.TYPE_STRING, "Field width" ), LENGTH(
      FIELD, IValueMeta.TYPE_STRING, "Field length" ), PRECISION(
      FIELD, IValueMeta.TYPE_STRING, "Field precision" ), FORMAT(
      FIELD, IValueMeta.TYPE_STRING, "Field conversion format" ), TRIM_TYPE(
      FIELD, IValueMeta.TYPE_STRING, "Field trim type (none, left, right, both)" ), CURRENCY(
      FIELD, IValueMeta.TYPE_STRING, "Field currency symbol" ), DECIMAL(
      FIELD, IValueMeta.TYPE_STRING, "Field decimal symbol" ), GROUP(
      FIELD, IValueMeta.TYPE_STRING, "Field group symbol" ),

    FILENAME( IValueMeta.TYPE_STRING, "Filename" ), HEADER_PRESENT(
      IValueMeta.TYPE_STRING, "Header present? (Y/N)" ), LINE_WIDTH(
      IValueMeta.TYPE_STRING, "The line width" ), BUFFER_SIZE(
      IValueMeta.TYPE_STRING, "The buffer size" ), LAZY_CONVERSION_ACTIVE(
      IValueMeta.TYPE_STRING, "Lazy conversion active? (Y/N)" ), LINE_FEED_PRESENT(
      IValueMeta.TYPE_STRING, "Line feed present? (Y/N)" ), RUNNING_IN_PARALLEL(
      IValueMeta.TYPE_STRING, "Running in parallel? (Y/N)" ), FILE_TYPE_CODE(
      IValueMeta.TYPE_STRING, "File type code (NONE, UNIX, DOS)" ), ADD_TO_RESULT(
      IValueMeta.TYPE_STRING, "Add filename to result? (Y/N)" );

    private int valueType;
    private String description;
    private Entry parent;

    private Entry( int valueType, String description ) {
      this.valueType = valueType;
      this.description = description;
    }

    private Entry( Entry parent, int valueType, String description ) {
      this.parent = parent;
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

    public Entry getParent() {
      return parent;
    }
  }
}
