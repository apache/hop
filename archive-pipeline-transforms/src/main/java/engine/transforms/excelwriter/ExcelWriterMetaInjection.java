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

package org.apache.hop.pipeline.transforms.excelwriter;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.transform.TransformInjectionMetaEntry;
import org.apache.hop.pipeline.transform.TransformMetaInjectionInterface;

import java.util.ArrayList;
import java.util.List;

/**
 * Injection support for the Excel Writer transform.
 * <p/>
 * Injection only supported for attributes of the output fields.
 *
 * @author Gretchen Moran
 */
public class ExcelWriterMetaInjection implements TransformMetaInjectionInterface {

  private static Class<?> PKG = ExcelWriterTransformMeta.class; // for i18n purposes, needed by Translator!!

  private ExcelWriterTransformMeta meta;

  public ExcelWriterMetaInjection( ExcelWriterTransformMeta meta ) {
    this.meta = meta;
  }

  @Override
  public List<TransformInjectionMetaEntry> getTransformInjectionMetadataEntries() throws HopException {
    List<TransformInjectionMetaEntry> all = new ArrayList<TransformInjectionMetaEntry>();

    TransformInjectionMetaEntry fieldsEntry =
      new TransformInjectionMetaEntry( "FIELDS",
        IValueMeta.TYPE_NONE, BaseMessages.getString( PKG, "ExcelWriterMetaInjection.AllFields" ) );
    all.add( fieldsEntry );

    TransformInjectionMetaEntry fieldEntry =
      new TransformInjectionMetaEntry( "FIELD",
        IValueMeta.TYPE_NONE, BaseMessages.getString( PKG, "ExcelWriterMetaInjection.AllFields" ) );
    fieldsEntry.getDetails().add( fieldEntry );

    for ( Entry entry : Entry.values() ) {
      if ( entry.getValueType() != IValueMeta.TYPE_NONE ) {
        TransformInjectionMetaEntry metaEntry =
          new TransformInjectionMetaEntry( entry.name(), entry.getValueType(), entry.getDescription() );
        fieldEntry.getDetails().add( metaEntry );
      }
    }

    return all;
  }

  @Override
  public void injectTransformMetadataEntries( List<TransformInjectionMetaEntry> all ) throws HopException {

    List<ExcelWriter Field> excelOutputFields = new ArrayList<ExcelWriter Field>();

    // Parse the fields in the Excel Transform, setting the metadata based on values passed.

    for ( TransformInjectionMetaEntry lookFields : all ) {
      Entry fieldsEntry = Entry.findEntry( lookFields.getKey() );
      if ( fieldsEntry != null ) {
        if ( fieldsEntry == Entry.FIELDS ) {
          for ( TransformInjectionMetaEntry lookField : lookFields.getDetails() ) {
            Entry fieldEntry = Entry.findEntry( lookField.getKey() );
            if ( fieldEntry != null ) {
              if ( fieldEntry == Entry.FIELD ) {

                ExcelWriter Field excelOutputField = new ExcelWriter Field();

                List<TransformInjectionMetaEntry> entries = lookField.getDetails();
                for ( TransformInjectionMetaEntry entry : entries ) {
                  Entry metaEntry = Entry.findEntry( entry.getKey() );
                  if ( metaEntry != null ) {
                    Object value = entry.getValue();
                    if ( value != null ) {
                      switch ( metaEntry ) {
                        case NAME:
                          excelOutputField.setName( (String) value );
                          break;
                        case TYPE:
                          excelOutputField.setType( (String) value );
                          break;
                        case FORMAT:
                          excelOutputField.setFormat( (String) value );
                          break;
                        case STYLECELL:
                          excelOutputField.setStyleCell( (String) value );
                          break;
                        case FIELDTITLE:
                          excelOutputField.setTitle( (String) value );
                          break;
                        case TITLESTYLE:
                          excelOutputField.setTitleStyleCell( (String) value );
                          break;
                        case FORMULA:
                          excelOutputField.setFormula( (Boolean) value );
                          break;
                        case HYPERLINKFIELD:
                          excelOutputField.setHyperlinkField( (String) value );
                          break;
                        case CELLCOMMENT:
                          excelOutputField.setCommentField( (String) value );
                          break;
                        case COMMENTAUTHOR:
                          excelOutputField.setCommentAuthorField( (String) value );
                          break;
                        default:
                          break;
                      }
                    }
                  }
                }

                excelOutputFields.add( excelOutputField );
              }
            }
          }
        }
      }
    }

    meta.setOutputFields( excelOutputFields.toArray( new ExcelWriter Field[ excelOutputFields.size() ] ) );

  }

  public List<TransformInjectionMetaEntry> extractTransformMetadataEntries() throws HopException {
    return null;
  }

  public ExcelWriterTransformMeta getMeta() {
    return meta;
  }

  private enum Entry {

    FIELDS( IValueMeta.TYPE_NONE,
      BaseMessages.getString( PKG, "ExcelWriterMetaInjection.AllFields" ) ),
    FIELD( IValueMeta.TYPE_NONE,
      BaseMessages.getString( PKG, "ExcelWriterMetaInjection.AllFields" ) ),

    NAME( IValueMeta.TYPE_STRING,
      BaseMessages.getString( PKG, "ExcelWriterMetaInjection.FieldName" ) ),
    TYPE( IValueMeta.TYPE_STRING,
      BaseMessages.getString( PKG, "ExcelWriterMetaInjection.FieldType" ) ),
    FORMAT( IValueMeta.TYPE_STRING,
      BaseMessages.getString( PKG, "ExcelWriterDialog.FormatColumn.Column" ) ),
    STYLECELL( IValueMeta.TYPE_STRING,
      BaseMessages.getString( PKG, "ExcelWriterDialog.UseStyleCell.Column" ) ),
    FIELDTITLE( IValueMeta.TYPE_STRING,
      BaseMessages.getString( PKG, "ExcelWriterDialog.TitleColumn.Column" ) ),
    TITLESTYLE( IValueMeta.TYPE_STRING,
      BaseMessages.getString( PKG, "ExcelWriterDialog.UseTitleStyleCell.Column" ) ),
    FORMULA( IValueMeta.TYPE_BOOLEAN,
      BaseMessages.getString( PKG, "ExcelWriterDialog.FormulaField.Column" ) ),
    HYPERLINKFIELD( IValueMeta.TYPE_STRING,
      BaseMessages.getString( PKG, "ExcelWriterDialog.HyperLinkField.Column" ) ),
    CELLCOMMENT( IValueMeta.TYPE_STRING,
      BaseMessages.getString( PKG, "ExcelWriterDialog.CommentField.Column" ) ),
    COMMENTAUTHOR( IValueMeta.TYPE_STRING,
      BaseMessages.getString( PKG, "ExcelWriterDialog.CommentAuthor.Column" ) );


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

}
