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

package org.apache.hop.pipeline.transforms.denormaliser;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.pipeline.transform.TransformInjectionMetaEntry;
import org.apache.hop.pipeline.transform.TransformMetaInjectionInterface;

import java.util.ArrayList;
import java.util.List;

/**
 * To keep it simple, this metadata injection interface only supports the fields to denormalize for the time being.
 *
 * @author Matt
 */
public class DenormaliserMetaInjection implements TransformMetaInjectionInterface {

  private DenormaliserMeta meta;

  public DenormaliserMetaInjection( DenormaliserMeta meta ) {
    this.meta = meta;
  }

  @Override
  public List<TransformInjectionMetaEntry> getTransformInjectionMetadataEntries() throws HopException {
    List<TransformInjectionMetaEntry> all = new ArrayList<TransformInjectionMetaEntry>();

    TransformInjectionMetaEntry fieldsEntry =
      new TransformInjectionMetaEntry( "FIELDS", IValueMeta.TYPE_NONE, "All the fields on the spreadsheets" );
    all.add( fieldsEntry );

    TransformInjectionMetaEntry fieldEntry =
      new TransformInjectionMetaEntry( "FIELD", IValueMeta.TYPE_NONE, "All the fields on the spreadsheets" );
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

    List<DenormaliserTargetField> denormaliserTargetFields = new ArrayList<DenormaliserTargetField>();

    // Parse the fields, inject into the meta class..
    //
    for ( TransformInjectionMetaEntry lookFields : all ) {
      Entry fieldsEntry = Entry.findEntry( lookFields.getKey() );
      if ( fieldsEntry != null ) {
        if ( fieldsEntry == Entry.FIELDS ) {
          for ( TransformInjectionMetaEntry lookField : lookFields.getDetails() ) {
            Entry fieldEntry = Entry.findEntry( lookField.getKey() );
            if ( fieldEntry != null ) {
              if ( fieldEntry == Entry.FIELD ) {

                DenormaliserTargetField inputField = new DenormaliserTargetField();

                List<TransformInjectionMetaEntry> entries = lookField.getDetails();
                for ( TransformInjectionMetaEntry entry : entries ) {
                  Entry metaEntry = Entry.findEntry( entry.getKey() );
                  if ( metaEntry != null ) {
                    String value = (String) entry.getValue();
                    switch ( metaEntry ) {
                      case NAME:
                        inputField.setFieldName( value );
                        break;
                      case KEY_VALUE:
                        inputField.setKeyValue( value );
                        break;
                      case TARGET_NAME:
                        inputField.setTargetName( value );
                        break;
                      case TARGET_TYPE:
                        inputField.setTargetType( ValueMetaFactory.getIdForValueMeta( value ) );
                        break;
                      case TARGET_LENGTH:
                        inputField.setTargetLength( Const.toInt( value, -1 ) );
                        break;
                      case TARGET_PRECISION:
                        inputField.setTargetPrecision( Const.toInt( value, -1 ) );
                        break;
                      case TARGET_CURRENCY:
                        inputField.setTargetCurrencySymbol( value );
                        break;
                      case TARGET_GROUP:
                        inputField.setTargetGroupingSymbol( value );
                        break;
                      case TARGET_DECIMAL:
                        inputField.setTargetDecimalSymbol( value );
                        break;
                      case TARGET_FORMAT:
                        inputField.setTargetFormat( value );
                        break;
                      case TARGET_AGGREGATION:
                        inputField.setTargetAggregationType( DenormaliserTargetField.getAggregationType( value ) );
                        break;
                      default:
                        break;
                    }
                  }
                }

                denormaliserTargetFields.add( inputField );
              }
            }
          }
        }
      }
    }

    if ( !denormaliserTargetFields.isEmpty() ) {
      // Pass the grid to the transform metadata
      //
      meta.setDenormaliserTargetField( denormaliserTargetFields.toArray(
        new DenormaliserTargetField[ denormaliserTargetFields.size() ] ) );
    }

  }

  @Override
  public List<TransformInjectionMetaEntry> extractTransformMetadataEntries() throws HopException {
    return null;
  }

  public DenormaliserMeta getMeta() {
    return meta;
  }

  private enum Entry {

    FIELDS( IValueMeta.TYPE_NONE, "All the fields" ),
    FIELD( IValueMeta.TYPE_NONE, "One field" ),

    TARGET_NAME( IValueMeta.TYPE_STRING, "Target field name" ),
    NAME( IValueMeta.TYPE_STRING, "Value field name" ),
    KEY_VALUE( IValueMeta.TYPE_STRING, "Key value" ),
    TARGET_TYPE( IValueMeta.TYPE_STRING, "Target field type" ),
    TARGET_LENGTH( IValueMeta.TYPE_STRING, "Target field length" ),
    TARGET_PRECISION( IValueMeta.TYPE_STRING, "Target field precision" ),
    TARGET_CURRENCY( IValueMeta.TYPE_STRING, "Target field currency symbol" ),
    TARGET_DECIMAL( IValueMeta.TYPE_STRING, "Target field decimal symbol" ),
    TARGET_GROUP( IValueMeta.TYPE_STRING, "Target field group symbol" ),
    TARGET_FORMAT( IValueMeta.TYPE_STRING, "Target field format" ),
    TARGET_AGGREGATION(
      IValueMeta.TYPE_STRING, "Target aggregation (-, SUM, AVERAGE, MIN, MAX, COUNT_ALL, CONCAT_COMMA)" );

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
