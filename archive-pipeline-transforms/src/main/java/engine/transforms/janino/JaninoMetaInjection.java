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

package org.apache.hop.pipeline.transforms.janino;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.pipeline.transform.TransformInjectionMetaEntry;
import org.apache.hop.pipeline.transform.TransformInjectionUtil;
import org.apache.hop.pipeline.transform.TransformMetaInjectionEntryInterface;
import org.apache.hop.pipeline.transform.TransformMetaInjectionInterface;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This takes care of the external metadata injection into the JaninoMeta class
 *
 * @author Chris
 */
public class JaninoMetaInjection implements TransformMetaInjectionInterface {

  public enum Entry implements TransformMetaInjectionEntryInterface {

    EXPRESSION_FIELDS( IValueMeta.TYPE_NONE, "The formula fields" ),
    EXPRESSION_FIELD( IValueMeta.TYPE_NONE, "One formula field" ),
    NEW_FIELDNAME( IValueMeta.TYPE_STRING, "New field" ),
    JAVA_EXPRESSION( IValueMeta.TYPE_STRING, "Java expression" ),
    VALUE_TYPE( IValueMeta.TYPE_STRING, "Value type (For valid values go to http://wiki.pentaho.com/display/EAI/User+Defined+Java+Expression)" ),
    LENGTH( IValueMeta.TYPE_STRING, "Length" ),
    PRECISION( IValueMeta.TYPE_STRING, "Precision" ),
    REPLACE_VALUE( IValueMeta.TYPE_STRING, "Replace value" );

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

  private JaninoMeta meta;

  public JaninoMetaInjection( JaninoMeta meta ) {
    this.meta = meta;
  }

  @Override
  public List<TransformInjectionMetaEntry> getTransformInjectionMetadataEntries() throws HopException {
    List<TransformInjectionMetaEntry> all = new ArrayList<TransformInjectionMetaEntry>();

    // The fields
    //
    TransformInjectionMetaEntry fieldsEntry =
      new TransformInjectionMetaEntry(
        Entry.EXPRESSION_FIELDS.name(), IValueMeta.TYPE_NONE, Entry.EXPRESSION_FIELDS.description );
    all.add( fieldsEntry );
    TransformInjectionMetaEntry fieldEntry =
      new TransformInjectionMetaEntry(
        Entry.EXPRESSION_FIELD.name(), IValueMeta.TYPE_NONE, Entry.EXPRESSION_FIELD.description );
    fieldsEntry.getDetails().add( fieldEntry );

    Entry[] fieldsEntries = new Entry[] { Entry.NEW_FIELDNAME, Entry.JAVA_EXPRESSION, Entry.VALUE_TYPE,
      Entry.LENGTH, Entry.PRECISION, Entry.REPLACE_VALUE, };
    for ( Entry entry : fieldsEntries ) {
      TransformInjectionMetaEntry metaEntry =
        new TransformInjectionMetaEntry( entry.name(), entry.getValueType(), entry.getDescription() );
      fieldEntry.getDetails().add( metaEntry );
    }

    return all;
  }

  @Override
  public void injectTransformMetadataEntries( List<TransformInjectionMetaEntry> all ) throws HopException {

    List<String> fieldNames = new ArrayList<>();
    List<String> javaExpressions = new ArrayList<>();
    List<String> valueTypes = new ArrayList<>();
    List<String> lengths = new ArrayList<>();
    List<String> precisions = new ArrayList<>();
    List<String> replaceValues = new ArrayList<>();

    // Parse the fields, inject into the meta class..
    //
    for ( TransformInjectionMetaEntry lookFields : all ) {
      Entry fieldsEntry = Entry.findEntry( lookFields.getKey() );
      if ( fieldsEntry == null ) {
        continue;
      }

      switch ( fieldsEntry ) {
        case EXPRESSION_FIELDS:
          for ( TransformInjectionMetaEntry lookField : lookFields.getDetails() ) {
            Entry fieldEntry = Entry.findEntry( lookField.getKey() );
            if ( fieldEntry == Entry.EXPRESSION_FIELD ) {

              String newFieldname = null;
              String javaExpression = null;
              String valueType = null;
              String length = null;
              String precision = null;
              String replaceValue = null;

              List<TransformInjectionMetaEntry> entries = lookField.getDetails();
              for ( TransformInjectionMetaEntry entry : entries ) {
                Entry metaEntry = Entry.findEntry( entry.getKey() );
                if ( metaEntry != null ) {
                  String value = (String) entry.getValue();
                  switch ( metaEntry ) {
                    case NEW_FIELDNAME:
                      newFieldname = value;
                      break;
                    case JAVA_EXPRESSION:
                      javaExpression = value;
                      break;
                    case VALUE_TYPE:
                      valueType = value;
                      break;
                    case LENGTH:
                      length = value;
                      break;
                    case PRECISION:
                      precision = value;
                      break;
                    case REPLACE_VALUE:
                      replaceValue = value;
                      break;
                    default:
                      break;
                  }
                }
              }
              fieldNames.add( newFieldname );
              javaExpressions.add( javaExpression );
              valueTypes.add( valueType );
              lengths.add( length );
              precisions.add( precision );
              replaceValues.add( replaceValue );

            }
          }
          break;
        default:
          break;
      }
    }

    // Pass the grid to the transform metadata
    //
    if ( fieldNames.size() > 0 ) {
      JaninoMetaFunction[] fields = new JaninoMetaFunction[ fieldNames.size() ];

      Iterator<String> iFieldNames = fieldNames.iterator();
      Iterator<String> iJavaExpressions = javaExpressions.iterator();
      Iterator<String> iValueTypes = valueTypes.iterator();
      Iterator<String> iLengths = lengths.iterator();
      Iterator<String> iPrecisions = precisions.iterator();
      Iterator<String> iReplaceValues = replaceValues.iterator();

      int i = 0;

      while ( iFieldNames.hasNext() ) {
        fields[ i ] = new JaninoMetaFunction( iFieldNames.next(), iJavaExpressions.next(),
          ValueMetaFactory.getIdForValueMeta( iValueTypes.next() ), Const.toInt( iLengths.next(), -1 ),
          Const.toInt( iPrecisions.next(), -1 ), iReplaceValues.next() );

        i++;
      }

      meta.setFormula( fields );
    }
  }

  public List<TransformInjectionMetaEntry> extractTransformMetadataEntries() throws HopException {
    List<TransformInjectionMetaEntry> list = new ArrayList<TransformInjectionMetaEntry>();

    TransformInjectionMetaEntry fieldsEntry = TransformInjectionUtil.getEntry( Entry.EXPRESSION_FIELDS );
    list.add( fieldsEntry );
    for ( int i = 0; i < meta.getFormula().length; i++ ) {
      TransformInjectionMetaEntry fieldEntry = TransformInjectionUtil.getEntry( Entry.EXPRESSION_FIELD );
      List<TransformInjectionMetaEntry> details = fieldEntry.getDetails();
      details.add( TransformInjectionUtil.getEntry( Entry.NEW_FIELDNAME, meta.getFormula()[ i ].getFieldName() ) );
      details.add( TransformInjectionUtil.getEntry( Entry.JAVA_EXPRESSION, meta.getFormula()[ i ].getFormula() ) );
      details.add( TransformInjectionUtil.getEntry( Entry.VALUE_TYPE, ValueMetaFactory.getValueMetaName( meta.getFormula()[ i ].getValueType() ) ) );
      details.add( TransformInjectionUtil.getEntry( Entry.LENGTH, meta.getFormula()[ i ].getValueLength() ) );
      details.add( TransformInjectionUtil.getEntry( Entry.PRECISION, meta.getFormula()[ i ].getValuePrecision() ) );
      details.add( TransformInjectionUtil.getEntry( Entry.REPLACE_VALUE, meta.getFormula()[ i ].getReplaceField() ) );


      fieldsEntry.getDetails().add( fieldEntry );
    }

    return list;
  }

  public JaninoMeta getMeta() {
    return meta;
  }
}
