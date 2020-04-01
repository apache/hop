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

package org.apache.hop.pipeline.steps.janino;

import org.apache.hop.core.Const;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.xml.XMLHandler;
import org.w3c.dom.Node;

import java.util.Objects;

public class JaninoMetaFunction implements Cloneable {
  public static final String XML_TAG = "formula";

  private String fieldName;
  private String formula;

  private int valueType;
  private int valueLength;
  private int valuePrecision;

  private String replaceField;

  /**
   * @param fieldName
   * @param formula
   * @param valueType
   * @param valueLength
   * @param valuePrecision
   */
  public JaninoMetaFunction( String fieldName, String formula, int valueType, int valueLength, int valuePrecision,
                             String replaceField ) {
    this.fieldName = fieldName;
    this.formula = formula;
    this.valueType = valueType;
    this.valueLength = valueLength;
    this.valuePrecision = valuePrecision;
    this.replaceField = replaceField;
  }

  public boolean equals( Object obj ) {
    if ( obj != null && ( obj.getClass().equals( this.getClass() ) ) ) {
      JaninoMetaFunction mf = (JaninoMetaFunction) obj;
      return ( getXML().equals( mf.getXML() ) );
    }

    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash( fieldName, formula, valueType, valueLength, valuePrecision, replaceField );
  }

  public Object clone() {
    try {
      JaninoMetaFunction retval = (JaninoMetaFunction) super.clone();
      return retval;
    } catch ( CloneNotSupportedException e ) {
      return null;
    }
  }

  public String getXML() {
    StringBuilder xml = new StringBuilder();

    xml.append( XMLHandler.openTag( XML_TAG ) );

    xml.append( XMLHandler.addTagValue( "field_name", fieldName ) );
    xml.append( XMLHandler.addTagValue( "formula_string", formula ) );
    xml.append( XMLHandler.addTagValue( "value_type", ValueMetaFactory.getValueMetaName( valueType ) ) );
    xml.append( XMLHandler.addTagValue( "value_length", valueLength ) );
    xml.append( XMLHandler.addTagValue( "value_precision", valuePrecision ) );
    xml.append( XMLHandler.addTagValue( "replace_field", replaceField ) );

    xml.append( XMLHandler.closeTag( XML_TAG ) );

    return xml.toString();
  }

  public JaninoMetaFunction( Node calcnode ) {
    fieldName = XMLHandler.getTagValue( calcnode, "field_name" );
    formula = XMLHandler.getTagValue( calcnode, "formula_string" );
    valueType = ValueMetaFactory.getIdForValueMeta( XMLHandler.getTagValue( calcnode, "value_type" ) );
    valueLength = Const.toInt( XMLHandler.getTagValue( calcnode, "value_length" ), -1 );
    valuePrecision = Const.toInt( XMLHandler.getTagValue( calcnode, "value_precision" ), -1 );
    replaceField = XMLHandler.getTagValue( calcnode, "replace_field" );
  }

  /**
   * @return Returns the fieldName.
   */
  public String getFieldName() {
    return fieldName;
  }

  /**
   * @param fieldName The fieldName to set.
   */
  public void setFieldName( String fieldName ) {
    this.fieldName = fieldName;
  }

  /**
   * @return Returns the valueLength.
   */
  public int getValueLength() {
    return valueLength;
  }

  /**
   * @param valueLength The valueLength to set.
   */
  public void setValueLength( int valueLength ) {
    this.valueLength = valueLength;
  }

  /**
   * @return Returns the valuePrecision.
   */
  public int getValuePrecision() {
    return valuePrecision;
  }

  /**
   * @param valuePrecision The valuePrecision to set.
   */
  public void setValuePrecision( int valuePrecision ) {
    this.valuePrecision = valuePrecision;
  }

  /**
   * @return Returns the valueType.
   */
  public int getValueType() {
    return valueType;
  }

  /**
   * @param valueType The valueType to set.
   */
  public void setValueType( int valueType ) {
    this.valueType = valueType;
  }

  /**
   * @return the formula
   */
  public String getFormula() {
    return formula;
  }

  /**
   * @param formula the formula to set
   */
  public void setFormula( String formula ) {
    this.formula = formula;
  }

  /**
   * @return the replaceField
   */
  public String getReplaceField() {
    return replaceField;
  }

  /**
   * @param replaceField the replaceField to set
   */
  public void setReplaceField( String replaceField ) {
    this.replaceField = replaceField;
  }
}
