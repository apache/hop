/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hop.pipeline.transforms.janino;

import org.apache.hop.core.Const;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.xml.XmlHandler;
import org.w3c.dom.Node;

import java.util.Objects;

public class JaninoMetaFunction implements Cloneable {
  public static final String XML_TAG = "formula";

  @Injection(name = "FIELD_NAME", group = "FORMULA")
  private String fieldName;

  @Injection(name = "FIELD_FORMULA", group = "FORMULA")
  private String formula;

  private int valueType;

  @Injection(name = "VALUE_LENGTH", group = "FORMULA")
  private int valueLength;

  @Injection(name = "VALUE_PRECISION", group = "FORMULA")
  private int valuePrecision;

  @Injection(name = "REPLACE_FIELD", group = "FORMULA")
  private String replaceField;

  public JaninoMetaFunction() {
  }

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
      return ( getXml().equals( mf.getXml() ) );
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

  public String getXml() {
    StringBuilder xml = new StringBuilder();

    xml.append( XmlHandler.openTag( XML_TAG ) );

    xml.append( XmlHandler.addTagValue( "field_name", fieldName ) );
    xml.append( XmlHandler.addTagValue( "formula_string", formula ) );
    xml.append( XmlHandler.addTagValue( "value_type", ValueMetaFactory.getValueMetaName( valueType ) ) );
    xml.append( XmlHandler.addTagValue( "value_length", valueLength ) );
    xml.append( XmlHandler.addTagValue( "value_precision", valuePrecision ) );
    xml.append( XmlHandler.addTagValue( "replace_field", replaceField ) );

    xml.append( XmlHandler.closeTag( XML_TAG ) );

    return xml.toString();
  }

  public JaninoMetaFunction( Node calcnode ) {
    fieldName = XmlHandler.getTagValue( calcnode, "field_name" );
    formula = XmlHandler.getTagValue( calcnode, "formula_string" );
    valueType = ValueMetaFactory.getIdForValueMeta( XmlHandler.getTagValue( calcnode, "value_type" ) );
    valueLength = Const.toInt( XmlHandler.getTagValue( calcnode, "value_length" ), -1 );
    valuePrecision = Const.toInt( XmlHandler.getTagValue( calcnode, "value_precision" ), -1 );
    replaceField = XmlHandler.getTagValue( calcnode, "replace_field" );
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

  @Injection( name = "VALUE_TYPE", group = "FORMULA" )
  public void setValueType( String typeDesc ) {
    this.valueType = ValueMetaFactory.getIdForValueMeta( typeDesc );
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
