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

package org.apache.hop.pipeline.transforms.formula;

import org.apache.hop.core.Const;
import org.w3c.dom.Node;

import java.util.Objects;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.core.row.value.ValueMetaFactory;

public class FormulaMetaFunction {
    public static final String XML_TAG = "formula";

    private String fieldName;
    private String formula;

    private int valueType;
    private int valueLength;
    private int valuePrecision;

    private String replaceField;

    /**
     * This value will be discovered on runtime and need not to be persisted into xml or rep.
     */
    private transient boolean needDataConversion = false;

    /**
     *
     * @param fieldName
     * @param formula
     * @param valueType
     * @param valueLength
     * @param valuePrecision
     * @param replaceField
     */
    public FormulaMetaFunction( String fieldName, String formula, int valueType, int valueLength,
                                int valuePrecision, String replaceField ) {
        this.fieldName = fieldName;
        this.formula = formula;
        this.valueType = valueType;
        this.valueLength = valueLength;
        this.valuePrecision = valuePrecision;
        this.replaceField = replaceField;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( obj != null && ( obj.getClass().equals( this.getClass() ) ) ) {
            FormulaMetaFunction mf = (FormulaMetaFunction) obj;
            return ( getXML().equals( mf.getXML() ) );
        }

        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash( fieldName, formula, valueType, valueLength, valuePrecision, replaceField );
    }

    @Override
    public Object clone() {
        try {
            FormulaMetaFunction retval = (FormulaMetaFunction) super.clone();
            return retval;
        } catch ( CloneNotSupportedException e ) {
            return null;
        }
    }

    public String getXML() {
        String xml = "";

        xml += "<" + XML_TAG + ">";

        xml += XmlHandler.addTagValue( "field_name", fieldName );
        xml += XmlHandler.addTagValue( "formula_string", formula );
        xml += XmlHandler.addTagValue( "value_type", ValueMetaFactory.getValueMetaName( valueType ) );
        xml += XmlHandler.addTagValue( "value_length", valueLength );
        xml += XmlHandler.addTagValue( "value_precision", valuePrecision );
        xml += XmlHandler.addTagValue( "replace_field", replaceField );

        xml += "</" + XML_TAG + ">";

        return xml;
    }

    public FormulaMetaFunction( Node calcnode ) {
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
     * @param fieldName
     *          The fieldName to set.
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
     * @param valueLength
     *          The valueLength to set.
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
     * @param valuePrecision
     *          The valuePrecision to set.
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
     * @param valueType
     *          The valueType to set.
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
     * @param formula
     *          the formula to set
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
     * @param replaceField
     *          the replaceField to set
     */
    public void setReplaceField( String replaceField ) {
        this.replaceField = replaceField;
    }

    /**
     * @return the needDataConversion
     */
    public boolean isNeedDataConversion() {
        return needDataConversion;
    }

    /**
     * @param needDataConversion the needDataConversion to set
     */
    public void setNeedDataConversion( boolean needDataConversion ) {
        this.needDataConversion = needDataConversion;
    }
}
