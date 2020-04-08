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

package org.apache.hop.pipeline.transforms.validator;

import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionTypeConverter;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.List;

public class Validation implements Cloneable {
  public static final String XML_TAG = "validator_field";
  public static final String XML_TAG_ALLOWED = "allowed_value";

  @Injection( name = "NAME", group = "VALIDATIONS" )
  private String name;
  @Injection( name = "FIELD_NAME", group = "VALIDATIONS" )
  private String fieldName;

  @Injection( name = "MAX_LENGTH", group = "VALIDATIONS" )
  private String maximumLength;
  @Injection( name = "MIN_LENGTH", group = "VALIDATIONS" )
  private String minimumLength;

  @Injection( name = "NULL_ALLOWED", group = "VALIDATIONS" )
  private boolean nullAllowed;
  @Injection( name = "ONLY_NULL_ALLOWED", group = "VALIDATIONS" )
  private boolean onlyNullAllowed;
  @Injection( name = "ONLY_NUMERIC_ALLOWED", group = "VALIDATIONS" )
  private boolean onlyNumericAllowed;

  @Injection( name = "DATA_TYPE", group = "VALIDATIONS", converter = DataTypeConverter.class )
  private int dataType;
  @Injection( name = "DATA_TYPE_VERIFIED", group = "VALIDATIONS" )
  private boolean dataTypeVerified;
  @Injection( name = "CONVERSION_MASK", group = "VALIDATIONS" )
  private String conversionMask;
  @Injection( name = "DECIMAL_SYMBOL", group = "VALIDATIONS" )
  private String decimalSymbol;
  @Injection( name = "GROUPING_SYMBOL", group = "VALIDATIONS" )
  private String groupingSymbol;

  @Injection( name = "MIN_VALUE", group = "VALIDATIONS" )
  private String minimumValue;
  @Injection( name = "MAX_VALUE", group = "VALIDATIONS" )
  private String maximumValue;
  private String[] allowedValues;
  @Injection( name = "SOURCING_VALUES", group = "VALIDATIONS" )
  private boolean sourcingValues;
  @Injection( name = "SOURCING_TRANSFORM_NAME", group = "VALIDATIONS" )
  private String sourcingTransformName;
  private TransformMeta sourcingTransform;
  @Injection( name = "SOURCING_FIELD", group = "VALIDATIONS" )
  private String sourcingField;

  @Injection( name = "START_STRING", group = "VALIDATIONS" )
  private String startString;
  @Injection( name = "START_STRING_NOT_ALLOWED", group = "VALIDATIONS" )
  private String startStringNotAllowed;
  @Injection( name = "END_STRING", group = "VALIDATIONS" )
  private String endString;
  @Injection( name = "END_STRING_NOT_ALLOWED", group = "VALIDATIONS" )
  private String endStringNotAllowed;

  @Injection( name = "REGULAR_EXPRESSION_EXPECTED", group = "VALIDATIONS" )
  private String regularExpression;
  @Injection( name = "REGULAR_EXPRESSION_NOT_ALLOWED", group = "VALIDATIONS" )
  private String regularExpressionNotAllowed;

  @Injection( name = "ERROR_CODE", group = "VALIDATIONS" )
  private String errorCode;
  @Injection( name = "ERROR_CODE_DESCRIPTION", group = "VALIDATIONS" )
  private String errorDescription;

  public Validation() {
    maximumLength = "";
    minimumLength = "";
    nullAllowed = true;
    onlyNullAllowed = false;
    onlyNumericAllowed = false;
  }

  public Validation( String name ) {
    this();
    this.fieldName = name;
  }

  @Override
  public Validation clone() {
    try {
      return (Validation) super.clone();
    } catch ( CloneNotSupportedException e ) {
      return null;
    }
  }

  public boolean equals( Validation validation ) {
    return validation.getName().equalsIgnoreCase( name );
  }

  public String getXml() {
    StringBuilder xml = new StringBuilder();

    xml.append( XmlHandler.openTag( XML_TAG ) );

    xml.append( XmlHandler.addTagValue( "name", fieldName ) );
    xml.append( XmlHandler.addTagValue( "validation_name", name ) );
    xml.append( XmlHandler.addTagValue( "max_length", maximumLength ) );
    xml.append( XmlHandler.addTagValue( "min_length", minimumLength ) );

    xml.append( XmlHandler.addTagValue( "null_allowed", nullAllowed ) );
    xml.append( XmlHandler.addTagValue( "only_null_allowed", onlyNullAllowed ) );
    xml.append( XmlHandler.addTagValue( "only_numeric_allowed", onlyNumericAllowed ) );

    xml.append( XmlHandler.addTagValue( "data_type", ValueMetaFactory.getValueMetaName( dataType ) ) );
    xml.append( XmlHandler.addTagValue( "data_type_verified", dataTypeVerified ) );
    xml.append( XmlHandler.addTagValue( "conversion_mask", conversionMask ) );
    xml.append( XmlHandler.addTagValue( "decimal_symbol", decimalSymbol ) );
    xml.append( XmlHandler.addTagValue( "grouping_symbol", groupingSymbol ) );

    xml.append( XmlHandler.addTagValue( "max_value", maximumValue ) );
    xml.append( XmlHandler.addTagValue( "min_value", minimumValue ) );

    xml.append( XmlHandler.addTagValue( "start_string", startString ) );
    xml.append( XmlHandler.addTagValue( "end_string", endString ) );
    xml.append( XmlHandler.addTagValue( "start_string_not_allowed", startStringNotAllowed ) );
    xml.append( XmlHandler.addTagValue( "end_string_not_allowed", endStringNotAllowed ) );

    xml.append( XmlHandler.addTagValue( "regular_expression", regularExpression ) );
    xml.append( XmlHandler.addTagValue( "regular_expression_not_allowed", regularExpressionNotAllowed ) );

    xml.append( XmlHandler.addTagValue( "error_code", errorCode ) );
    xml.append( XmlHandler.addTagValue( "error_description", errorDescription ) );

    xml.append( XmlHandler.addTagValue( "is_sourcing_values", sourcingValues ) );
    xml.append( XmlHandler.addTagValue( "sourcing_transform", sourcingTransform == null ? sourcingTransformName : sourcingTransform
      .getName() ) );
    xml.append( XmlHandler.addTagValue( "sourcing_field", sourcingField ) );

    xml.append( XmlHandler.openTag( XML_TAG_ALLOWED ) );
    if ( allowedValues != null ) {

      for ( String allowedValue : allowedValues ) {
        xml.append( XmlHandler.addTagValue( "value", allowedValue ) );
      }
    }
    xml.append( XmlHandler.closeTag( XML_TAG_ALLOWED ) );

    xml.append( XmlHandler.closeTag( XML_TAG ) );

    return xml.toString();
  }

  public Validation( Node calcnode ) throws HopXmlException {
    this();

    fieldName = XmlHandler.getTagValue( calcnode, "name" );
    name = XmlHandler.getTagValue( calcnode, "validation_name" );
    if ( Utils.isEmpty( name ) ) {
      name = fieldName; // remain backward compatible
    }

    maximumLength = XmlHandler.getTagValue( calcnode, "max_length" );
    minimumLength = XmlHandler.getTagValue( calcnode, "min_length" );

    nullAllowed = "Y".equalsIgnoreCase( XmlHandler.getTagValue( calcnode, "null_allowed" ) );
    onlyNullAllowed = "Y".equalsIgnoreCase( XmlHandler.getTagValue( calcnode, "only_null_allowed" ) );
    onlyNumericAllowed = "Y".equalsIgnoreCase( XmlHandler.getTagValue( calcnode, "only_numeric_allowed" ) );

    dataType = ValueMetaFactory.getIdForValueMeta( XmlHandler.getTagValue( calcnode, "data_type" ) );
    dataTypeVerified = "Y".equalsIgnoreCase( XmlHandler.getTagValue( calcnode, "data_type_verified" ) );
    conversionMask = XmlHandler.getTagValue( calcnode, "conversion_mask" );
    decimalSymbol = XmlHandler.getTagValue( calcnode, "decimal_symbol" );
    groupingSymbol = XmlHandler.getTagValue( calcnode, "grouping_symbol" );

    minimumValue = XmlHandler.getTagValue( calcnode, "min_value" );
    maximumValue = XmlHandler.getTagValue( calcnode, "max_value" );

    startString = XmlHandler.getTagValue( calcnode, "start_string" );
    endString = XmlHandler.getTagValue( calcnode, "end_string" );
    startStringNotAllowed = XmlHandler.getTagValue( calcnode, "start_string_not_allowed" );
    endStringNotAllowed = XmlHandler.getTagValue( calcnode, "end_string_not_allowed" );

    regularExpression = XmlHandler.getTagValue( calcnode, "regular_expression" );
    regularExpressionNotAllowed = XmlHandler.getTagValue( calcnode, "regular_expression_not_allowed" );

    errorCode = XmlHandler.getTagValue( calcnode, "error_code" );
    errorDescription = XmlHandler.getTagValue( calcnode, "error_description" );

    sourcingValues = "Y".equalsIgnoreCase( XmlHandler.getTagValue( calcnode, "is_sourcing_values" ) );
    sourcingTransformName = XmlHandler.getTagValue( calcnode, "sourcing_transform" );
    sourcingField = XmlHandler.getTagValue( calcnode, "sourcing_field" );

    Node allowedValuesNode = XmlHandler.getSubNode( calcnode, XML_TAG_ALLOWED );
    int nrValues = XmlHandler.countNodes( allowedValuesNode, "value" );
    allowedValues = new String[ nrValues ];
    for ( int i = 0; i < nrValues; i++ ) {
      Node allowedNode = XmlHandler.getSubNodeByNr( allowedValuesNode, "value", i );
      allowedValues[ i ] = XmlHandler.getNodeValue( allowedNode );
    }
  }

  /**
   * @return the field name to validate
   */
  public String getFieldName() {
    return fieldName;
  }

  /**
   * @param fieldName the field name to validate
   */
  public void setFieldName( String fieldName ) {
    this.fieldName = fieldName;
  }

  /**
   * @return the maximumLength
   */
  public String getMaximumLength() {
    return maximumLength;
  }

  /**
   * @param maximumLength the maximumLength to set
   */
  public void setMaximumLength( String maximumLength ) {
    this.maximumLength = maximumLength;
  }

  /**
   * @return the minimumLength
   */
  public String getMinimumLength() {
    return minimumLength;
  }

  /**
   * @param minimumLength the minimumLength to set
   */
  public void setMinimumLength( String minimumLength ) {
    this.minimumLength = minimumLength;
  }

  /**
   * @return the nullAllowed
   */
  public boolean isNullAllowed() {
    return nullAllowed;
  }

  /**
   * @param nullAllowed the nullAllowed to set
   */
  public void setNullAllowed( boolean nullAllowed ) {
    this.nullAllowed = nullAllowed;
  }

  /**
   * @return the dataType
   */
  public int getDataType() {
    return dataType;
  }

  /**
   * @param dataType the dataType to set
   */
  public void setDataType( int dataType ) {
    this.dataType = dataType;
  }

  /**
   * @return the conversionMask
   */
  public String getConversionMask() {
    return conversionMask;
  }

  /**
   * @param conversionMask the conversionMask to set
   */
  public void setConversionMask( String conversionMask ) {
    this.conversionMask = conversionMask;
  }

  /**
   * @return the decimalSymbol
   */
  public String getDecimalSymbol() {
    return decimalSymbol;
  }

  /**
   * @param decimalSymbol the decimalSymbol to set
   */
  public void setDecimalSymbol( String decimalSymbol ) {
    this.decimalSymbol = decimalSymbol;
  }

  /**
   * @return the groupingSymbol
   */
  public String getGroupingSymbol() {
    return groupingSymbol;
  }

  /**
   * @param groupingSymbol the groupingSymbol to set
   */
  public void setGroupingSymbol( String groupingSymbol ) {
    this.groupingSymbol = groupingSymbol;
  }

  /**
   * @return the minimumValue
   */
  public String getMinimumValue() {
    return minimumValue;
  }

  /**
   * @param minimumValue the minimumValue to set
   */
  public void setMinimumValue( String minimumValue ) {
    this.minimumValue = minimumValue;
  }

  /**
   * @return the maximumValue
   */
  public String getMaximumValue() {
    return maximumValue;
  }

  /**
   * @param maximumValue the maximumValue to set
   */
  public void setMaximumValue( String maximumValue ) {
    this.maximumValue = maximumValue;
  }

  /**
   * @return the allowedValues
   */
  public String[] getAllowedValues() {
    return allowedValues;
  }

  /**
   * @param allowedValues the allowedValues to set
   */
  public void setAllowedValues( String[] allowedValues ) {
    this.allowedValues = allowedValues;
  }

  /**
   * @return the dataTypeVerified
   */
  public boolean isDataTypeVerified() {
    return dataTypeVerified;
  }

  /**
   * @param dataTypeVerified the dataTypeVerified to set
   */
  public void setDataTypeVerified( boolean dataTypeVerified ) {
    this.dataTypeVerified = dataTypeVerified;
  }

  /**
   * @return the errorCode
   */
  public String getErrorCode() {
    return errorCode;
  }

  /**
   * @param errorCode the errorCode to set
   */
  public void setErrorCode( String errorCode ) {
    this.errorCode = errorCode;
  }

  /**
   * @return the errorDescription
   */
  public String getErrorDescription() {
    return errorDescription;
  }

  /**
   * @param errorDescription the errorDescription to set
   */
  public void setErrorDescription( String errorDescription ) {
    this.errorDescription = errorDescription;
  }

  /**
   * @return true if only numeric values are allowed: A numeric data type, a date or a String containing digits only
   */
  public boolean isOnlyNumericAllowed() {
    return onlyNumericAllowed;
  }

  /**
   * @return the startString
   */
  public String getStartString() {
    return startString;
  }

  /**
   * @param startString the startString to set
   */
  public void setStartString( String startString ) {
    this.startString = startString;
  }

  /**
   * @return the startStringNotAllowed
   */
  public String getStartStringNotAllowed() {
    return startStringNotAllowed;
  }

  /**
   * @param startStringNotAllowed the startStringNotAllowed to set
   */
  public void setStartStringNotAllowed( String startStringNotAllowed ) {
    this.startStringNotAllowed = startStringNotAllowed;
  }

  /**
   * @return the endString
   */
  public String getEndString() {
    return endString;
  }

  /**
   * @param endString the endString to set
   */
  public void setEndString( String endString ) {
    this.endString = endString;
  }

  /**
   * @return the endStringNotAllowed
   */
  public String getEndStringNotAllowed() {
    return endStringNotAllowed;
  }

  /**
   * @param endStringNotAllowed the endStringNotAllowed to set
   */
  public void setEndStringNotAllowed( String endStringNotAllowed ) {
    this.endStringNotAllowed = endStringNotAllowed;
  }

  /**
   * @param onlyNumericAllowed the onlyNumericAllowed to set
   */
  public void setOnlyNumericAllowed( boolean onlyNumericAllowed ) {
    this.onlyNumericAllowed = onlyNumericAllowed;
  }

  /**
   * @return the regularExpression
   */
  public String getRegularExpression() {
    return regularExpression;
  }

  /**
   * @param regularExpression the regularExpression to set
   */
  public void setRegularExpression( String regularExpression ) {
    this.regularExpression = regularExpression;
  }

  /**
   * @return the name of this validation
   */
  public String getName() {
    return name;
  }

  /**
   * @param name the new name for this validation
   */
  public void setName( String name ) {
    this.name = name;
  }

  /**
   * @return the regularExpressionNotAllowed
   */
  public String getRegularExpressionNotAllowed() {
    return regularExpressionNotAllowed;
  }

  /**
   * @param regularExpressionNotAllowed the regularExpressionNotAllowed to set
   */
  public void setRegularExpressionNotAllowed( String regularExpressionNotAllowed ) {
    this.regularExpressionNotAllowed = regularExpressionNotAllowed;
  }

  /**
   * Find a validation by name in a list of validations
   *
   * @param validations The list to search
   * @param name        the name to search for
   * @return the validation if one matches or null if none is found.
   */
  public static Validation findValidation( List<Validation> validations, String name ) {
    for ( Validation validation : validations ) {
      if ( validation.getName().equalsIgnoreCase( name ) ) {
        return validation;
      }
    }
    return null;
  }

  /**
   * @return the onlyNullAllowed
   */
  public boolean isOnlyNullAllowed() {
    return onlyNullAllowed;
  }

  /**
   * @param onlyNullAllowed the onlyNullAllowed to set
   */
  public void setOnlyNullAllowed( boolean onlyNullAllowed ) {
    this.onlyNullAllowed = onlyNullAllowed;
  }

  /**
   * @return the sourcingValues
   */
  public boolean isSourcingValues() {
    return sourcingValues;
  }

  /**
   * @param sourcingValues the sourcingValues to set
   */
  public void setSourcingValues( boolean sourcingValues ) {
    this.sourcingValues = sourcingValues;
  }

  /**
   * @return the sourcingField
   */
  public String getSourcingField() {
    return sourcingField;
  }

  /**
   * @param sourcingField the sourcingField to set
   */
  public void setSourcingField( String sourcingField ) {
    this.sourcingField = sourcingField;
  }

  /**
   * @return the sourcingTransformName
   */
  public String getSourcingTransformName() {
    return sourcingTransformName;
  }

  /**
   * @param sourcingTransformName the sourcingTransformName to set
   */
  public void setSourcingTransformName( String sourcingTransformName ) {
    this.sourcingTransformName = sourcingTransformName;
  }

  /**
   * @return the sourcingTransform
   */
  public TransformMeta getSourcingTransform() {
    return sourcingTransform;
  }

  /**
   * @param sourcingTransform the sourcingTransform to set
   */
  public void setSourcingTransform( TransformMeta sourcingTransform ) {
    this.sourcingTransform = sourcingTransform;
  }

  public static class DataTypeConverter extends InjectionTypeConverter {
    @Override
    public int string2intPrimitive( String v ) throws HopValueException {
      return ValueMetaFactory.getIdForValueMeta( v );
    }
  }
}
