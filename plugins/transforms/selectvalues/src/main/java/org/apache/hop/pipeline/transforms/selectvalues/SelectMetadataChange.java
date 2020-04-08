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

package org.apache.hop.pipeline.transforms.selectvalues;

import org.apache.hop.core.Const;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.core.xml.IXml;
import org.apache.hop.pipeline.transform.ITransformAttributes;
import org.w3c.dom.Node;

public class SelectMetadataChange implements Cloneable, IXml {

  public static final String XML_TAG = "meta";

  // META-DATA mode
  /**
   * Fields of which we want to change the meta data
   */
  @Injection( name = "META_NAME", group = "METAS" )
  private String name;
  /**
   * Meta: new name of field
   */
  @Injection( name = "META_RENAME", group = "METAS" )
  private String rename;
  /**
   * Meta: new Value type for this field or TYPE_NONE if no change needed!
   */
  private int type;
  /**
   * Meta: new length of field
   */
  @Injection( name = "META_LENGTH", group = "METAS" )
  private int length = -1;
  /**
   * Meta: new precision of field (for numbers)
   */
  @Injection( name = "META_PRECISION", group = "METAS" )
  private int precision = -1;
  /**
   * Meta: the storage type, NORMAL or BINARY_STRING
   */
  private int storageType;
  /**
   * The conversion metadata if any conversion needs to take place
   */
  @Injection( name = "META_CONVERSION_MASK", group = "METAS" )
  private String conversionMask;
  /**
   * Treat the date format as lenient
   */
  @Injection( name = "META_DATE_FORMAT_LENIENT", group = "METAS" )
  private boolean dateFormatLenient;

  /**
   * This is the locale to use for date parsing
   */
  @Injection( name = "META_DATE_FORMAT_LOCALE", group = "METAS" )
  private String dateFormatLocale;

  /**
   * This is the time zone to use for date parsing
   */
  @Injection( name = "META_DATE_FORMAT_TIMEZONE", group = "METAS" )
  private String dateFormatTimeZone;

  /**
   * Treat string to number format as lenient
   */
  @Injection( name = "META_LENIENT_STRING_TO_NUMBER", group = "METAS" )
  private boolean lenientStringToNumber;
  /**
   * The decimal symbol for number conversions
   */
  @Injection( name = "META_DECIMAL", group = "METAS" )
  private String decimalSymbol;
  /**
   * The grouping symbol for number conversions
   */
  @Injection( name = "META_GROUPING", group = "METAS" )
  private String groupingSymbol;
  /**
   * The currency symbol for number conversions
   */
  @Injection( name = "META_CURRENCY", group = "METAS" )
  private String currencySymbol;
  /**
   * The encoding to use when decoding binary data to Strings
   */
  @Injection( name = "META_ENCODING", group = "METAS" )
  private String encoding;

  private ITransformAttributes attributesInterface;

  public SelectMetadataChange( ITransformAttributes attributesInterface ) {
    this.attributesInterface = attributesInterface;
    storageType = -1; // storage type is not used by default!
  }

  /**
   * @Deprecated This method is left here for external code that may be using it. It may be removed in the future.
   * @see #SelectMetadataChange(ITransformAttributes, String, String, int, int, int, int, String, boolean, String,
   * String, String)
   */
  public SelectMetadataChange( ITransformAttributes attributesInterface, String name, String rename, int type,
                               int length, int precision, int storageType, String conversionMask, String decimalSymbol,
                               String groupingSymbol, String currencySymbol ) {
    this(
      attributesInterface, name, rename, type, length, precision, storageType, conversionMask, false, null,
      null, false, decimalSymbol, groupingSymbol, currencySymbol );
  }

  /**
   * @param attributesInterface
   * @param name
   * @param rename
   * @param type
   * @param length
   * @param precision
   * @param storageType
   * @param conversionMask
   * @param dateFormatLenient
   * @param dateFormatLocale
   * @param dateFormatTimeZone
   * @param lenientStringToNumber
   * @param decimalSymbol
   * @param groupingSymbol
   * @param currencySymbol
   */
  public SelectMetadataChange( ITransformAttributes attributesInterface, String name, String rename, int type,
                               int length, int precision, int storageType, String conversionMask, boolean dateFormatLenient,
                               String dateFormatLocale, String dateFormatTimeZone, boolean lenientStringToNumber, String decimalSymbol,
                               String groupingSymbol, String currencySymbol ) {
    this( attributesInterface );
    this.name = name;
    this.rename = rename;
    this.type = type;
    this.length = length;
    this.precision = precision;
    this.storageType = storageType;
    this.conversionMask = conversionMask;
    this.dateFormatLenient = dateFormatLenient;
    this.dateFormatLocale = dateFormatLocale;
    this.dateFormatTimeZone = dateFormatTimeZone;
    this.lenientStringToNumber = lenientStringToNumber;
    this.decimalSymbol = decimalSymbol;
    this.groupingSymbol = groupingSymbol;
    this.currencySymbol = currencySymbol;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder();
    retval.append( "      " ).append( XmlHandler.openTag( XML_TAG ) );
    retval.append( "        " ).append(
      XmlHandler.addTagValue( attributesInterface.getXmlCode( "META_NAME" ), name ) );
    retval.append( "        " ).append(
      XmlHandler.addTagValue( attributesInterface.getXmlCode( "META_RENAME" ), rename ) );
    retval.append( "        " ).append(
      XmlHandler.addTagValue( attributesInterface.getXmlCode( "META_TYPE" ),
        ValueMetaFactory.getValueMetaName( type ) ) );
    retval.append( "        " ).append(
      XmlHandler.addTagValue( attributesInterface.getXmlCode( "META_LENGTH" ), length ) );
    retval.append( "        " ).append(
      XmlHandler.addTagValue( attributesInterface.getXmlCode( "META_PRECISION" ), precision ) );
    retval.append( "        " ).append(
      XmlHandler.addTagValue( attributesInterface.getXmlCode( "META_CONVERSION_MASK" ), conversionMask ) );
    retval.append( "        " ).append(
      XmlHandler.addTagValue( attributesInterface.getXmlCode( "META_DATE_FORMAT_LENIENT" ), Boolean
        .toString( dateFormatLenient ) ) );
    retval.append( "        " ).append(
      XmlHandler.addTagValue( attributesInterface.getXmlCode( "META_DATE_FORMAT_LOCALE" ), dateFormatLocale ) );
    retval
      .append( "        " ).append(
      XmlHandler.addTagValue(
        attributesInterface.getXmlCode( "META_DATE_FORMAT_TIMEZONE" ), dateFormatTimeZone ) );
    retval.append( "        " ).append(
      XmlHandler.addTagValue( attributesInterface.getXmlCode( "META_LENIENT_STRING_TO_NUMBER" ), Boolean
        .toString( lenientStringToNumber ) ) );
    retval.append( "        " ).append(
      XmlHandler.addTagValue( attributesInterface.getXmlCode( "META_ENCODING" ), encoding ) );
    retval.append( "        " ).append(
      XmlHandler.addTagValue( attributesInterface.getXmlCode( "META_DECIMAL" ), decimalSymbol ) );
    retval.append( "        " ).append(
      XmlHandler.addTagValue( attributesInterface.getXmlCode( "META_GROUPING" ), groupingSymbol ) );
    retval.append( "        " ).append(
      XmlHandler.addTagValue( attributesInterface.getXmlCode( "META_CURRENCY" ), currencySymbol ) );
    retval.append( "        " ).append(
      XmlHandler.addTagValue( attributesInterface.getXmlCode( "META_STORAGE_TYPE" ), ValueMetaBase
        .getStorageTypeCode( storageType ) ) );
    retval.append( "      " ).append( XmlHandler.closeTag( XML_TAG ) );
    return retval.toString();
  }

  public void loadXml( Node metaNode ) {
    name = XmlHandler.getTagValue( metaNode, attributesInterface.getXmlCode( "META_NAME" ) );
    rename = XmlHandler.getTagValue( metaNode, attributesInterface.getXmlCode( "META_RENAME" ) );
    type = ValueMetaFactory.getIdForValueMeta(
      XmlHandler.getTagValue( metaNode, attributesInterface.getXmlCode( "META_TYPE" ) ) );
    length = Const.toInt( XmlHandler.getTagValue( metaNode, attributesInterface.getXmlCode( "META_LENGTH" ) ), -2 );
    precision =
      Const.toInt( XmlHandler.getTagValue( metaNode, attributesInterface.getXmlCode( "META_PRECISION" ) ), -2 );
    storageType =
      ValueMetaBase.getStorageType( XmlHandler.getTagValue( metaNode, attributesInterface
        .getXmlCode( "META_STORAGE_TYPE" ) ) );
    conversionMask = XmlHandler.getTagValue( metaNode, attributesInterface.getXmlCode( "META_CONVERSION_MASK" ) );
    dateFormatLenient =
      Boolean.parseBoolean( XmlHandler.getTagValue( metaNode, attributesInterface
        .getXmlCode( "META_DATE_FORMAT_LENIENT" ) ) );
    dateFormatLocale =
      XmlHandler.getTagValue( metaNode, attributesInterface.getXmlCode( "META_DATE_FORMAT_LOCALE" ) );
    dateFormatTimeZone =
      XmlHandler.getTagValue( metaNode, attributesInterface.getXmlCode( "META_DATE_FORMAT_TIMEZONE" ) );
    encoding = XmlHandler.getTagValue( metaNode, attributesInterface.getXmlCode( "META_ENCODING" ) );
    lenientStringToNumber =
      Boolean.parseBoolean( XmlHandler.getTagValue( metaNode, attributesInterface
        .getXmlCode( "META_LENIENT_STRING_TO_NUMBER" ) ) );
    encoding = XmlHandler.getTagValue( metaNode, attributesInterface.getXmlCode( "META_ENCODING" ) );
    decimalSymbol = XmlHandler.getTagValue( metaNode, attributesInterface.getXmlCode( "META_DECIMAL" ) );
    groupingSymbol = XmlHandler.getTagValue( metaNode, attributesInterface.getXmlCode( "META_GROUPING" ) );
    currencySymbol = XmlHandler.getTagValue( metaNode, attributesInterface.getXmlCode( "META_CURRENCY" ) );
  }

  public SelectMetadataChange clone() {
    try {
      return (SelectMetadataChange) super.clone();
    } catch ( CloneNotSupportedException e ) {
      return null;
    }
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name the name to set
   */
  public void setName( String name ) {
    this.name = name;
  }

  /**
   * @return the rename
   */
  public String getRename() {
    return rename;
  }

  /**
   * @param rename the rename to set
   */
  public void setRename( String rename ) {
    this.rename = rename;
  }

  /**
   * @return the type
   */
  public int getType() {
    return type;
  }

  /**
   * @param type the type to set
   */
  public void setType( int type ) {
    this.type = type;
  }

  @Injection( name = "META_TYPE", group = "METAS" )
  public void setType( String value ) {
    type = ValueMetaFactory.getIdForValueMeta( value );
  }

  /**
   * @return the length
   */
  public int getLength() {
    return length;
  }

  /**
   * @param length the length to set
   */
  public void setLength( int length ) {
    this.length = length;
  }

  /**
   * @return the precision
   */
  public int getPrecision() {
    return precision;
  }

  /**
   * @param precision the precision to set
   */
  public void setPrecision( int precision ) {
    this.precision = precision;
  }

  /**
   * @return the storageType
   */
  public int getStorageType() {
    return storageType;
  }

  /**
   * @param storageType the storageType to set
   */
  public void setStorageType( int storageType ) {
    this.storageType = storageType;
  }

  @Injection( name = "META_STORAGE_TYPE", group = "METAS" )
  public void setStorageType( String storageType ) {
    this.storageType = ValueMetaBase.getStorageType( storageType );
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
   * @return whether date conversion from string is lenient or not
   */
  public boolean isDateFormatLenient() {
    return dateFormatLenient;
  }

  /**
   * @param dateFormatLenient whether date conversion from string is lenient or not
   */
  public void setDateFormatLenient( boolean dateFormatLenient ) {
    this.dateFormatLenient = dateFormatLenient;
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
   * @return the currencySymbol
   */
  public String getCurrencySymbol() {
    return currencySymbol;
  }

  /**
   * @param currencySymbol the currencySymbol to set
   */
  public void setCurrencySymbol( String currencySymbol ) {
    this.currencySymbol = currencySymbol;
  }

  /**
   * @return the encoding to use when decoding binary data to strings
   */
  public String getEncoding() {
    return encoding;
  }

  /**
   * @param encoding the encoding to use when decoding binary data to strings
   */
  public void setEncoding( String encoding ) {
    this.encoding = encoding;
  }

  /**
   * @return the lenientStringToNumber
   */
  public boolean isLenientStringToNumber() {
    return lenientStringToNumber;
  }

  /**
   * @param lenientStringToNumber the lenientStringToNumber to set
   */
  public void setLenientStringToNumber( boolean lenientStringToNumber ) {
    this.lenientStringToNumber = lenientStringToNumber;
  }

  /**
   * @return the dateFormatLocale
   */
  public String getDateFormatLocale() {
    return dateFormatLocale;
  }

  /**
   * @param dateFormatLocale the dateFormatLocale to set
   */
  public void setDateFormatLocale( String dateFormatLocale ) {
    this.dateFormatLocale = dateFormatLocale;
  }

  /**
   * @return the dateFormatTimeZone
   */
  public String getDateFormatTimeZone() {
    return dateFormatTimeZone;
  }

  /**
   * @param dateFormatTimeZone the dateFormatTimeZone to set
   */
  public void setDateFormatTimeZone( String dateFormatTimeZone ) {
    this.dateFormatTimeZone = dateFormatTimeZone;
  }
}
