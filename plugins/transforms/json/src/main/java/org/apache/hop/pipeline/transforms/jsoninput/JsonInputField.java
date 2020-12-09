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

package org.apache.hop.pipeline.transforms.jsoninput;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.pipeline.transforms.file.BaseFileField;
import org.w3c.dom.Node;

/**
 * Describes a JsonPath field.
 *
 * @author Samatar
 * @since 20-06-20010
 */
public class JsonInputField extends BaseFileField implements Cloneable {

  @Deprecated
  public static final int TYPE_TRIM_NONE = IValueMeta.TRIM_TYPE_NONE;
  @Deprecated
  public static final int TYPE_TRIM_LEFT = IValueMeta.TRIM_TYPE_LEFT;
  @Deprecated
  public static final int TYPE_TRIM_RIGHT = IValueMeta.TRIM_TYPE_RIGHT;
  @Deprecated
  public static final int TYPE_TRIM_BOTH = IValueMeta.TRIM_TYPE_BOTH;

  @Deprecated
  public static final String[] trimTypeCode = ValueMetaBase.trimTypeCode;

  @Deprecated
  public static final String[] trimTypeDesc = ValueMetaBase.trimTypeDesc;

  @Injection( name = "FIELD_PATH", group = "FIELDS" )
  private String path;

  public JsonInputField( String fieldname ) {
    super();
    setName( fieldname );
  }

  public JsonInputField() {
    this( "" );
  }

  public String getXml() {
    StringBuffer retval = new StringBuffer( 400 );

    retval.append( "      <field>" ).append( Const.CR );
    retval.append( "        " ).append( XmlHandler.addTagValue( "name", getName() ) );
    retval.append( "        " ).append( XmlHandler.addTagValue( "path", getPath() ) );
    retval.append( "        " ).append( XmlHandler.addTagValue( "type", getTypeDesc() ) );
    retval.append( "        " ).append( XmlHandler.addTagValue( "format", getFormat() ) );
    retval.append( "        " ).append( XmlHandler.addTagValue( "currency", getCurrencySymbol() ) );
    retval.append( "        " ).append( XmlHandler.addTagValue( "decimal", getDecimalSymbol() ) );
    retval.append( "        " ).append( XmlHandler.addTagValue( "group", getGroupSymbol() ) );
    retval.append( "        " ).append( XmlHandler.addTagValue( "length", getLength() ) );
    retval.append( "        " ).append( XmlHandler.addTagValue( "precision", getPrecision() ) );
    retval.append( "        " ).append( XmlHandler.addTagValue( "trim_type", getTrimTypeCode() ) );
    retval.append( "        " ).append( XmlHandler.addTagValue( "repeat", isRepeated() ) );

    retval.append( "      </field>" ).append( Const.CR );

    return retval.toString();
  }

  public JsonInputField( Node fnode ) throws HopValueException {
    setName( XmlHandler.getTagValue( fnode, "name" ) );
    setPath( XmlHandler.getTagValue( fnode, "path" ) );
    setType( ValueMetaFactory.getIdForValueMeta( XmlHandler.getTagValue( fnode, "type" ) ) );
    setFormat( XmlHandler.getTagValue( fnode, "format" ) );
    setCurrencySymbol( XmlHandler.getTagValue( fnode, "currency" ) );
    setDecimalSymbol( XmlHandler.getTagValue( fnode, "decimal" ) );
    setGroupSymbol( XmlHandler.getTagValue( fnode, "group" ) );
    setLength( Const.toInt( XmlHandler.getTagValue( fnode, "length" ), -1 ) );
    setPrecision( Const.toInt( XmlHandler.getTagValue( fnode, "precision" ), -1 ) );
    setTrimType( getTrimTypeByCode( XmlHandler.getTagValue( fnode, "trim_type" ) ) );
    setRepeated( !"N".equalsIgnoreCase( XmlHandler.getTagValue( fnode, "repeat" ) ) );
  }

  public IValueMeta toValueMeta( String fieldOriginTransformName, IVariables vspace ) throws HopPluginException {
    int type = getType();
    if ( type == IValueMeta.TYPE_NONE ) {
      type = IValueMeta.TYPE_STRING;
    }
    IValueMeta v =
        ValueMetaFactory.createValueMeta( vspace != null ? vspace.resolve( getName() ) : getName(), type );
    v.setLength( getLength() );
    v.setPrecision( getPrecision() );
    v.setOrigin( fieldOriginTransformName );
    v.setConversionMask( getFormat() );
    v.setDecimalSymbol( getDecimalSymbol() );
    v.setGroupingSymbol( getGroupSymbol() );
    v.setCurrencySymbol( getCurrencySymbol() );
    v.setTrimType( getTrimType() );
    return v;
  }

  @Deprecated
  public static final int getTrimTypeByCode( String tt ) {
    return ValueMetaBase.getTrimTypeByCode( tt );
  }

  @Deprecated
  public static final int getTrimTypeByDesc( String tt ) {
    return ValueMetaBase.getTrimTypeByDesc( tt );
  }

  @Deprecated
  public static final String getTrimTypeCode( int i ) {
    return ValueMetaBase.getTrimTypeCode( i );
  }

  @Deprecated
  public static final String getTrimTypeDesc( int i ) {
    return ValueMetaBase.getTrimTypeDesc( i );
  }

  public JsonInputField clone() {
    JsonInputField retval = (JsonInputField) super.clone();
    return retval;
  }

  public String getPath() {
    return path;
  }

  public void setPath( String value ) {
    this.path = value;
  }

}
