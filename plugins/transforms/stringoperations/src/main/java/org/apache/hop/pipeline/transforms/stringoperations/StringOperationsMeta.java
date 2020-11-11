/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.stringoperations;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.List;

@Transform(
        id = "StringOperations",
        image = "stringoperations.svg",
        i18nPackageName = "org.apache.hop.pipeline.transforms.StringOperations",
        name = "BaseTransform.TypeLongDesc.StringOperations",
        description = "BaseTransform.TypeTooltipDesc.StringOperations",
        categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Flow",
        documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/stringoperations.html"
)
public class StringOperationsMeta extends BaseTransformMeta implements ITransformMeta<StringOperations,StringOperationsData> {

  private static final Class<?> PKG = StringOperationsMeta.class; // Needed by Translator

  /**
   * which field in input stream to compare with?
   */
  private String[] fieldInStream;

  /**
   * output field
   */
  private String[] fieldOutStream;

  /**
   * Trim type
   */
  private int[] trimType;

  /**
   * Lower/Upper type
   */
  private int[] lowerUpper;

  /**
   * InitCap
   */
  private int[] initCap;

  private int[] maskXML;

  private int[] digits;

  private int[] remove_special_characters;

  /**
   * padding type
   */
  private int[] paddingType;

  /**
   * Pad length
   */
  private String[] padLen;

  private String[] padChar;

  /**
   * The trim type codes
   */
  public static final String[] trimTypeCode = { "none", "left", "right", "both" };

  public static final int TRIM_NONE = 0;

  public static final int TRIM_LEFT = 1;

  public static final int TRIM_RIGHT = 2;

  public static final int TRIM_BOTH = 3;

  /**
   * The trim description
   */
  public static final String[] trimTypeDesc = {
    BaseMessages.getString( PKG, "StringOperationsMeta.TrimType.None" ),
    BaseMessages.getString( PKG, "StringOperationsMeta.TrimType.Left" ),
    BaseMessages.getString( PKG, "StringOperationsMeta.TrimType.Right" ),
    BaseMessages.getString( PKG, "StringOperationsMeta.TrimType.Both" ) };

  /**
   * The lower upper codes
   */
  public static final String[] lowerUpperCode = { "none", "lower", "upper" };

  public static final int LOWER_UPPER_NONE = 0;

  public static final int LOWER_UPPER_LOWER = 1;

  public static final int LOWER_UPPER_UPPER = 2;

  /**
   * The lower upper description
   */
  public static final String[] lowerUpperDesc = {
    BaseMessages.getString( PKG, "StringOperationsMeta.LowerUpper.None" ),
    BaseMessages.getString( PKG, "StringOperationsMeta.LowerUpper.Lower" ),
    BaseMessages.getString( PKG, "StringOperationsMeta.LowerUpper.Upper" ) };

  public static final String[] initCapDesc = new String[] {
    BaseMessages.getString( PKG, "System.Combo.No" ), BaseMessages.getString( PKG, "System.Combo.Yes" ) };

  public static final String[] initCapCode = { "no", "yes" };

  public static final int INIT_CAP_NO = 0;

  public static final int INIT_CAP_YES = 1;

  // digits
  public static final String[] digitsCode = { "none", "digits_only", "remove_digits" };

  public static final int DIGITS_NONE = 0;

  public static final int DIGITS_ONLY = 1;

  public static final int DIGITS_REMOVE = 2;

  public static final String[] digitsDesc = new String[] {
    BaseMessages.getString( PKG, "StringOperationsMeta.Digits.None" ),
    BaseMessages.getString( PKG, "StringOperationsMeta.Digits.Only" ),
    BaseMessages.getString( PKG, "StringOperationsMeta.Digits.Remove" ) };

  // mask XML

  public static final String[] maskXMLDesc = new String[] {
    BaseMessages.getString( PKG, "StringOperationsMeta.MaskXML.None" ),
    BaseMessages.getString( PKG, "StringOperationsMeta.MaskXML.EscapeXML" ),
    BaseMessages.getString( PKG, "StringOperationsMeta.MaskXML.CDATA" ),
    BaseMessages.getString( PKG, "StringOperationsMeta.MaskXML.UnEscapeXML" ),
    BaseMessages.getString( PKG, "StringOperationsMeta.MaskXML.EscapeSQL" ),
    BaseMessages.getString( PKG, "StringOperationsMeta.MaskXML.EscapeHTML" ),
    BaseMessages.getString( PKG, "StringOperationsMeta.MaskXML.UnEscapeHTML" ), };

  public static final String[] maskXMLCode = {
    "none", "escapexml", "cdata", "unescapexml", "escapesql", "escapehtml", "unescapehtml" };

  public static final int MASK_NONE = 0;
  public static final int MASK_ESCAPE_XML = 1;
  public static final int MASK_CDATA = 2;
  public static final int MASK_UNESCAPE_XML = 3;
  public static final int MASK_ESCAPE_SQL = 4;
  public static final int MASK_ESCAPE_HTML = 5;
  public static final int MASK_UNESCAPE_HTML = 6;

  // remove special characters
  public static final String[] removeSpecialCharactersCode = { "none", "cr", "lf", "crlf", "tab", "espace" };

  public static final int REMOVE_SPECIAL_CHARACTERS_NONE = 0;

  public static final int REMOVE_SPECIAL_CHARACTERS_CR = 1;

  public static final int REMOVE_SPECIAL_CHARACTERS_LF = 2;

  public static final int REMOVE_SPECIAL_CHARACTERS_CRLF = 3;

  public static final int REMOVE_SPECIAL_CHARACTERS_TAB = 4;

  public static final int REMOVE_SPECIAL_CHARACTERS_ESPACE = 5;

  public static final String[] removeSpecialCharactersDesc = new String[] {
    BaseMessages.getString( PKG, "StringOperationsMeta.RemoveSpecialCharacters.None" ),
    BaseMessages.getString( PKG, "StringOperationsMeta.RemoveSpecialCharacters.CR" ),
    BaseMessages.getString( PKG, "StringOperationsMeta.RemoveSpecialCharacters.LF" ),
    BaseMessages.getString( PKG, "StringOperationsMeta.RemoveSpecialCharacters.CRLF" ),
    BaseMessages.getString( PKG, "StringOperationsMeta.RemoveSpecialCharacters.TAB" ),
    BaseMessages.getString( PKG, "StringOperationsMeta.RemoveSpecialCharacters.Space" ) };

  /**
   * The padding description
   */
  public static final String[] paddingDesc = {
    BaseMessages.getString( PKG, "StringOperationsMeta.Padding.None" ),
    BaseMessages.getString( PKG, "StringOperationsMeta.Padding.Left" ),
    BaseMessages.getString( PKG, "StringOperationsMeta.Padding.Right" ) };

  public static final String[] paddingCode = { "none", "left", "right" };

  public static final int PADDING_NONE = 0;

  public static final int PADDING_LEFT = 1;

  public static final int PADDING_RIGHT = 2;

  public StringOperationsMeta() {
    super(); // allocate BaseTransformMeta
  }

  /**
   * @return Returns the fieldInStream.
   */
  public String[] getFieldInStream() {
    return fieldInStream;
  }

  /**
   * @param keyStream The fieldInStream to set.
   */
  public void setFieldInStream( String[] keyStream ) {
    this.fieldInStream = keyStream;
  }

  /**
   * @return Returns the fieldOutStream.
   */
  public String[] getFieldOutStream() {
    return fieldOutStream;
  }

  /**
   * @param keyStream The fieldOutStream to set.
   */
  public void setFieldOutStream( String[] keyStream ) {
    this.fieldOutStream = keyStream;
  }

  public String[] getPadLen() {
    return padLen;
  }

  public void setPadLen( String[] value ) {
    padLen = value;
  }

  public String[] getPadChar() {
    return padChar;
  }

  public void setPadChar( String[] value ) {
    padChar = value;
  }

  public int[] getTrimType() {
    return trimType;
  }

  public void setTrimType( int[] trimType ) {
    this.trimType = trimType;
  }

  public int[] getLowerUpper() {
    return lowerUpper;
  }

  public void setLowerUpper( int[] lowerUpper ) {
    this.lowerUpper = lowerUpper;
  }

  public int[] getInitCap() {
    return initCap;
  }

  public void setInitCap( int[] value ) {
    initCap = value;
  }

  public int[] getMaskXML() {
    return maskXML;
  }

  public void setMaskXML( int[] value ) {
    maskXML = value;
  }

  public int[] getDigits() {
    return digits;
  }

  public void setDigits( int[] value ) {
    digits = value;
  }

  public int[] getRemoveSpecialCharacters() {
    return remove_special_characters;
  }

  public void setRemoveSpecialCharacters( int[] value ) {
    remove_special_characters = value;
  }

  public int[] getPaddingType() {
    return paddingType;
  }

  public void setPaddingType( int[] value ) {
    paddingType = value;
  }

  @Override
  public void loadXml( Node transformNode, IHopMetadataProvider metadataProvider ) throws HopXmlException {
    readData( transformNode );
  }

  public void allocate( int nrkeys ) {
    fieldInStream = new String[ nrkeys ];
    fieldOutStream = new String[ nrkeys ];
    trimType = new int[ nrkeys ];
    lowerUpper = new int[ nrkeys ];
    paddingType = new int[ nrkeys ];
    padChar = new String[ nrkeys ];
    padLen = new String[ nrkeys ];
    initCap = new int[ nrkeys ];
    maskXML = new int[ nrkeys ];
    digits = new int[ nrkeys ];
    remove_special_characters = new int[ nrkeys ];
  }

  @Override
  public Object clone() {
    StringOperationsMeta retval = (StringOperationsMeta) super.clone();
    int nrkeys = fieldInStream.length;

    retval.allocate( nrkeys );
    System.arraycopy( fieldInStream, 0, retval.fieldInStream, 0, nrkeys );
    System.arraycopy( fieldOutStream, 0, retval.fieldOutStream, 0, nrkeys );
    System.arraycopy( trimType, 0, retval.trimType, 0, nrkeys );
    System.arraycopy( lowerUpper, 0, retval.lowerUpper, 0, nrkeys );
    System.arraycopy( paddingType, 0, retval.paddingType, 0, nrkeys );
    System.arraycopy( padChar, 0, retval.padChar, 0, nrkeys );
    System.arraycopy( padLen, 0, retval.padLen, 0, nrkeys );
    System.arraycopy( initCap, 0, retval.initCap, 0, nrkeys );
    System.arraycopy( maskXML, 0, retval.maskXML, 0, nrkeys );
    System.arraycopy( digits, 0, retval.digits, 0, nrkeys );
    System.arraycopy( remove_special_characters, 0, retval.remove_special_characters, 0, nrkeys );

    return retval;
  }

  private void readData( Node transformNode ) throws HopXmlException {
    try {
      int nrkeys;

      Node lookup = XmlHandler.getSubNode( transformNode, "fields" );
      nrkeys = XmlHandler.countNodes( lookup, "field" );

      allocate( nrkeys );

      for ( int i = 0; i < nrkeys; i++ ) {
        Node fnode = XmlHandler.getSubNodeByNr( lookup, "field", i );

        fieldInStream[ i ] = Const.NVL( XmlHandler.getTagValue( fnode, "in_stream_name" ), "" );
        fieldOutStream[ i ] = Const.NVL( XmlHandler.getTagValue( fnode, "out_stream_name" ), "" );

        trimType[ i ] = getTrimTypeByCode( Const.NVL( XmlHandler.getTagValue( fnode, "trim_type" ), "" ) );
        lowerUpper[ i ] = getLowerUpperByCode( Const.NVL( XmlHandler.getTagValue( fnode, "lower_upper" ), "" ) );
        paddingType[ i ] = getPaddingByCode( Const.NVL( XmlHandler.getTagValue( fnode, "padding_type" ), "" ) );
        padChar[ i ] = Const.NVL( XmlHandler.getTagValue( fnode, "pad_char" ), "" );
        padLen[ i ] = Const.NVL( XmlHandler.getTagValue( fnode, "pad_len" ), "" );
        initCap[ i ] = getInitCapByCode( Const.NVL( XmlHandler.getTagValue( fnode, "init_cap" ), "" ) );
        maskXML[ i ] = getMaskXMLByCode( Const.NVL( XmlHandler.getTagValue( fnode, "mask_xml" ), "" ) );
        digits[ i ] = getDigitsByCode( Const.NVL( XmlHandler.getTagValue( fnode, "digits" ), "" ) );
        remove_special_characters[ i ] =
          getRemoveSpecialCharactersByCode( Const.NVL( XmlHandler.getTagValue(
            fnode, "remove_special_characters" ), "" ) );

      }
    } catch ( Exception e ) {
      throw new HopXmlException( BaseMessages.getString(
        PKG, "StringOperationsMeta.Exception.UnableToReadTransformMetaFromXML" ), e );
    }
  }

  @Override
  public void setDefault() {
    fieldInStream = null;
    fieldOutStream = null;

    int nrkeys = 0;

    allocate( nrkeys );
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder( 500 );

    retval.append( "    <fields>" ).append( Const.CR );

    for ( int i = 0; i < fieldInStream.length; i++ ) {
      retval.append( "      <field>" ).append( Const.CR );
      retval.append( "        " ).append( XmlHandler.addTagValue( "in_stream_name", fieldInStream[ i ] ) );
      retval.append( "        " ).append( XmlHandler.addTagValue( "out_stream_name", fieldOutStream[ i ] ) );

      retval.append( "        " ).append( XmlHandler.addTagValue( "trim_type", getTrimTypeCode( trimType[ i ] ) ) );
      retval.append( "        " ).append(
        XmlHandler.addTagValue( "lower_upper", getLowerUpperCode( lowerUpper[ i ] ) ) );
      retval.append( "        " ).append(
        XmlHandler.addTagValue( "padding_type", getPaddingCode( paddingType[ i ] ) ) );
      retval.append( "        " ).append( XmlHandler.addTagValue( "pad_char", padChar[ i ] ) );
      retval.append( "        " ).append( XmlHandler.addTagValue( "pad_len", padLen[ i ] ) );
      retval.append( "        " ).append( XmlHandler.addTagValue( "init_cap", getInitCapCode( initCap[ i ] ) ) );
      retval.append( "        " ).append( XmlHandler.addTagValue( "mask_xml", getMaskXMLCode( maskXML[ i ] ) ) );
      retval.append( "        " ).append( XmlHandler.addTagValue( "digits", getDigitsCode( digits[ i ] ) ) );
      retval.append( "        " ).append(
        XmlHandler.addTagValue(
          "remove_special_characters", getRemoveSpecialCharactersCode( remove_special_characters[ i ] ) ) );

      retval.append( "      </field>" ).append( Const.CR );
    }

    retval.append( "    </fields>" ).append( Const.CR );

    return retval.toString();
  }

  @Override
  public void getFields( IRowMeta inputRowMeta, String name, IRowMeta[] info, TransformMeta nextTransform,
                         IVariables variables, IHopMetadataProvider metadataProvider ) throws HopTransformException {
    // Add new field?
    for ( int i = 0; i < fieldOutStream.length; i++ ) {
      IValueMeta v;
      String outputField = variables.environmentSubstitute( fieldOutStream[ i ] );
      if ( !Utils.isEmpty( outputField ) ) {
        // Add a new field
        v = new ValueMetaString( outputField );
        v.setLength( 100, -1 );
        v.setOrigin( name );
        inputRowMeta.addValueMeta( v );
      } else {
        v = inputRowMeta.searchValueMeta( fieldInStream[ i ] );
        if ( v == null ) {
          continue;
        }
        v.setStorageType( IValueMeta.STORAGE_TYPE_NORMAL );
        int paddingType = getPaddingType()[ i ];
        if ( paddingType == PADDING_LEFT || paddingType == PADDING_RIGHT ) {
          int padLen = Const.toInt( variables.environmentSubstitute( getPadLen()[ i ] ), 0 );
          if ( padLen > v.getLength() ) {
            // alter meta data
            v.setLength( padLen );
          }
        }
      }
    }
  }

  @Override
  public void check( List<ICheckResult> remarks, PipelineMeta pipelineMeta, TransformMeta transforminfo,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, IVariables variables,
                     IHopMetadataProvider metadataProvider ) {

    CheckResult cr;
    String errorMessage = "";
    boolean first = true;
    boolean errorFound = false;

    if ( prev == null ) {

      errorMessage +=
        BaseMessages.getString( PKG, "StringOperationsMeta.CheckResult.NoInputReceived" ) + Const.CR;
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, errorMessage, transforminfo );
      remarks.add( cr );
    } else {

      for ( int i = 0; i < fieldInStream.length; i++ ) {
        String field = fieldInStream[ i ];

        IValueMeta v = prev.searchValueMeta( field );
        if ( v == null ) {
          if ( first ) {
            first = false;
            errorMessage +=
              BaseMessages.getString( PKG, "StringOperationsMeta.CheckResult.MissingInStreamFields" ) + Const.CR;
          }
          errorFound = true;
          errorMessage += "\t\t" + field + Const.CR;
        }
      }
      if ( errorFound ) {
        cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, errorMessage, transforminfo );
      } else {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
            PKG, "StringOperationsMeta.CheckResult.FoundInStreamFields" ), transforminfo );
      }
      remarks.add( cr );

      // Check whether all are strings
      first = true;
      errorFound = false;
      for ( int i = 0; i < fieldInStream.length; i++ ) {
        String field = fieldInStream[ i ];

        IValueMeta v = prev.searchValueMeta( field );
        if ( v != null ) {
          if ( v.getType() != IValueMeta.TYPE_STRING ) {
            if ( first ) {
              first = false;
              errorMessage +=
                BaseMessages.getString( PKG, "StringOperationsMeta.CheckResult.OperationOnNonStringFields" )
                  + Const.CR;
            }
            errorFound = true;
            errorMessage += "\t\t" + field + Const.CR;
          }
        }
      }
      if ( errorFound ) {
        cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, errorMessage, transforminfo );
      } else {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
            PKG, "StringOperationsMeta.CheckResult.AllOperationsOnStringFields" ), transforminfo );
      }
      remarks.add( cr );

      if ( fieldInStream.length > 0 ) {
        for ( int idx = 0; idx < fieldInStream.length; idx++ ) {
          if ( Utils.isEmpty( fieldInStream[ idx ] ) ) {
            cr =
              new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
                PKG, "StringOperationsMeta.CheckResult.InStreamFieldMissing", new Integer( idx + 1 )
                  .toString() ), transforminfo );
            remarks.add( cr );

          }
        }
      }

      // Check if all input fields are distinct.
      for ( int idx = 0; idx < fieldInStream.length; idx++ ) {
        for ( int jdx = 0; jdx < fieldInStream.length; jdx++ ) {
          if ( fieldInStream[ idx ].equals( fieldInStream[ jdx ] ) && idx != jdx && idx < jdx ) {
            errorMessage =
              BaseMessages.getString(
                PKG, "StringOperationsMeta.CheckResult.FieldInputError", fieldInStream[ idx ] );
            cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, errorMessage, transforminfo );
            remarks.add( cr );
          }
        }
      }

    }
  }

  @Override
  public StringOperations createTransform( TransformMeta transformMeta, StringOperationsData data, int cnr,
                                PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new StringOperations( transformMeta, this, data, cnr, pipelineMeta, pipeline );
  }

  @Override
  public StringOperationsData getTransformData() {
    return new StringOperationsData();
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  private static String getTrimTypeCode( int i ) {
    if ( i < 0 || i >= trimTypeCode.length ) {
      return trimTypeCode[ 0 ];
    }
    return trimTypeCode[ i ];
  }

  private static String getLowerUpperCode( int i ) {
    if ( i < 0 || i >= lowerUpperCode.length ) {
      return lowerUpperCode[ 0 ];
    }
    return lowerUpperCode[ i ];
  }

  private static String getInitCapCode( int i ) {
    if ( i < 0 || i >= initCapCode.length ) {
      return initCapCode[ 0 ];
    }
    return initCapCode[ i ];
  }

  private static String getMaskXMLCode( int i ) {
    if ( i < 0 || i >= maskXMLCode.length ) {
      return maskXMLCode[ 0 ];
    }
    return maskXMLCode[ i ];
  }

  private static String getDigitsCode( int i ) {
    if ( i < 0 || i >= digitsCode.length ) {
      return digitsCode[ 0 ];
    }
    return digitsCode[ i ];
  }

  private static String getRemoveSpecialCharactersCode( int i ) {
    if ( i < 0 || i >= removeSpecialCharactersCode.length ) {
      return removeSpecialCharactersCode[ 0 ];
    }
    return removeSpecialCharactersCode[ i ];
  }

  private static String getPaddingCode( int i ) {
    if ( i < 0 || i >= paddingCode.length ) {
      return paddingCode[ 0 ];
    }
    return paddingCode[ i ];
  }

  public static String getTrimTypeDesc( int i ) {
    if ( i < 0 || i >= trimTypeDesc.length ) {
      return trimTypeDesc[ 0 ];
    }
    return trimTypeDesc[ i ];
  }

  public static String getLowerUpperDesc( int i ) {
    if ( i < 0 || i >= lowerUpperDesc.length ) {
      return lowerUpperDesc[ 0 ];
    }
    return lowerUpperDesc[ i ];
  }

  public static String getInitCapDesc( int i ) {
    if ( i < 0 || i >= initCapDesc.length ) {
      return initCapDesc[ 0 ];
    }
    return initCapDesc[ i ];
  }

  public static String getMaskXMLDesc( int i ) {
    if ( i < 0 || i >= maskXMLDesc.length ) {
      return maskXMLDesc[ 0 ];
    }
    return maskXMLDesc[ i ];
  }

  public static String getDigitsDesc( int i ) {
    if ( i < 0 || i >= digitsDesc.length ) {
      return digitsDesc[ 0 ];
    }
    return digitsDesc[ i ];
  }

  public static String getRemoveSpecialCharactersDesc( int i ) {
    if ( i < 0 || i >= removeSpecialCharactersDesc.length ) {
      return removeSpecialCharactersDesc[ 0 ];
    }
    return removeSpecialCharactersDesc[ i ];
  }

  public static String getPaddingDesc( int i ) {
    if ( i < 0 || i >= paddingDesc.length ) {
      return paddingDesc[ 0 ];
    }
    return paddingDesc[ i ];
  }

  private static int getTrimTypeByCode( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < trimTypeCode.length; i++ ) {
      if ( trimTypeCode[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    return 0;
  }

  private static int getLowerUpperByCode( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < lowerUpperCode.length; i++ ) {
      if ( lowerUpperCode[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    return 0;
  }

  private static int getInitCapByCode( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < initCapCode.length; i++ ) {
      if ( initCapCode[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    return 0;
  }

  private static int getMaskXMLByCode( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < maskXMLCode.length; i++ ) {
      if ( maskXMLCode[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    return 0;
  }

  private static int getDigitsByCode( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < digitsCode.length; i++ ) {
      if ( digitsCode[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    return 0;
  }

  private static int getRemoveSpecialCharactersByCode( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < removeSpecialCharactersCode.length; i++ ) {
      if ( removeSpecialCharactersCode[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    return 0;
  }

  private static int getPaddingByCode( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < paddingCode.length; i++ ) {
      if ( paddingCode[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    return 0;
  }

  public static int getTrimTypeByDesc( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < trimTypeDesc.length; i++ ) {
      if ( trimTypeDesc[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }

    // If this fails, try to match using the code.
    return getTrimTypeByCode( tt );
  }

  public static int getLowerUpperByDesc( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < lowerUpperDesc.length; i++ ) {
      if ( lowerUpperDesc[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }

    // If this fails, try to match using the code.
    return getLowerUpperByCode( tt );
  }

  public static int getInitCapByDesc( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < initCapDesc.length; i++ ) {
      if ( initCapDesc[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }

    // If this fails, try to match using the code.
    return getInitCapByCode( tt );
  }

  public static int getMaskXMLByDesc( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < maskXMLDesc.length; i++ ) {
      if ( maskXMLDesc[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }

    // If this fails, try to match using the code.
    return getMaskXMLByCode( tt );
  }

  public static int getDigitsByDesc( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < digitsDesc.length; i++ ) {
      if ( digitsDesc[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }

    // If this fails, try to match using the code.
    return getDigitsByCode( tt );
  }

  public static int getRemoveSpecialCharactersByDesc( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < removeSpecialCharactersDesc.length; i++ ) {
      if ( removeSpecialCharactersDesc[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }

    // If this fails, try to match using the code.
    return getRemoveSpecialCharactersByCode( tt );
  }

  public static int getPaddingByDesc( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < paddingDesc.length; i++ ) {
      if ( paddingDesc[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }

    // If this fails, try to match using the code.
    return getPaddingByCode( tt );
  }
}
