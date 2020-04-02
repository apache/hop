/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.fuzzymatch;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformDataInterface;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformIOMetaInterface;
import org.apache.hop.pipeline.transform.TransformInterface;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInterface;
import org.apache.hop.pipeline.transform.errorhandling.Stream;
import org.apache.hop.pipeline.transform.errorhandling.StreamIcon;
import org.apache.hop.pipeline.transform.errorhandling.StreamInterface;
import org.apache.hop.pipeline.transform.errorhandling.StreamInterface.StreamType;
import org.w3c.dom.Node;

import java.util.List;

public class FuzzyMatchMeta extends BaseTransformMeta implements TransformMetaInterface {
  private static Class<?> PKG = FuzzyMatchMeta.class; // for i18n purposes, needed by Translator!!

  public static final String DEFAULT_SEPARATOR = ",";
  /**
   * Algorithms type
   */
  private int algorithm;

  /**
   * The algorithms description
   */
  public static final String[] algorithmDesc = {
    BaseMessages.getString( PKG, "FuzzyMatchMeta.algorithm.Levenshtein" ),
    BaseMessages.getString( PKG, "FuzzyMatchMeta.algorithm.DamerauLevenshtein" ),
    BaseMessages.getString( PKG, "FuzzyMatchMeta.algorithm.NeedlemanWunsch" ),
    BaseMessages.getString( PKG, "FuzzyMatchMeta.algorithm.Jaro" ),
    BaseMessages.getString( PKG, "FuzzyMatchMeta.algorithm.JaroWinkler" ),
    BaseMessages.getString( PKG, "FuzzyMatchMeta.algorithm.PairSimilarity" ),
    BaseMessages.getString( PKG, "FuzzyMatchMeta.algorithm.Metaphone" ),
    BaseMessages.getString( PKG, "FuzzyMatchMeta.algorithm.DoubleMetaphone" ),
    BaseMessages.getString( PKG, "FuzzyMatchMeta.algorithm.SoundEx" ),
    BaseMessages.getString( PKG, "FuzzyMatchMeta.algorithm.RefinedSoundEx" ) };

  /**
   * The algorithms type codes
   */
  public static final String[] algorithmCode = {
    "levenshtein", "dameraulevenshtein", "needlemanwunsch", "jaro", "jarowinkler", "pairsimilarity",
    "metaphone", "doublemataphone", "soundex", "refinedsoundex" };

  public static final int OPERATION_TYPE_LEVENSHTEIN = 0;

  public static final int OPERATION_TYPE_DAMERAU_LEVENSHTEIN = 1;

  public static final int OPERATION_TYPE_NEEDLEMAN_WUNSH = 2;

  public static final int OPERATION_TYPE_JARO = 3;

  public static final int OPERATION_TYPE_JARO_WINKLER = 4;

  public static final int OPERATION_TYPE_PAIR_SIMILARITY = 5;

  public static final int OPERATION_TYPE_METAPHONE = 6;

  public static final int OPERATION_TYPE_DOUBLE_METAPHONE = 7;

  public static final int OPERATION_TYPE_SOUNDEX = 8;

  public static final int OPERATION_TYPE_REFINED_SOUNDEX = 9;

  /**
   * field in lookup stream with which we look up values
   */
  private String lookupfield;

  /**
   * field in input stream for which we lookup values
   */
  private String mainstreamfield;

  /**
   * output match fieldname
   **/
  private String outputmatchfield;

  /**
   * ouput value fieldname
   **/
  private String outputvaluefield;

  /**
   * case sensitive
   **/
  private boolean caseSensitive;

  /**
   * minimal value, distance for levenshtein, similarity, ...
   **/
  private String minimalValue;

  /**
   * maximal value, distance for levenshtein, similarity, ...
   **/
  private String maximalValue;

  /**
   * values separator ...
   **/
  private String separator;

  /**
   * get closer matching value
   **/
  private boolean closervalue;

  /**
   * return these field values from lookup
   */
  private String[] value;

  /**
   * rename to this after lookup
   */
  private String[] valueName;

  public FuzzyMatchMeta() {
    super(); // allocate BaseTransformMeta
  }

  /**
   * @return Returns the value.
   */
  public String[] getValue() {
    return value;
  }

  /**
   * @param value The value to set.
   */
  public void setValue( String[] value ) {
    this.value = value;
  }

  public void allocate( int nrvalues ) {
    value = new String[ nrvalues ];
    valueName = new String[ nrvalues ];
  }

  public Object clone() {
    FuzzyMatchMeta retval = (FuzzyMatchMeta) super.clone();

    int nrvalues = value.length;

    retval.allocate( nrvalues );

    System.arraycopy( value, 0, retval.value, 0, nrvalues );
    System.arraycopy( valueName, 0, retval.valueName, 0, nrvalues );

    return retval;
  }

  /**
   * @return Returns the mainstreamfield.
   */
  public String getMainStreamField() {
    return mainstreamfield;
  }

  /**
   * @param mainstreamfield The mainstreamfield to set.
   */
  public void setMainStreamField( String mainstreamfield ) {
    this.mainstreamfield = mainstreamfield;
  }

  /**
   * @return Returns the lookupfield.
   */
  public String getLookupField() {
    return lookupfield;
  }

  /**
   * @param lookupfield The lookupfield to set.
   */
  public void setLookupField( String lookupfield ) {
    this.lookupfield = lookupfield;
  }

  /**
   * @return Returns the outputmatchfield.
   */
  public String getOutputMatchField() {
    return outputmatchfield;
  }

  /**
   * @param outputmatchfield The outputmatchfield to set.
   */
  public void setOutputMatchField( String outputmatchfield ) {
    this.outputmatchfield = outputmatchfield;
  }

  /**
   * @return Returns the outputmatchfield.
   */
  public String getOutputValueField() {
    return outputvaluefield;
  }

  /**
   * @param outputvaluefield The outputvaluefield to set.
   */
  public void setOutputValueField( String outputvaluefield ) {
    this.outputvaluefield = outputvaluefield;
  }

  /**
   * @return Returns the closervalue.
   */
  public boolean isGetCloserValue() {
    return closervalue;
  }

  /**
   * @return Returns the valueName.
   */
  public String[] getValueName() {
    return valueName;
  }

  /**
   * @param valueName The valueName to set.
   */
  public void setValueName( String[] valueName ) {
    this.valueName = valueName;
  }

  /**
   * @param closervalue The closervalue to set.
   */
  public void setGetCloserValue( boolean closervalue ) {
    this.closervalue = closervalue;
  }

  /**
   * @return Returns the caseSensitive.
   */
  public boolean isCaseSensitive() {
    return caseSensitive;
  }

  /**
   * @param caseSensitive The caseSensitive to set.
   */
  public void setCaseSensitive( boolean caseSensitive ) {
    this.caseSensitive = caseSensitive;
  }

  /**
   * @return Returns the minimalValue.
   */
  public String getMinimalValue() {
    return minimalValue;
  }

  /**
   * @param minimalValue The minimalValue to set.
   */
  public void setMinimalValue( String minimalValue ) {
    this.minimalValue = minimalValue;
  }

  /**
   * @return Returns the minimalValue.
   */
  public String getMaximalValue() {
    return maximalValue;
  }

  /**
   * @param maximalValue The maximalValue to set.
   */
  public void setMaximalValue( String maximalValue ) {
    this.maximalValue = maximalValue;
  }

  /**
   * @return Returns the separator.
   */
  public String getSeparator() {
    return separator;
  }

  /**
   * @param separator The separator to set.
   */
  public void setSeparator( String separator ) {
    this.separator = separator;
  }

  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode, metaStore );
  }

  public int getAlgorithmType() {
    return algorithm;
  }

  public void setAlgorithmType( int algorithm ) {
    this.algorithm = algorithm;
  }

  public static String getAlgorithmTypeDesc( int i ) {
    if ( i < 0 || i >= algorithmDesc.length ) {
      return algorithmDesc[ 0 ];
    }
    return algorithmDesc[ i ];
  }

  public static int getAlgorithmTypeByDesc( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < algorithmDesc.length; i++ ) {
      if ( algorithmDesc[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    // If this fails, try to match using the code.
    return getAlgorithmTypeByCode( tt );
  }

  private static int getAlgorithmTypeByCode( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < algorithmCode.length; i++ ) {
      if ( algorithmCode[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    return 0;
  }

  private void readData( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    try {

      String lookupFromTransformName = XMLHandler.getTagValue( transformNode, "from" );
      StreamInterface infoStream = getTransformIOMeta().getInfoStreams().get( 0 );
      infoStream.setSubject( lookupFromTransformName );

      lookupfield = XMLHandler.getTagValue( transformNode, "lookupfield" );
      mainstreamfield = XMLHandler.getTagValue( transformNode, "mainstreamfield" );

      caseSensitive = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "caseSensitive" ) );
      closervalue = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "closervalue" ) );
      minimalValue = XMLHandler.getTagValue( transformNode, "minimalValue" );
      maximalValue = XMLHandler.getTagValue( transformNode, "maximalValue" );
      separator = XMLHandler.getTagValue( transformNode, "separator" );

      outputmatchfield = XMLHandler.getTagValue( transformNode, "outputmatchfield" );
      outputvaluefield = XMLHandler.getTagValue( transformNode, "outputvaluefield" );

      algorithm = getAlgorithmTypeByCode( Const.NVL( XMLHandler.getTagValue( transformNode, "algorithm" ), "" ) );

      Node lookup = XMLHandler.getSubNode( transformNode, "lookup" );
      int nrvalues = XMLHandler.countNodes( lookup, "value" );

      allocate( nrvalues );

      for ( int i = 0; i < nrvalues; i++ ) {
        Node vnode = XMLHandler.getSubNodeByNr( lookup, "value", i );

        value[ i ] = XMLHandler.getTagValue( vnode, "name" );
        valueName[ i ] = XMLHandler.getTagValue( vnode, "rename" );
        if ( valueName[ i ] == null ) {
          valueName[ i ] = value[ i ]; // default: same name to return!
        }
      }

    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString(
        PKG, "FuzzyMatchMeta.Exception.UnableToLoadTransformMetaFromXML" ), e );
    }
  }

  private static String getAlgorithmTypeCode( int i ) {
    if ( i < 0 || i >= algorithmCode.length ) {
      return algorithmCode[ 0 ];
    }
    return algorithmCode[ i ];
  }

  public void setDefault() {
    value = null;
    valueName = null;
    separator = DEFAULT_SEPARATOR;
    closervalue = true;
    minimalValue = "0";
    maximalValue = "1";
    caseSensitive = false;
    lookupfield = null;
    mainstreamfield = null;
    outputmatchfield = BaseMessages.getString( PKG, "FuzzyMatchMeta.OutputMatchFieldname" );
    outputvaluefield = BaseMessages.getString( PKG, "FuzzyMatchMeta.OutputValueFieldname" );

    int nrvalues = 0;

    allocate( nrvalues );

    for ( int i = 0; i < nrvalues; i++ ) {
      value[ i ] = "value" + i;
      valueName[ i ] = "valuename" + i;
    }

  }

  public void getFields( RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, TransformMeta nextTransform,
                         VariableSpace space, IMetaStore metaStore ) throws HopTransformException {
    // Add match field
    ValueMetaInterface v =
      new ValueMetaString( space.environmentSubstitute( getOutputMatchField() ) );
    v.setOrigin( name );
    v.setStorageType( ValueMetaInterface.STORAGE_TYPE_NORMAL );
    inputRowMeta.addValueMeta( v );

    String mainField = space.environmentSubstitute( getOutputValueField() );
    if ( !Utils.isEmpty( mainField ) && isGetCloserValue() ) {
      switch ( getAlgorithmType() ) {
        case FuzzyMatchMeta.OPERATION_TYPE_DAMERAU_LEVENSHTEIN:
        case FuzzyMatchMeta.OPERATION_TYPE_LEVENSHTEIN:
          v = new ValueMetaInteger( mainField );
          v.setLength( ValueMetaInterface.DEFAULT_INTEGER_LENGTH );
          break;
        case FuzzyMatchMeta.OPERATION_TYPE_JARO:
        case FuzzyMatchMeta.OPERATION_TYPE_JARO_WINKLER:
        case FuzzyMatchMeta.OPERATION_TYPE_PAIR_SIMILARITY:
          v = new ValueMetaNumber( mainField );
          break;
        default:
          // Phonetic algorithms
          v = new ValueMetaString( mainField );
          break;
      }
      v.setStorageType( ValueMetaInterface.STORAGE_TYPE_NORMAL );
      v.setOrigin( name );
      inputRowMeta.addValueMeta( v );
    }

    boolean activateAdditionalFields =
      isGetCloserValue()
        || ( getAlgorithmType() == FuzzyMatchMeta.OPERATION_TYPE_DOUBLE_METAPHONE )
        || ( getAlgorithmType() == FuzzyMatchMeta.OPERATION_TYPE_SOUNDEX )
        || ( getAlgorithmType() == FuzzyMatchMeta.OPERATION_TYPE_REFINED_SOUNDEX )
        || ( getAlgorithmType() == FuzzyMatchMeta.OPERATION_TYPE_METAPHONE );

    if ( activateAdditionalFields ) {
      if ( info != null && info.length == 1 && info[ 0 ] != null ) {
        for ( int i = 0; i < valueName.length; i++ ) {
          v = info[ 0 ].searchValueMeta( value[ i ] );
          if ( v != null ) {
            // Configuration error/missing resources...

            v.setName( valueName[ i ] );
            v.setOrigin( name );
            v.setStorageType( ValueMetaInterface.STORAGE_TYPE_NORMAL ); // Only normal storage goes into the cache
            inputRowMeta.addValueMeta( v );
          } else {
            throw new HopTransformException( BaseMessages.getString(
              PKG, "FuzzyMatchMeta.Exception.ReturnValueCanNotBeFound", value[ i ] ) );
          }
        }
      } else {
        for ( int i = 0; i < valueName.length; i++ ) {
          v = new ValueMetaString( valueName[ i ] );
          v.setOrigin( name );
          inputRowMeta.addValueMeta( v );
        }
      }
    }
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder();

    StreamInterface infoStream = getTransformIOMeta().getInfoStreams().get( 0 );
    retval.append( "    " + XMLHandler.addTagValue( "from", infoStream.getTransformName() ) );
    retval.append( "    " + XMLHandler.addTagValue( "lookupfield", lookupfield ) );
    retval.append( "    " + XMLHandler.addTagValue( "mainstreamfield", mainstreamfield ) );
    retval.append( "    " + XMLHandler.addTagValue( "outputmatchfield", outputmatchfield ) );
    retval.append( "    " + XMLHandler.addTagValue( "outputvaluefield", outputvaluefield ) );

    retval.append( "    " + XMLHandler.addTagValue( "caseSensitive", caseSensitive ) );
    retval.append( "    " + XMLHandler.addTagValue( "closervalue", closervalue ) );
    retval.append( "    " + XMLHandler.addTagValue( "minimalValue", minimalValue ) );
    retval.append( "    " + XMLHandler.addTagValue( "maximalValue", maximalValue ) );
    retval.append( "    " + XMLHandler.addTagValue( "separator", separator ) );

    retval.append( "    " ).append( XMLHandler.addTagValue( "algorithm", getAlgorithmTypeCode( algorithm ) ) );

    retval.append( "    <lookup>" + Const.CR );
    for ( int i = 0; i < value.length; i++ ) {
      retval.append( "      <value>" + Const.CR );
      retval.append( "        " + XMLHandler.addTagValue( "name", value[ i ] ) );
      retval.append( "        " + XMLHandler.addTagValue( "rename", valueName[ i ] ) );
      retval.append( "      </value>" + Const.CR );
    }
    retval.append( "    </lookup>" + Const.CR );

    return retval.toString();
  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                     IMetaStore metaStore ) {
    CheckResult cr;

    if ( prev != null && prev.size() > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "FuzzyMatchMeta.CheckResult.TransformReceivingFields", prev.size() + "" ), transformMeta );
      remarks.add( cr );

      // Starting from selected fields in ...
      // Check the fields from the previous stream!
      String mainField = pipelineMeta.environmentSubstitute( getMainStreamField() );
      int idx = prev.indexOfValue( mainField );
      if ( idx < 0 ) {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
            PKG, "FuzzyMatchMeta.CheckResult.MainFieldNotFound", mainField ), transformMeta );
      } else {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
            PKG, "FuzzyMatchMeta.CheckResult.MainFieldFound", mainField ), transformMeta );
      }
      remarks.add( cr );

    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "FuzzyMatchMeta.CheckResult.CouldNotFindFieldsFromPreviousTransforms" ), transformMeta );
      remarks.add( cr );
    }

    if ( info != null && info.size() > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "FuzzyMatchMeta.CheckResult.TransformReceivingLookupData", info.size() + "" ), transformMeta );
      remarks.add( cr );

      // Check the fields from the lookup stream!
      String lookupField = pipelineMeta.environmentSubstitute( getLookupField() );

      int idx = info.indexOfValue( lookupField );
      if ( idx < 0 ) {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
            PKG, "FuzzyMatchMeta.CheckResult.FieldNotFoundInLookupStream", lookupField ), transformMeta );
      } else {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
            PKG, "FuzzyMatchMeta.CheckResult.FieldFoundInTheLookupStream", lookupField ), transformMeta );
      }
      remarks.add( cr );

      String error_message = "";
      boolean error_found = false;

      // Check the values to retrieve from the lookup stream!
      for ( int i = 0; i < value.length; i++ ) {
        idx = info.indexOfValue( value[ i ] );
        if ( idx < 0 ) {
          error_message += "\t\t" + value[ i ] + Const.CR;
          error_found = true;
        }
      }
      if ( error_found ) {
        error_message =
          BaseMessages.getString( PKG, "FuzzyMatchMeta.CheckResult.FieldsNotFoundInLookupStream2" )
            + Const.CR + Const.CR + error_message;
        cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, error_message, transformMeta );
      } else {
        cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
            PKG, "FuzzyMatchMeta.CheckResult.AllFieldsFoundInTheLookupStream2" ), transformMeta );
      }
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "FuzzyMatchMeta.CheckResult.FieldsNotFoundFromInLookupSep" ), transformMeta );
      remarks.add( cr );
    }

    // See if the source transform is filled in!
    StreamInterface infoStream = getTransformIOMeta().getInfoStreams().get( 0 );
    if ( infoStream.getTransformMeta() == null ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "FuzzyMatchMeta.CheckResult.SourceTransformNotSelected" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "FuzzyMatchMeta.CheckResult.SourceTransformIsSelected" ), transformMeta );
      remarks.add( cr );

      // See if the transform exists!
      //
      if ( info != null ) {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
            PKG, "FuzzyMatchMeta.CheckResult.SourceTransformExist", infoStream.getTransformName() + "" ), transformMeta );
        remarks.add( cr );
      } else {
        cr =
          new CheckResult(
            CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
            PKG, "FuzzyMatchMeta.CheckResult.SourceTransformDoesNotExist", infoStream.getTransformName() + "" ),
            transformMeta );
        remarks.add( cr );
      }
    }

    // See if we have input streams leading to this transform!
    if ( input.length >= 2 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "FuzzyMatchMeta.CheckResult.TransformReceivingInfoFromInputTransforms", input.length + "" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "FuzzyMatchMeta.CheckResult.NeedAtLeast2InputStreams", Const.CR, Const.CR ), transformMeta );
      remarks.add( cr );
    }
  }

  @Override
  public void searchInfoAndTargetTransforms( List<TransformMeta> transforms ) {
    List<StreamInterface> infoStreams = getTransformIOMeta().getInfoStreams();
    for ( StreamInterface stream : infoStreams ) {
      stream.setTransformMeta( TransformMeta.findTransform( transforms, (String) stream.getSubject() ) );
    }
  }

  public TransformInterface getTransform( TransformMeta transformMeta, TransformDataInterface transformDataInterface, int cnr,
                                PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new FuzzyMatch( transformMeta, transformDataInterface, cnr, pipelineMeta, pipeline );
  }

  public TransformDataInterface getTransformData() {
    return new FuzzyMatchData();
  }

  public boolean excludeFromRowLayoutVerification() {
    return true;
  }

  public boolean supportsErrorHandling() {
    return true;
  }

  /**
   * Returns the Input/Output metadata for this transform. The generator transform only produces output, does not accept input!
   */
  public TransformIOMetaInterface getTransformIOMeta() {
    TransformIOMetaInterface ioMeta = super.getTransformIOMeta( false );
    if ( ioMeta == null ) {

      ioMeta = new TransformIOMeta( true, true, false, false, false, false );

      StreamInterface stream =
        new Stream(
          StreamType.INFO, null, BaseMessages.getString( PKG, "FuzzyMatchMeta.InfoStream.Description" ),
          StreamIcon.INFO, null );
      ioMeta.addStream( stream );
      setTransformIOMeta( ioMeta );
    }

    return ioMeta;
  }

  public void resetTransformIoMeta() {
    // Do nothing, don't reset as there is no need to do this.
  }
}
