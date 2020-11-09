/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
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

package org.apache.hop.pipeline.transforms.symmetriccrypto.secretkeygenerator;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.w3c.dom.Node;

import java.util.List;

/**
 * Generate secret key. for symmetric algorithms
 *
 * @author Samatar
 * @since 01-4-2011
 */

public class SecretKeyGeneratorMeta extends BaseTransformMeta implements ITransform {
  private static final Class<?> PKG = SecretKeyGeneratorMeta.class; // Needed by Translator

  private String[] algorithm;
  private String[] scheme;
  private String[] secretKeyLength;
  private String[] secretKeyCount;

  private String secretKeyFieldName;
  private String secretKeyLengthFieldName;
  private String algorithmFieldName;

  private boolean outputKeyInBinary;

  public SecretKeyGeneratorMeta() {
    super(); // allocate BaseTransformMeta
  }

  /**
   * @return Returns the fieldAlgorithm.
   */
  public String[] getAlgorithm() {
    return algorithm;
  }

  /**
   * @return Returns the scheme.
   */
  public String[] getScheme() {
    return scheme;
  }

  /**
   * @return Returns the secretKeyFieldName.
   */
  public String getSecretKeyFieldName() {
    return secretKeyFieldName;
  }

  /**
   * @param secretKeyFieldName The secretKeyFieldName to set.
   */
  public void setSecretKeyFieldName( String secretKeyFieldName ) {
    this.secretKeyFieldName = secretKeyFieldName;
  }

  /**
   * @return Returns the secretKeyLengthFieldName.
   */
  public String getSecretKeyLengthFieldName() {
    return secretKeyLengthFieldName;
  }

  /**
   * @param outputKeyInBinary The outputKeyInBinary to set.
   */
  public void setOutputKeyInBinary( boolean outputKeyInBinary ) {
    this.outputKeyInBinary = outputKeyInBinary;
  }

  /**
   * @return Returns outputKeyInBinary secretKeyLengthFieldName.
   */
  public boolean isOutputKeyInBinary() {
    return outputKeyInBinary;
  }

  /**
   * @param secretKeyLengthFieldName The secretKeyLengthFieldName to set.
   */
  public void setSecretKeyLengthFieldName( String secretKeyLengthFieldName ) {
    this.secretKeyLengthFieldName = secretKeyLengthFieldName;
  }

  /**
   * @return Returns the algorithmFieldName.
   */
  public String getAlgorithmFieldName() {
    return algorithmFieldName;
  }

  /**
   * @param algorithmFieldName The algorithmFieldName to set.
   */
  public void setAlgorithmFieldName( String algorithmFieldName ) {
    this.algorithmFieldName = algorithmFieldName;
  }

  /**
   * @param fieldName The fieldAlgorithm to set.
   */
  public void setFieldAlgorithm( String[] fieldName ) {
    this.algorithm = fieldName;
  }

  /**
   * @param scheme The scheme to set.
   */
  public void setScheme( String[] scheme ) {
    this.scheme = scheme;
  }

  /**
   * @return Returns the fieldType.
   */
  public String[] getSecretKeyLength() {
    return secretKeyLength;
  }

  /**
   * @return Returns the secretKeyCount.
   */
  public String[] getSecretKeyCount() {
    return secretKeyCount;
  }

  public void setSecretKeyCount( String[] value ) {
    this.secretKeyCount = value;
  }

  /**
   * @param fieldType The fieldType to set.
   * @deprecated mis-named setter
   */
  @Deprecated
  public void setFieldType( String[] fieldType ) {
    this.secretKeyLength = fieldType;
  }

  /**
   * @param secretKeyLength The fieldType to set.
   */
  public void setSecretKeyLength( String[] secretKeyLength ) {
    this.secretKeyLength = secretKeyLength;
  }

  @Override
  public void loadXml( Node transformNode, IHopMetadataProvider metadataProvider ) throws HopXmlException {
    readData( transformNode );
  }

  public void allocate( int count ) {
    algorithm = new String[ count ];
    scheme = new String[ count ];
    secretKeyLength = new String[ count ];
    secretKeyCount = new String[ count ];
  }

  @Override
  public Object clone() {
    SecretKeyGeneratorMeta retval = (SecretKeyGeneratorMeta) super.clone();

    int count = algorithm.length;

    retval.allocate( count );
    System.arraycopy( algorithm, 0, retval.algorithm, 0, count );
    System.arraycopy( scheme, 0, retval.scheme, 0, count );
    System.arraycopy( secretKeyLength, 0, retval.secretKeyLength, 0, count );
    System.arraycopy( secretKeyCount, 0, retval.secretKeyCount, 0, count );

    return retval;
  }

  private void readData( Node transformNode ) throws HopXmlException {
    try {
      Node fields = XmlHandler.getSubNode( transformNode, "fields" );
      int count = XmlHandler.countNodes( fields, "field" );

      allocate( count );

      for ( int i = 0; i < count; i++ ) {
        Node fnode = XmlHandler.getSubNodeByNr( fields, "field", i );

        algorithm[ i ] = XmlHandler.getTagValue( fnode, "algorithm" );
        scheme[ i ] = XmlHandler.getTagValue( fnode, "scheme" );
        secretKeyLength[ i ] = XmlHandler.getTagValue( fnode, "secretKeyLen" );
        secretKeyCount[ i ] = XmlHandler.getTagValue( fnode, "secretKeyCount" );
      }

      secretKeyFieldName = XmlHandler.getTagValue( transformNode, "secretKeyFieldName" );
      secretKeyLengthFieldName = XmlHandler.getTagValue( transformNode, "secretKeyLengthFieldName" );
      algorithmFieldName = XmlHandler.getTagValue( transformNode, "algorithmFieldName" );

      outputKeyInBinary = "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, "outputKeyInBinary" ) );

    } catch ( Exception e ) {
      throw new HopXmlException( "Unable to read transform information from XML", e );
    }
  }

  @Override
  public void setDefault() {
    int count = 0;

    allocate( count );

    for ( int i = 0; i < count; i++ ) {
      algorithm[ i ] = "field" + i;
      scheme[ i ] = "";
      secretKeyLength[ i ] = "";
      secretKeyCount[ i ] = "";
    }
    secretKeyFieldName = BaseMessages.getString( PKG, "SecretKeyGeneratorMeta.secretKeyField" );
    secretKeyLengthFieldName = BaseMessages.getString( PKG, "SecretKeyGeneratorMeta.secretKeyLengthField" );
    algorithmFieldName = BaseMessages.getString( PKG, "SecretKeyGeneratorMeta.algorithmField" );

    outputKeyInBinary = false;
  }

  @Override
  public void getFields( IRowMeta row, String name, IRowMeta[] info, TransformMeta nextTransform,
                         IVariables variables, IHopMetadataProvider metadataProvider ) throws HopTransformException {

    IValueMeta v;
    if ( isOutputKeyInBinary() ) {
      v = new ValueMetaBinary( secretKeyFieldName );
    } else {
      v = new ValueMetaString( secretKeyFieldName );
    }
    v.setOrigin( name );
    row.addValueMeta( v );

    if ( !Utils.isEmpty( getAlgorithmFieldName() ) ) {
      v = new ValueMetaString( algorithmFieldName );
      v.setOrigin( name );
      row.addValueMeta( v );
    }

    if ( !Utils.isEmpty( getSecretKeyLengthFieldName() ) ) {
      v = new ValueMetaInteger( secretKeyLengthFieldName );
      v.setLength( IValueMeta.DEFAULT_INTEGER_LENGTH, 0 );
      v.setOrigin( name );
      row.addValueMeta( v );
    }

  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder( 200 );

    retval.append( "    <fields>" ).append( Const.CR );

    for ( int i = 0; i < algorithm.length; i++ ) {
      retval.append( "      <field>" ).append( Const.CR );
      retval.append( "        " ).append( XmlHandler.addTagValue( "algorithm", algorithm[ i ] ) );
      retval.append( "        " ).append( XmlHandler.addTagValue( "scheme", scheme[ i ] ) );
      retval.append( "        " ).append( XmlHandler.addTagValue( "secretKeyLen", secretKeyLength[ i ] ) );
      retval.append( "        " ).append( XmlHandler.addTagValue( "secretKeyCount", secretKeyCount[ i ] ) );
      retval.append( "      </field>" ).append( Const.CR );
    }
    retval.append( "    </fields>" + Const.CR );

    retval.append( "    " + XmlHandler.addTagValue( "secretKeyFieldName", secretKeyFieldName ) );
    retval.append( "    " + XmlHandler.addTagValue( "secretKeyLengthFieldName", secretKeyLengthFieldName ) );
    retval.append( "    " + XmlHandler.addTagValue( "algorithmFieldName", algorithmFieldName ) );
    retval.append( "    " + XmlHandler.addTagValue( "algorithmFieldName", algorithmFieldName ) );
    retval.append( "    " + XmlHandler.addTagValue( "outputKeyInBinary", outputKeyInBinary ) );
    return retval.toString();
  }

  @Override
  public void check( List<ICheckResult> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, IVariables variables,
                     IHopMetadataProvider metadataProvider ) {
    // See if we have input streams leading to this transform!
    int nrRemarks = remarks.size();
    for ( int i = 0; i < algorithm.length; i++ ) {
      int len = Const.toInt( pipelineMeta.environmentSubstitute( getSecretKeyLength()[ i ] ), -1 );
      if ( len < 0 ) {
        CheckResult cr =
          new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
            PKG, "SecretKeyGeneratorMeta.CheckResult.WrongLen", String.valueOf( i ) ), transformMeta );
        remarks.add( cr );
      }
      int size = Const.toInt( pipelineMeta.environmentSubstitute( getSecretKeyCount()[ i ] ), -1 );
      if ( size < 0 ) {
        CheckResult cr =
          new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
            PKG, "SecretKeyGeneratorMeta.CheckResult.WrongSize", String.valueOf( i ) ), transformMeta );
        remarks.add( cr );
      }
    }
    if ( remarks.size() == nrRemarks ) {
      CheckResult cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "SecretKeyGeneratorMeta.CheckResult.AllTypesSpecified" ), transformMeta );
      remarks.add( cr );
    }

    if ( Utils.isEmpty( getSecretKeyFieldName() ) ) {
      CheckResult cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "SecretKeyGeneratorMeta.CheckResult.secretKeyFieldMissing" ), transformMeta );
      remarks.add( cr );
    }
  }

  @Override
  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr,
                                PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new SecretKeyGenerator( transformMeta, this, data, cnr, pipelineMeta, pipeline );
  }

  @Override
  public ITransformData getTransformData() {
    return new SecretKeyGeneratorData();
  }

}
