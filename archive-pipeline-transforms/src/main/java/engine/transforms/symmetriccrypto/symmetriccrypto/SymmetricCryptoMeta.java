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

package org.apache.hop.pipeline.transforms.symmetriccrypto.symmetriccrypto;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
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
import org.apache.hop.pipeline.transforms.symmetriccrypto.symmetricalgorithm.SymmetricCryptoMeta;
import org.w3c.dom.Node;

import java.util.List;

/**
 * Symmetric algorithm Executes a SymmetricCrypto on the values in the input stream. Selected calculated values can
 * then be put on the output stream.
 *
 * @author Samatar
 * @since 5-apr-2003
 */
public class SymmetricCryptoPipelineMeta extends BaseTransformMeta implements ITransform {
  private static final Class<?> PKG = SymmetricCryptoPipelineMeta.class; // Needed by Translator

  /**
   * Operations type
   */
  private int operationType;

  /**
   * The operations description
   */
  public static final String[] operationTypeDesc = {
    BaseMessages.getString( PKG, "SymmetricCryptoMeta.operationType.Encrypt" ),
    BaseMessages.getString( PKG, "SymmetricCryptoMeta.operationType.Decrypt" ) };

  /**
   * The operations type codes
   */
  public static final String[] operationTypeCode = { "encrypt", "decrypt" };

  public static final int OPERATION_TYPE_ENCRYPT = 0;

  public static final int OPERATION_TYPE_DECRYPT = 1;

  private String algorithm;
  private String schema;
  private String messageField;

  private String secretKey;
  private boolean secretKeyInField;
  private String secretKeyField;

  private String resultfieldname;

  private boolean readKeyAsBinary;
  private boolean outputResultAsBinary;

  public SymmetricCryptoPipelineMeta() {
    super(); // allocate BaseTransformMeta
  }

  private static int getOperationTypeByCode( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < operationTypeCode.length; i++ ) {
      if ( operationTypeCode[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    return 0;
  }

  public int getOperationType() {
    return operationType;
  }

  public static int getOperationTypeByDesc( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < operationTypeDesc.length; i++ ) {
      if ( operationTypeDesc[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    // If this fails, try to match using the code.
    return getOperationTypeByCode( tt );
  }

  public void setOperationType( int operationType ) {
    this.operationType = operationType;
  }

  public static String getOperationTypeDesc( int i ) {
    if ( i < 0 || i >= operationTypeDesc.length ) {
      return operationTypeDesc[ 0 ];
    }
    return operationTypeDesc[ i ];
  }

  /**
   * @return Returns the XSL filename.
   */
  public String getSecretKeyField() {
    return secretKeyField;
  }

  public void setSecretKey( String secretKeyin ) {
    this.secretKey = secretKeyin;
  }

  public String getSecretKey() {
    return secretKey;
  }

  public String getResultfieldname() {
    return resultfieldname;
  }

  /*
   * Get the Message Field name
   *
   * @deprecated use {@link #getMessageField()} instead.
   */
  @Deprecated
  public String getMessageFied() {
    return getMessageField();
  }

  /*
   * Get the Message Field name
   */
  public String getMessageField() {
    return messageField;
  }

  public String getAlgorithm() {
    return algorithm;
  }

  /**
   * @param algorithm The algorithm to set.
   */
  public void setAlgorithm( String algorithm ) {
    this.algorithm = algorithm;
  }

  public boolean isReadKeyAsBinary() {
    return readKeyAsBinary;
  }

  /**
   * @param readKeyAsBinary The readKeyAsBinary to set.
   */
  public void setReadKeyAsBinary( boolean readKeyAsBinary ) {
    this.readKeyAsBinary = readKeyAsBinary;
  }

  public boolean isOutputResultAsBinary() {
    return outputResultAsBinary;
  }

  /**
   * @param outputResultAsBinary The outputResultAsBinary to set.
   */
  public void setOutputResultAsBinary( boolean outputResultAsBinary ) {
    this.outputResultAsBinary = outputResultAsBinary;
  }

  public String getSchema() {
    return schema;
  }

  /**
   * @param schema The schema to set.
   */
  public void setSchema( String schema ) {
    this.schema = schema;
  }

  /**
   * @param secretKeyField The secretKeyField to set.
   */
  public void setsecretKeyField( String secretKeyField ) {
    this.secretKeyField = secretKeyField;
  }

  public void setResultfieldname( String resultfield ) {
    this.resultfieldname = resultfield;
  }

  public void setMessageField( String fieldnamein ) {
    this.messageField = fieldnamein;
  }

  public void loadXml( Node transformNode, IHopMetadataProvider metadataProvider ) throws HopXmlException {
    readData( transformNode );
  }

  public Object clone() {
    SymmetricCryptoPipelineMeta retval = (SymmetricCryptoPipelineMeta) super.clone();

    return retval;
  }

  public boolean isSecretKeyInField() {
    return secretKeyInField;
  }

  public void setSecretKeyInField( boolean secretKeyInField ) {
    this.secretKeyInField = secretKeyInField;
  }

  private void readData( Node transformNode ) throws HopXmlException {
    try {
      operationType =
        getOperationTypeByCode( Const.NVL( XmlHandler.getTagValue( transformNode, "operation_type" ), "" ) );
      algorithm = XmlHandler.getTagValue( transformNode, "algorithm" );
      schema = XmlHandler.getTagValue( transformNode, "schema" );
      secretKeyField = XmlHandler.getTagValue( transformNode, "secretKeyField" );
      messageField = XmlHandler.getTagValue( transformNode, "messageField" );
      resultfieldname = XmlHandler.getTagValue( transformNode, "resultfieldname" );

      setSecretKey( Encr.decryptPasswordOptionallyEncrypted( XmlHandler.getTagValue( transformNode, "secretKey" ) ) );
      secretKeyInField = "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, "secretKeyInField" ) );
      readKeyAsBinary = "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, "readKeyAsBinary" ) );
      outputResultAsBinary = "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, "outputResultAsBinary" ) );

    } catch ( Exception e ) {
      throw new HopXmlException( BaseMessages.getString(
        PKG, "SymmetricCryptoMeta.Exception.UnableToLoadTransformMetaFromXML" ), e );
    }
  }

  public void setDefault() {
    secretKeyField = null;
    messageField = null;
    resultfieldname = "result";
    secretKey = null;
    secretKeyInField = false;
    operationType = OPERATION_TYPE_ENCRYPT;
    algorithm = SymmetricCryptoMeta.TYPE_ALGORYTHM_CODE[ 0 ];
    schema = algorithm;
    readKeyAsBinary = false;
    outputResultAsBinary = false;
  }

  public void getFields( IRowMeta rowMeta, String origin, IRowMeta[] info, TransformMeta nextTransform,
                         IVariables variables, IHopMetadataProvider metadataProvider ) throws HopTransformException {

    if ( !Utils.isEmpty( getResultfieldname() ) ) {
      int type = IValueMeta.TYPE_STRING;
      if ( isOutputResultAsBinary() ) {
        type = IValueMeta.TYPE_BINARY;
      }
      try {
        IValueMeta v = ValueMetaFactory.createValueMeta( getResultfieldname(), type );
        v.setOrigin( origin );
        rowMeta.addValueMeta( v );
      } catch ( Exception e ) {
        throw new HopTransformException( e );
      }
    }
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder();
    retval.append( "    " + XmlHandler.addTagValue( "operation_type", getOperationTypeCode( operationType ) ) );
    retval.append( "    " + XmlHandler.addTagValue( "algorithm", algorithm ) );
    retval.append( "    " + XmlHandler.addTagValue( "schema", schema ) );
    retval.append( "    " + XmlHandler.addTagValue( "secretKeyField", secretKeyField ) );
    retval.append( "    " + XmlHandler.addTagValue( "messageField", messageField ) );
    retval.append( "    " + XmlHandler.addTagValue( "resultfieldname", resultfieldname ) );

    retval.append( "    " ).append(
      XmlHandler.addTagValue( "secretKey", Encr.encryptPasswordIfNotUsingVariables( secretKey ) ) );

    retval.append( "    " + XmlHandler.addTagValue( "secretKeyInField", secretKeyInField ) );
    retval.append( "    " + XmlHandler.addTagValue( "readKeyAsBinary", readKeyAsBinary ) );
    retval.append( "    " + XmlHandler.addTagValue( "outputResultAsBinary", outputResultAsBinary ) );

    return retval.toString();
  }

  private static String getOperationTypeCode( int i ) {
    if ( i < 0 || i >= operationTypeCode.length ) {
      return operationTypeCode[ 0 ];
    }
    return operationTypeCode[ i ];
  }

  public void check( List<ICheckResult> remarks, PipelineMeta pipelineMeta, TransformMeta transforminfo,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, IVariables variables,
                     IHopMetadataProvider metadataProvider ) {

    CheckResult cr;

    if ( prev != null && prev.size() > 0 ) {
      cr =
        new CheckResult(
          CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "SymmetricCryptoMeta.CheckResult.ConnectedTransformOK", String.valueOf( prev.size() ) ),
          transforminfo );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "SymmetricCryptoMeta.CheckResult.NoInputReceived" ), transforminfo );

    }
    remarks.add( cr );

    // Check if The result field is given
    if ( getResultfieldname() == null ) {
      // Result Field is missing !
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "SymmetricCryptoMeta.CheckResult.ErrorResultFieldNameMissing" ), transforminfo );
      remarks.add( cr );

    }
  }

  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr,
                                PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new SymmetricCrypto( transformMeta, this, data, cnr, pipelineMeta, pipeline );

  }

  public ITransformData getTransformData() {
    return new SymmetricCryptoData();
  }

  public boolean supportsErrorHandling() {
    return true;
  }
}
