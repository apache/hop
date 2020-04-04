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

package org.apache.hop.pipeline.transforms.pgpdecryptstream;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.iVariables;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInterface;
import org.w3c.dom.Node;

import java.util.List;

/*
 * Created on 03-Juin-2008
 *
 */

public class PGPDecryptStreamMeta extends BaseTransformMeta implements TransformMetaInterface {
  private static Class<?> PKG = PGPDecryptStreamMeta.class; // for i18n purposes, needed by Translator!!

  /**
   * GPG location
   */
  private String gpglocation;

  /**
   * passhrase
   **/
  private String passhrase;

  /**
   * Flag : passphrase from field
   **/
  private boolean passphraseFromField;

  /**
   * passphrase fieldname
   **/
  private String passphraseFieldName;

  /**
   * dynamic stream filed
   */
  private String streamfield;

  /**
   * function result: new value name
   */
  private String resultfieldname;

  public PGPDecryptStreamMeta() {
    super(); // allocate BaseTransformMeta
  }

  public void setGPGLocation( String gpglocation ) {
    this.gpglocation = gpglocation;
  }

  public String getGPGLocation() {
    return gpglocation;
  }

  /**
   * @return Returns the streamfield.
   */
  public String getStreamField() {
    return streamfield;
  }

  /**
   * @param streamfield The streamfield to set.
   */
  public void setStreamField( String streamfield ) {
    this.streamfield = streamfield;
  }

  /**
   * @return Returns the passphraseFieldName.
   */
  public String getPassphraseFieldName() {
    return passphraseFieldName;
  }

  /**
   * @param passphraseFieldName The passphraseFieldName to set.
   */
  public void setPassphraseFieldName( String passphraseFieldName ) {
    this.passphraseFieldName = passphraseFieldName;
  }

  /**
   * @return Returns the passphraseFromField.
   */
  public boolean isPassphraseFromField() {
    return passphraseFromField;
  }

  /**
   * @param passphraseFromField The passphraseFromField to set.
   */
  public void setPassphraseFromField( boolean passphraseFromField ) {
    this.passphraseFromField = passphraseFromField;
  }

  /**
   * @return Returns the resultName.
   */
  public String getResultFieldName() {
    return resultfieldname;
  }

  /**
   * @param resultfieldname The resultFieldName to set
   */
  public void setResultFieldName( String resultfieldname ) {
    this.resultfieldname = resultfieldname;
  }

  /**
   * @return Returns the passhrase.
   */
  public String getPassphrase() {
    return passhrase;
  }

  /**
   * @param passhrase The passhrase to set.
   */
  public void setPassphrase( String passhrase ) {
    this.passhrase = passhrase;
  }

  @Override
  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode, metaStore );
  }

  @Override
  public Object clone() {
    PGPDecryptStreamMeta retval = (PGPDecryptStreamMeta) super.clone();

    return retval;
  }

  @Override
  public void setDefault() {
    resultfieldname = "result";
    streamfield = null;
    passhrase = null;
    gpglocation = null;
  }

  @Override
  public void getFields( IRowMeta inputRowMeta, String name, IRowMeta[] info, TransformMeta nextTransform,
                         iVariables variables, IMetaStore metaStore ) throws HopTransformException {
    // Output fields (String)
    if ( !Utils.isEmpty( resultfieldname ) ) {
      IValueMeta v = new ValueMetaString( variables.environmentSubstitute( resultfieldname ) );
      v.setOrigin( name );
      inputRowMeta.addValueMeta( v );
    }

  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder();
    retval.append( "    " + XMLHandler.addTagValue( "gpglocation", gpglocation ) );
    retval.append( "    " ).append(
      XMLHandler.addTagValue( "passhrase", Encr.encryptPasswordIfNotUsingVariables( passhrase ) ) );
    retval.append( "    " + XMLHandler.addTagValue( "streamfield", streamfield ) );
    retval.append( "    " + XMLHandler.addTagValue( "resultfieldname", resultfieldname ) );
    retval.append( "    " + XMLHandler.addTagValue( "passphraseFromField", passphraseFromField ) );
    retval.append( "    " + XMLHandler.addTagValue( "passphraseFieldName", passphraseFieldName ) );
    return retval.toString();
  }

  private void readData( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    try {
      gpglocation = XMLHandler.getTagValue( transformNode, "gpglocation" );
      passhrase = Encr.decryptPasswordOptionallyEncrypted( XMLHandler.getTagValue( transformNode, "passhrase" ) );
      streamfield = XMLHandler.getTagValue( transformNode, "streamfield" );
      resultfieldname = XMLHandler.getTagValue( transformNode, "resultfieldname" );
      passphraseFromField = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "passphraseFromField" ) );
      passphraseFieldName = XMLHandler.getTagValue( transformNode, "passphraseFieldName" );
    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString(
        PKG, "PGPDecryptStreamMeta.Exception.UnableToReadTransformMeta" ), e );
    }
  }

  @Override
  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, iVariables variables,
                     IMetaStore metaStore ) {
    CheckResult cr;
    String error_message = "";

    if ( Utils.isEmpty( gpglocation ) ) {
      error_message = BaseMessages.getString( PKG, "PGPDecryptStreamMeta.CheckResult.GPGLocationMissing" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, transformMeta );
      remarks.add( cr );
    } else {
      error_message = BaseMessages.getString( PKG, "PGPDecryptStreamMeta.CheckResult.GPGLocationOK" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK, error_message, transformMeta );
    }
    if ( !isPassphraseFromField() ) {
      // Check static pass-phrase
      if ( Utils.isEmpty( passhrase ) ) {
        error_message = BaseMessages.getString( PKG, "PGPDecryptStreamMeta.CheckResult.PassphraseMissing" );
        cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, transformMeta );
        remarks.add( cr );
      } else {
        error_message = BaseMessages.getString( PKG, "PGPDecryptStreamMeta.CheckResult.PassphraseOK" );
        cr = new CheckResult( CheckResult.TYPE_RESULT_OK, error_message, transformMeta );
      }
    }
    if ( Utils.isEmpty( resultfieldname ) ) {
      error_message = BaseMessages.getString( PKG, "PGPDecryptStreamMeta.CheckResult.ResultFieldMissing" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, transformMeta );
      remarks.add( cr );
    } else {
      error_message = BaseMessages.getString( PKG, "PGPDecryptStreamMeta.CheckResult.ResultFieldOK" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK, error_message, transformMeta );
      remarks.add( cr );
    }
    if ( Utils.isEmpty( streamfield ) ) {
      error_message = BaseMessages.getString( PKG, "PGPDecryptStreamMeta.CheckResult.StreamFieldMissing" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, transformMeta );
      remarks.add( cr );
    } else {
      error_message = BaseMessages.getString( PKG, "PGPDecryptStreamMeta.CheckResult.StreamFieldOK" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK, error_message, transformMeta );
      remarks.add( cr );
    }
    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "PGPDecryptStreamMeta.CheckResult.ReceivingInfoFromOtherTransforms" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "PGPDecryptStreamMeta.CheckResult.NoInpuReceived" ), transformMeta );
      remarks.add( cr );
    }

  }

  @Override
  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr,
                                PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new PGPDecryptStream( transformMeta, this, data, cnr, pipelineMeta, pipeline );
  }

  @Override
  public ITransformData getTransformData() {
    return new PGPDecryptStreamData();
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

}
