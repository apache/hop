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

package org.apache.hop.pipeline.transforms.pgpencryptstream;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
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

public class PGPEncryptStreamMeta extends BaseTransformMeta implements TransformMetaInterface {
  private static Class<?> PKG = PGPEncryptStreamMeta.class; // for i18n purposes, needed by Translator!!

  /**
   * GPG location
   */
  private String gpglocation;

  /**
   * Key name
   **/
  private String keyname;

  /**
   * dynamic stream filed
   */
  private String streamfield;

  /**
   * function result: new value name
   */
  private String resultFieldName;

  /**
   * Flag: keyname is dynamic
   **/
  private boolean keynameInField;

  /**
   * keyname fieldname
   **/
  private String keynameFieldName;

  public PGPEncryptStreamMeta() {
    super(); // allocate BaseTransformMeta
  }

  /**
   * @deprecated - typo
   */
  @Deprecated
  public void setGPGPLocation( String value ) {
    this.setGPGLocation( value );
  }

  public void setGPGLocation( String value ) {
    this.gpglocation = value;
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
   * @return Returns the keynameFieldName.
   */
  public String getKeynameFieldName() {
    return keynameFieldName;
  }

  /**
   * @param keynameFieldName The keynameFieldName to set.
   */
  public void setKeynameFieldName( String keynameFieldName ) {
    this.keynameFieldName = keynameFieldName;
  }

  /**
   * @return Returns the keynameInField.
   */
  public boolean isKeynameInField() {
    return keynameInField;
  }

  /**
   * @param keynameInField The keynameInField to set.
   */
  public void setKeynameInField( boolean keynameInField ) {
    this.keynameInField = keynameInField;
  }

  /**
   * @return Returns the resultName.
   */
  public String getResultFieldName() {
    return resultFieldName;
  }

  /**
   * @param resultFieldName The resultfieldname to set.
   */
  public void setResultFieldName( String resultFieldName ) {
    this.resultFieldName = resultFieldName;
  }

  /**
   * @return Returns the keyname.
   */
  public String getKeyName() {
    return keyname;
  }

  /**
   * @param keyname The keyname to set.
   */
  public void setKeyName( String keyname ) {
    this.keyname = keyname;
  }

  @Override
  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode, metaStore );
  }

  @Override
  public Object clone() {
    PGPEncryptStreamMeta retval = (PGPEncryptStreamMeta) super.clone();

    return retval;
  }

  @Override
  public void setDefault() {
    resultFieldName = "result";
    streamfield = null;
    keyname = null;
    gpglocation = null;
    keynameInField = false;
    keynameFieldName = null;
  }

  @Override
  public void getFields( IRowMeta inputRowMeta, String name, IRowMeta[] info, TransformMeta nextTransform,
                         iVariables variables, IMetaStore metaStore ) throws HopTransformException {
    // Output fields (String)
    if ( !Utils.isEmpty( resultFieldName ) ) {
      IValueMeta v = new ValueMetaString( variables.environmentSubstitute( resultFieldName ) );
      v.setOrigin( name );
      inputRowMeta.addValueMeta( v );
    }

  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder();
    retval.append( "    " + XMLHandler.addTagValue( "gpglocation", gpglocation ) );
    retval.append( "    " + XMLHandler.addTagValue( "keyname", keyname ) );
    retval.append( "    " + XMLHandler.addTagValue( "keynameInField", keynameInField ) );
    retval.append( "    " + XMLHandler.addTagValue( "keynameFieldName", keynameFieldName ) );
    retval.append( "    " + XMLHandler.addTagValue( "streamfield", streamfield ) );
    retval.append( "    " + XMLHandler.addTagValue( "resultfieldname", resultFieldName ) );
    return retval.toString();
  }

  private void readData( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    try {
      gpglocation = XMLHandler.getTagValue( transformNode, "gpglocation" );
      keyname = XMLHandler.getTagValue( transformNode, "keyname" );

      keynameInField = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "keynameInField" ) );
      keynameFieldName = XMLHandler.getTagValue( transformNode, "keynameFieldName" );
      streamfield = XMLHandler.getTagValue( transformNode, "streamfield" );
      resultFieldName = XMLHandler.getTagValue( transformNode, "resultfieldname" );
    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString(
        PKG, "PGPEncryptStreamMeta.Exception.UnableToReadTransformMeta" ), e );
    }
  }

  @Override
  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, iVariables variables,
                     IMetaStore metaStore ) {
    CheckResult cr;
    String error_message = "";

    if ( Utils.isEmpty( gpglocation ) ) {
      error_message = BaseMessages.getString( PKG, "PGPEncryptStreamMeta.CheckResult.GPGLocationMissing" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, transformMeta );
      remarks.add( cr );
    } else {
      error_message = BaseMessages.getString( PKG, "PGPEncryptStreamMeta.CheckResult.GPGLocationOK" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK, error_message, transformMeta );
    }
    if ( !isKeynameInField() ) {
      if ( Utils.isEmpty( keyname ) ) {
        error_message = BaseMessages.getString( PKG, "PGPEncryptStreamMeta.CheckResult.KeyNameMissing" );
        cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, transformMeta );
        remarks.add( cr );
      } else {
        error_message = BaseMessages.getString( PKG, "PGPEncryptStreamMeta.CheckResult.KeyNameOK" );
        cr = new CheckResult( CheckResult.TYPE_RESULT_OK, error_message, transformMeta );
      }
    }
    if ( Utils.isEmpty( resultFieldName ) ) {
      error_message = BaseMessages.getString( PKG, "PGPEncryptStreamMeta.CheckResult.ResultFieldMissing" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, transformMeta );
      remarks.add( cr );
    } else {
      error_message = BaseMessages.getString( PKG, "PGPEncryptStreamMeta.CheckResult.ResultFieldOK" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK, error_message, transformMeta );
      remarks.add( cr );
    }
    if ( Utils.isEmpty( streamfield ) ) {
      error_message = BaseMessages.getString( PKG, "PGPEncryptStreamMeta.CheckResult.StreamFieldMissing" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, transformMeta );
      remarks.add( cr );
    } else {
      error_message = BaseMessages.getString( PKG, "PGPEncryptStreamMeta.CheckResult.StreamFieldOK" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK, error_message, transformMeta );
      remarks.add( cr );
    }
    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "PGPEncryptStreamMeta.CheckResult.ReceivingInfoFromOtherTransforms" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "PGPEncryptStreamMeta.CheckResult.NoInpuReceived" ), transformMeta );
      remarks.add( cr );
    }

  }

  @Override
  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr,
                                PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new PGPEncryptStream( transformMeta, this, data, cnr, pipelineMeta, pipeline );
  }

  @Override
  public ITransformData getTransformData() {
    return new PGPEncryptStreamData();
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

}
