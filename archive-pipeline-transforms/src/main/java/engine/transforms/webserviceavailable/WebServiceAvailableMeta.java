/*! ******************************************************************************
 *
 * Pentaho Data Integration
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

package org.apache.hop.pipeline.transforms.webserviceavailable;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformDataInterface;
import org.apache.hop.pipeline.transform.TransformInterface;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInterface;
import org.w3c.dom.Node;

import java.util.List;

/*
 * Created on 03-01-2010
 *
 */

public class WebServiceAvailableMeta extends BaseTransformMeta implements TransformMetaInterface {
  private static Class<?> PKG = WebServiceAvailableMeta.class; // for i18n purposes, needed by Translator!!

  /**
   * dynamic filename
   */
  private String urlField;

  /**
   * function result: new value name
   */
  private String resultfieldname;

  private String connectTimeOut;

  private String readTimeOut;

  public WebServiceAvailableMeta() {
    super(); // allocate BaseTransformMeta
  }

  /**
   * @return Returns the urlField.
   */
  public String getURLField() {
    return urlField;
  }

  /**
   * @param urlField The urlField to set.
   */
  public void setURLField( String urlField ) {
    this.urlField = urlField;
  }

  public void setConnectTimeOut( String timeout ) {
    this.connectTimeOut = timeout;
  }

  public String getConnectTimeOut() {
    return connectTimeOut;
  }

  public void setReadTimeOut( String timeout ) {
    this.readTimeOut = timeout;
  }

  public String getReadTimeOut() {
    return readTimeOut;
  }

  /**
   * @return Returns the resultName.
   */
  public String getResultFieldName() {
    return resultfieldname;
  }

  /**
   * @param resultfieldname The resultfieldname to set.
   */
  public void setResultFieldName( String resultfieldname ) {
    this.resultfieldname = resultfieldname;
  }

  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode, metaStore );
  }

  public Object clone() {
    WebServiceAvailableMeta retval = (WebServiceAvailableMeta) super.clone();

    return retval;
  }

  public void setDefault() {
    resultfieldname = "result";
    connectTimeOut = "0";
    readTimeOut = "0";
  }

  public void getFields( RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, TransformMeta nextTransform,
                         VariableSpace space, IMetaStore metaStore ) throws HopTransformException {

    if ( !Utils.isEmpty( resultfieldname ) ) {
      ValueMetaInterface v = new ValueMetaBoolean( resultfieldname );
      v.setOrigin( name );
      inputRowMeta.addValueMeta( v );
    }

  }

  public String getXML() {
    StringBuilder retval = new StringBuilder();

    retval.append( "    " + XMLHandler.addTagValue( "urlField", urlField ) );
    retval.append( "    " + XMLHandler.addTagValue( "readTimeOut", readTimeOut ) );
    retval.append( "    " + XMLHandler.addTagValue( "connectTimeOut", connectTimeOut ) );
    retval.append( "    " + XMLHandler.addTagValue( "resultfieldname", resultfieldname ) );
    return retval.toString();
  }

  private void readData( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    try {
      urlField = XMLHandler.getTagValue( transformNode, "urlField" );
      connectTimeOut = XMLHandler.getTagValue( transformNode, "connectTimeOut" );
      readTimeOut = XMLHandler.getTagValue( transformNode, "readTimeOut" );
      resultfieldname = XMLHandler.getTagValue( transformNode, "resultfieldname" );
    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString(
        PKG, "WebServiceAvailableMeta.Exception.UnableToReadTransformMeta" ), e );
    }
  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                     IMetaStore metaStore ) {
    CheckResult cr;
    String error_message = "";

    if ( Utils.isEmpty( resultfieldname ) ) {
      error_message = BaseMessages.getString( PKG, "WebServiceAvailableMeta.CheckResult.ResultFieldMissing" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, transformMeta );
      remarks.add( cr );
    } else {
      error_message = BaseMessages.getString( PKG, "WebServiceAvailableMeta.CheckResult.ResultFieldOK" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK, error_message, transformMeta );
      remarks.add( cr );
    }
    if ( Utils.isEmpty( urlField ) ) {
      error_message = BaseMessages.getString( PKG, "WebServiceAvailableMeta.CheckResult.URLFieldMissing" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, transformMeta );
      remarks.add( cr );
    } else {
      error_message = BaseMessages.getString( PKG, "WebServiceAvailableMeta.CheckResult.URLFieldOK" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK, error_message, transformMeta );
      remarks.add( cr );
    }
    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "WebServiceAvailableMeta.CheckResult.ReceivingInfoFromOtherTransforms" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "WebServiceAvailableMeta.CheckResult.NoInpuReceived" ), transformMeta );
      remarks.add( cr );
    }

  }

  public TransformInterface getTransform( TransformMeta transformMeta, TransformDataInterface transformDataInterface, int cnr,
                                PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new WebServiceAvailable( transformMeta, transformDataInterface, cnr, pipelineMeta, pipeline );
  }

  public TransformDataInterface getTransformData() {
    return new WebServiceAvailableData();
  }

  public boolean supportsErrorHandling() {
    return true;
  }
}
