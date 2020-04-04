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

package org.apache.hop.pipeline.transforms.fileexists;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
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

public class FileExistsMeta extends BaseTransformMeta implements TransformMetaInterface {
  private static Class<?> PKG = FileExistsMeta.class; // for i18n purposes, needed by Translator!!

  private boolean addresultfilenames;

  /**
   * dynamic filename
   */
  private String filenamefield;

  private String filetypefieldname;

  private boolean includefiletype;

  /**
   * function result: new value name
   */
  private String resultfieldname;

  public FileExistsMeta() {
    super(); // allocate BaseTransformMeta
  }

  /**
   * @return Returns the filenamefield.
   */
  public String getDynamicFilenameField() {
    return filenamefield;
  }

  /**
   * @param filenamefield The filenamefield to set.
   */
  public void setDynamicFilenameField( String filenamefield ) {
    this.filenamefield = filenamefield;
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

  /**
   * @param filetypefieldname The filetypefieldname to set.
   */
  public void setFileTypeFieldName( String filetypefieldname ) {
    this.filetypefieldname = filetypefieldname;
  }

  /**
   * @return Returns the filetypefieldname.
   */
  public String getFileTypeFieldName() {
    return filetypefieldname;
  }

  public boolean includeFileType() {
    return includefiletype;
  }

  public boolean addResultFilenames() {
    return addresultfilenames;
  }

  public void setaddResultFilenames( boolean addresultfilenames ) {
    this.addresultfilenames = addresultfilenames;
  }

  public void setincludeFileType( boolean includefiletype ) {
    this.includefiletype = includefiletype;
  }

  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode, metaStore );
  }

  public Object clone() {
    FileExistsMeta retval = (FileExistsMeta) super.clone();

    return retval;
  }

  public void setDefault() {
    resultfieldname = "result";
    filetypefieldname = null;
    includefiletype = false;
    addresultfilenames = false;
  }

  public void getFields( IRowMeta inputRowMeta, String name, IRowMeta[] info, TransformMeta nextTransform,
                         iVariables variables, IMetaStore metaStore ) throws HopTransformException {
    // Output fields (String)
    if ( !Utils.isEmpty( resultfieldname ) ) {
      IValueMeta v =
        new ValueMetaBoolean( variables.environmentSubstitute( resultfieldname ) );
      v.setOrigin( name );
      inputRowMeta.addValueMeta( v );
    }

    if ( includefiletype && !Utils.isEmpty( filetypefieldname ) ) {
      IValueMeta v =
        new ValueMetaString( variables.environmentSubstitute( filetypefieldname ) );
      v.setOrigin( name );
      inputRowMeta.addValueMeta( v );
    }
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder();

    retval.append( "    " + XMLHandler.addTagValue( "filenamefield", filenamefield ) );
    retval.append( "    " + XMLHandler.addTagValue( "resultfieldname", resultfieldname ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "includefiletype", includefiletype ) );
    retval.append( "    " + XMLHandler.addTagValue( "filetypefieldname", filetypefieldname ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "addresultfilenames", addresultfilenames ) );
    return retval.toString();
  }

  private void readData( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    try {
      filenamefield = XMLHandler.getTagValue( transformNode, "filenamefield" );
      resultfieldname = XMLHandler.getTagValue( transformNode, "resultfieldname" );
      includefiletype = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "includefiletype" ) );
      filetypefieldname = XMLHandler.getTagValue( transformNode, "filetypefieldname" );
      addresultfilenames = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "addresultfilenames" ) );
    } catch ( Exception e ) {
      throw new HopXMLException(
        BaseMessages.getString( PKG, "FileExistsMeta.Exception.UnableToReadTransformMeta" ), e );
    }
  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, iVariables variables,
                     IMetaStore metaStore ) {
    CheckResult cr;
    String error_message = "";

    if ( Utils.isEmpty( resultfieldname ) ) {
      error_message = BaseMessages.getString( PKG, "FileExistsMeta.CheckResult.ResultFieldMissing" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, transformMeta );
      remarks.add( cr );
    } else {
      error_message = BaseMessages.getString( PKG, "FileExistsMeta.CheckResult.ResultFieldOK" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK, error_message, transformMeta );
      remarks.add( cr );
    }
    if ( Utils.isEmpty( filenamefield ) ) {
      error_message = BaseMessages.getString( PKG, "FileExistsMeta.CheckResult.FileFieldMissing" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, transformMeta );
      remarks.add( cr );
    } else {
      error_message = BaseMessages.getString( PKG, "FileExistsMeta.CheckResult.FileFieldOK" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK, error_message, transformMeta );
      remarks.add( cr );
    }
    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "FileExistsMeta.CheckResult.ReceivingInfoFromOtherTransforms" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "FileExistsMeta.CheckResult.NoInpuReceived" ), transformMeta );
      remarks.add( cr );
    }

  }

  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr,
                                PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new FileExists( transformMeta, this, data, cnr, pipelineMeta, pipeline );
  }

  public ITransformData getTransformData() {
    return new FileExistsData();
  }

  public boolean supportsErrorHandling() {
    return true;
  }

}
