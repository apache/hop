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

package org.apache.hop.pipeline.transforms.filelocked;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.annotations.Transform;
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
import org.apache.hop.pipeline.transform.*;
import org.w3c.dom.Node;

import java.util.List;

/**
 * Check if a file is locked *
 *
 * @author Samatar
 * @since 03-Juin-2009
 */

@Transform(
        id = "FileLocked",
        image = "ui/images/CFL.svg",
        i18nPackageName = "i18n:org.apache.hop.pipeline.transforms.filelocked",
        name = "BaseTransform.TypeLongDesc.FileLocked",
        description = "BaseTransform.TypeTooltipDesc.FileLocked",
        categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Lookup",
        documentationUrl = ""
)
public class FileLockedMeta extends BaseTransformMeta implements TransformMetaInterface<FileLocked, FileLockedData> {
  private static Class<?> PKG = FileLockedMeta.class; // for i18n purposes, needed by Translator!!

  private boolean addresultfilenames;

  /**
   * dynamic filename
   */
  private String filenamefield;

  /**
   * function result: new value name
   */
  private String resultfieldname;

  public FileLockedMeta() {
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

  public boolean addResultFilenames() {
    return addresultfilenames;
  }

  public void setaddResultFilenames( boolean addresultfilenames ) {
    this.addresultfilenames = addresultfilenames;
  }

  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode, metaStore );
  }

  public Object clone() {
    FileLockedMeta retval = (FileLockedMeta) super.clone();

    return retval;
  }

  public void setDefault() {
    resultfieldname = "result";
    addresultfilenames = false;
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

    retval.append( "    " + XMLHandler.addTagValue( "filenamefield", filenamefield ) );
    retval.append( "    " + XMLHandler.addTagValue( "resultfieldname", resultfieldname ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "addresultfilenames", addresultfilenames ) );
    return retval.toString();
  }

  private void readData( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    try {
      filenamefield = XMLHandler.getTagValue( transformNode, "filenamefield" );
      resultfieldname = XMLHandler.getTagValue( transformNode, "resultfieldname" );
      addresultfilenames = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "addresultfilenames" ) );
    } catch ( Exception e ) {
      throw new HopXMLException(
        BaseMessages.getString( PKG, "FileLockedMeta.Exception.UnableToReadTransformMeta" ), e );
    }
  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                     IMetaStore metaStore ) {
    CheckResult cr;
    String error_message = "";

    if ( Utils.isEmpty( resultfieldname ) ) {
      error_message = BaseMessages.getString( PKG, "FileLockedMeta.CheckResult.ResultFieldMissing" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, transformMeta );
      remarks.add( cr );
    } else {
      error_message = BaseMessages.getString( PKG, "FileLockedMeta.CheckResult.ResultFieldOK" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK, error_message, transformMeta );
      remarks.add( cr );
    }
    if ( Utils.isEmpty( filenamefield ) ) {
      error_message = BaseMessages.getString( PKG, "FileLockedMeta.CheckResult.FileFieldMissing" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, transformMeta );
      remarks.add( cr );
    } else {
      error_message = BaseMessages.getString( PKG, "FileLockedMeta.CheckResult.FileFieldOK" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK, error_message, transformMeta );
      remarks.add( cr );
    }
    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "FileLockedMeta.CheckResult.ReceivingInfoFromOtherTransforms" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "FileLockedMeta.CheckResult.NoInpuReceived" ), transformMeta );
      remarks.add( cr );
    }

  }

  public FileLocked createTransform( TransformMeta transformMeta, FileLockedData transformDataInterface, int cnr,
                                     PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new FileLocked( transformMeta, transformDataInterface, cnr, pipelineMeta, pipeline );
  }

  public FileLockedData getTransformData() {
    return new FileLockedData();
  }

  public boolean supportsErrorHandling() {
    return true;
  }

  @Override
  public String getDialogClassName(){
    return FileLockedDialog.class.getName();
  }
}
