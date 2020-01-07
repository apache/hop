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

package org.apache.hop.trans.steps.changefileencoding;

import java.util.List;

import org.apache.hop.core.annotations.Step;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;

import org.apache.hop.shared.SharedObjectInterface;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStepMeta;
import org.apache.hop.trans.step.StepDataInterface;
import org.apache.hop.trans.step.StepInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.StepMetaInterface;
import org.apache.hop.metastore.api.IMetaStore;
import org.w3c.dom.Node;

@Step( id = "ChangeFileEncoding", i18nPackageName = "org.apache.hop.trans.steps.changefileencoding",
    name = "ChangeFileEncoding.Name", description = "ChangeFileEncoding.Description",
    categoryDescription = "i18n:org.apache.hop.trans.step:BaseStep.Category.Utility" )
public class ChangeFileEncodingMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = ChangeFileEncoding.class; // for i18n purposes, needed by Translator2!!

  private boolean addsourceresultfilenames;
  private boolean addtargetresultfilenames;

  /** dynamic filename */
  private String filenamefield;

  private String targetfilenamefield;
  private String targetencoding;
  private String sourceencoding;
  private boolean createparentfolder;

  public ChangeFileEncodingMeta() {
    super(); // allocate BaseStepMeta
  }

  /**
   * @return Returns the filenamefield.
   */
  public String getDynamicFilenameField() {
    return filenamefield;
  }

  /**
   * @param filenamefield
   *          The filenamefield to set.
   */
  public void setDynamicFilenameField( String filenamefield ) {
    this.filenamefield = filenamefield;
  }

  /**
   * @return Returns the targetfilenamefield.
   */
  public String getTargetFilenameField() {
    return targetfilenamefield;
  }

  /**
   * @param targetfilenamefield
   *          The targetfilenamefield to set.
   */
  public void setTargetFilenameField( String targetfilenamefield ) {
    this.targetfilenamefield = targetfilenamefield;
  }

  /**
   * @return Returns the sourceencoding.
   */
  public String getSourceEncoding() {
    return sourceencoding;
  }

  /**
   * @param encoding
   *          The sourceencoding to set.
   */
  public void setSourceEncoding( String encoding ) {
    this.sourceencoding = encoding;
  }

  /**
   * @return Returns the targetencoding.
   */
  public String getTargetEncoding() {
    return targetencoding;
  }

  /**
   * @param encoding
   *          The targetencoding to set.
   */
  public void setTargetEncoding( String encoding ) {
    this.targetencoding = encoding;
  }

  public boolean addSourceResultFilenames() {
    return addsourceresultfilenames;
  }

  public void setaddSourceResultFilenames( boolean addresultfilenames ) {
    this.addsourceresultfilenames = addresultfilenames;
  }

  public boolean addTargetResultFilenames() {
    return addtargetresultfilenames;
  }

  public void setaddTargetResultFilenames( boolean addresultfilenames ) {
    this.addtargetresultfilenames = addresultfilenames;
  }

  public boolean isCreateParentFolder() {
    return createparentfolder;
  }

  public void setCreateParentFolder( boolean createparentfolder ) {
    this.createparentfolder = createparentfolder;
  }

  public void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    readData( stepnode, metaStore );
  }

  public Object clone() {
    ChangeFileEncodingMeta retval = (ChangeFileEncodingMeta) super.clone();

    return retval;
  }

  public void setDefault() {
    addsourceresultfilenames = false;
    addtargetresultfilenames = false;
    targetfilenamefield = null;
    sourceencoding = System.getProperty( "file.encoding" );
    targetencoding = null;
    createparentfolder = false;
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder();

    retval.append( "    " + XMLHandler.addTagValue( "filenamefield", filenamefield ) );
    retval.append( "    " + XMLHandler.addTagValue( "targetfilenamefield", targetfilenamefield ) );
    retval.append( "    " + XMLHandler.addTagValue( "sourceencoding", sourceencoding ) );
    retval.append( "    " + XMLHandler.addTagValue( "targetencoding", targetencoding ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "addsourceresultfilenames", addsourceresultfilenames ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "addtargetresultfilenames", addtargetresultfilenames ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "createparentfolder", createparentfolder ) );

    return retval.toString();
  }

  private void readData( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    try {
      filenamefield = XMLHandler.getTagValue( stepnode, "filenamefield" );
      targetfilenamefield = XMLHandler.getTagValue( stepnode, "targetfilenamefield" );
      sourceencoding = XMLHandler.getTagValue( stepnode, "sourceencoding" );
      targetencoding = XMLHandler.getTagValue( stepnode, "targetencoding" );
      addsourceresultfilenames = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "addsourceresultfilenames" ) );
      addtargetresultfilenames = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "addtargetresultfilenames" ) );
      createparentfolder = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "createparentfolder" ) );

    } catch ( Exception e ) {
      throw new HopXMLException(
          BaseMessages.getString( PKG, "ChangeFileEncodingMeta.Exception.UnableToReadStepInfo" ), e );
    }
  }

  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev,
      String[] input, String[] output, RowMetaInterface info, VariableSpace space, IMetaStore metaStore ) {
    CheckResult cr;
    String error_message = "";

    if ( Utils.isEmpty( filenamefield ) ) {
      error_message = BaseMessages.getString( PKG, "ChangeFileEncodingMeta.CheckResult.FileFieldMissing" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, stepMeta );
      remarks.add( cr );
    } else {
      error_message = BaseMessages.getString( PKG, "ChangeFileEncodingMeta.CheckResult.FileFieldOK" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK, error_message, stepMeta );
      remarks.add( cr );
    }
    if ( Utils.isEmpty( targetfilenamefield ) ) {
      error_message = BaseMessages.getString( PKG, "ChangeFileEncodingMeta.CheckResult.TargetFileFieldMissing" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, stepMeta );
      remarks.add( cr );
    } else {
      error_message = BaseMessages.getString( PKG, "ChangeFileEncodingMeta.CheckResult.TargetFileFieldOK" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK, error_message, stepMeta );
      remarks.add( cr );
    }
    String realSourceEncoding = transMeta.environmentSubstitute( getSourceEncoding() );
    if ( Utils.isEmpty( realSourceEncoding ) ) {
      error_message = BaseMessages.getString( PKG, "ChangeFileEncodingMeta.CheckResult.SourceEncodingMissing" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, stepMeta );
      remarks.add( cr );
    } else {
      error_message = BaseMessages.getString( PKG, "ChangeFileEncodingMeta.CheckResult.SourceEncodingOK" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK, error_message, stepMeta );
      remarks.add( cr );
    }
    String realTargetEncoding = transMeta.environmentSubstitute( getTargetEncoding() );
    if ( Utils.isEmpty( realTargetEncoding ) ) {
      error_message = BaseMessages.getString( PKG, "ChangeFileEncodingMeta.CheckResult.TargetEncodingMissing" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, stepMeta );
      remarks.add( cr );
    } else {
      error_message = BaseMessages.getString( PKG, "ChangeFileEncodingMeta.CheckResult.TargetEncodingOK" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK, error_message, stepMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr =
          new CheckResult( CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString( PKG, "ChangeFileEncodingMeta.CheckResult.ReceivingInfoFromOtherSteps" ),
              stepMeta );
      remarks.add( cr );
    } else {
      cr =
          new CheckResult( CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString( PKG, "ChangeFileEncodingMeta.CheckResult.NoInpuReceived" ), stepMeta );
      remarks.add( cr );
    }

  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta,
      Trans trans ) {
    return new ChangeFileEncoding( stepMeta, stepDataInterface, cnr, transMeta, trans );
  }

  public StepDataInterface getStepData() {
    return new ChangeFileEncodingData();
  }

  public boolean supportsErrorHandling() {
    return true;
  }

}
