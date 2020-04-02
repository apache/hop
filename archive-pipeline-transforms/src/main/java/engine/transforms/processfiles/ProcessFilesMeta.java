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

package org.apache.hop.pipeline.transforms.processfiles;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMetaInterface;
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
 * Created on 03-Juin-2008
 *
 */

public class ProcessFilesMeta extends BaseTransformMeta implements TransformMetaInterface {
  private static Class<?> PKG = ProcessFilesMeta.class; // for i18n purposes, needed by Translator!!

  private boolean addresultfilenames;
  private boolean overwritetargetfile;
  private boolean createparentfolder;
  public boolean simulate;

  /**
   * dynamic filename
   */
  private String sourcefilenamefield;
  private String targetfilenamefield;

  /**
   * Operations type
   */
  private int operationType;

  /**
   * The operations description
   */
  public static final String[] operationTypeDesc = {
    BaseMessages.getString( PKG, "ProcessFilesMeta.operationType.Copy" ),
    BaseMessages.getString( PKG, "ProcessFilesMeta.operationType.Move" ),
    BaseMessages.getString( PKG, "ProcessFilesMeta.operationType.Delete" ) };

  /**
   * The operations type codes
   */
  public static final String[] operationTypeCode = { "copy", "move", "delete" };

  public static final int OPERATION_TYPE_COPY = 0;

  public static final int OPERATION_TYPE_MOVE = 1;

  public static final int OPERATION_TYPE_DELETE = 2;

  public ProcessFilesMeta() {
    super(); // allocate BaseTransformMeta
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
   * @return Returns the sourcefilenamefield.
   */
  public String getDynamicSourceFileNameField() {
    return sourcefilenamefield;
  }

  /**
   * @param sourcefilenamefield The sourcefilenamefield to set.
   */
  public void setDynamicSourceFileNameField( String sourcefilenamefield ) {
    this.sourcefilenamefield = sourcefilenamefield;
  }

  /**
   * @return Returns the targetfilenamefield.
   */
  public String getDynamicTargetFileNameField() {
    return targetfilenamefield;
  }

  /**
   * @param targetfilenamefield The targetfilenamefield to set.
   */
  public void setDynamicTargetFileNameField( String targetfilenamefield ) {
    this.targetfilenamefield = targetfilenamefield;
  }

  /**
   * @return
   * @deprecated use {@link #isAddTargetFileNameToResult()}
   */
  @Deprecated
  public boolean isaddTargetFileNametoResult() {
    return isAddTargetFileNameToResult();
  }

  public boolean isAddTargetFileNameToResult() {
    return addresultfilenames;
  }

  public boolean isOverwriteTargetFile() {
    return overwritetargetfile;
  }

  public boolean isCreateParentFolder() {
    return createparentfolder;
  }

  /**
   * @param addresultfilenames
   * @deprecated use {@link #setAddTargetFileNameToResult(boolean)}
   */
  @Deprecated
  public void setaddTargetFileNametoResult( boolean addresultfilenames ) {
    setAddTargetFileNameToResult( addresultfilenames );
  }

  public void setAddTargetFileNameToResult( boolean addresultfilenames ) {
    this.addresultfilenames = addresultfilenames;
  }

  public void setOverwriteTargetFile( boolean overwritetargetfile ) {
    this.overwritetargetfile = overwritetargetfile;
  }

  public void setCreateParentFolder( boolean createparentfolder ) {
    this.createparentfolder = createparentfolder;
  }

  public void setSimulate( boolean simulate ) {
    this.simulate = simulate;
  }

  public boolean isSimulate() {
    return this.simulate;
  }

  @Override
  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode, metaStore );
  }

  @Override
  public Object clone() {
    ProcessFilesMeta retval = (ProcessFilesMeta) super.clone();

    return retval;
  }

  @Override
  public void setDefault() {
    addresultfilenames = false;
    overwritetargetfile = false;
    createparentfolder = false;
    simulate = true;
    operationType = OPERATION_TYPE_COPY;
  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder();

    retval.append( "    " + XMLHandler.addTagValue( "sourcefilenamefield", sourcefilenamefield ) );
    retval.append( "    " + XMLHandler.addTagValue( "targetfilenamefield", targetfilenamefield ) );
    retval.append( "    " ).append(
      XMLHandler.addTagValue( "operation_type", getOperationTypeCode( operationType ) ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "addresultfilenames", addresultfilenames ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "overwritetargetfile", overwritetargetfile ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "createparentfolder", createparentfolder ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "simulate", simulate ) );

    return retval.toString();
  }

  private static String getOperationTypeCode( int i ) {
    if ( i < 0 || i >= operationTypeCode.length ) {
      return operationTypeCode[ 0 ];
    }
    return operationTypeCode[ i ];
  }

  private void readData( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    try {
      sourcefilenamefield = XMLHandler.getTagValue( transformNode, "sourcefilenamefield" );
      targetfilenamefield = XMLHandler.getTagValue( transformNode, "targetfilenamefield" );
      operationType =
        getOperationTypeByCode( Const.NVL( XMLHandler.getTagValue( transformNode, "operation_type" ), "" ) );
      addresultfilenames = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "addresultfilenames" ) );
      overwritetargetfile = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "overwritetargetfile" ) );
      createparentfolder = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "createparentfolder" ) );
      simulate = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "simulate" ) );

    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages
        .getString( PKG, "ProcessFilesMeta.Exception.UnableToReadTransformMeta" ), e );
    }
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

  @Override
  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                     IMetaStore metaStore ) {
    CheckResult cr;
    String error_message = "";

    // source filename
    if ( Utils.isEmpty( sourcefilenamefield ) ) {
      error_message = BaseMessages.getString( PKG, "ProcessFilesMeta.CheckResult.SourceFileFieldMissing" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, transformMeta );
      remarks.add( cr );
    } else {
      error_message = BaseMessages.getString( PKG, "ProcessFilesMeta.CheckResult.TargetFileFieldOK" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK, error_message, transformMeta );
      remarks.add( cr );
    }
    if ( operationType != OPERATION_TYPE_DELETE && Utils.isEmpty( targetfilenamefield ) ) {
      error_message = BaseMessages.getString( PKG, "ProcessFilesMeta.CheckResult.TargetFileFieldMissing" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, transformMeta );
      remarks.add( cr );
    } else {
      error_message = BaseMessages.getString( PKG, "ProcessFilesMeta.CheckResult.SourceFileFieldOK" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK, error_message, transformMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "ProcessFilesMeta.CheckResult.ReceivingInfoFromOtherTransforms" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "ProcessFilesMeta.CheckResult.NoInpuReceived" ), transformMeta );
      remarks.add( cr );
    }

  }

  @Override
  public TransformInterface getTransform( TransformMeta transformMeta, TransformDataInterface transformDataInterface, int cnr,
                                PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new ProcessFiles( transformMeta, transformDataInterface, cnr, pipelineMeta, pipeline );
  }

  @Override
  public TransformDataInterface getTransformData() {
    return new ProcessFilesData();
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }
}
