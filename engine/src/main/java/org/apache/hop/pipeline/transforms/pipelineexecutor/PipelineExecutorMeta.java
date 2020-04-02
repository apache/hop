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

package org.apache.hop.pipeline.transforms.pipelineexecutor;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.TransformWithMappingMeta;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformIOMetaInterface;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.pipeline.ISubPipelineAwareMeta;
import org.apache.hop.pipeline.PipelineMeta.PipelineType;
import org.apache.hop.pipeline.transform.TransformInterface;
import org.apache.hop.pipeline.transform.TransformMetaInterface;
import org.apache.hop.pipeline.transform.errorhandling.Stream;
import org.apache.hop.pipeline.transform.errorhandling.StreamIcon;
import org.apache.hop.pipeline.transform.errorhandling.StreamInterface;
import org.apache.hop.pipeline.transform.errorhandling.StreamInterface.StreamType;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

/**
 * Meta-data for the Pipeline Executor transform.
 *
 * @author Matt
 * @since 18-mar-2013
 */
public class PipelineExecutorMeta
  extends TransformWithMappingMeta<PipelineExecutor, PipelineExecutorData>
  implements TransformMetaInterface<PipelineExecutor, PipelineExecutorData>, ISubPipelineAwareMeta {

  private static Class<?> PKG = PipelineExecutorMeta.class; // for i18n purposes, needed by Translator!!

  static final String F_EXECUTION_RESULT_TARGET_TRANSFORM = "execution_result_target_transform";
  static final String F_RESULT_FILE_TARGET_TRANSFORM = "result_files_target_transform";
  static final String F_EXECUTOR_OUTPUT_TRANSFORM = "executors_output_transform";

  /**
   * The number of input rows that are sent as result rows to the job in one go, defaults to "1"
   */
  private String groupSize;

  /**
   * Optional name of a field to group rows together that are sent together to the job as result rows (empty default)
   */
  private String groupField;

  /**
   * Optional time in ms that is spent waiting and accumulating rows before they are given to the job as result rows
   * (empty default, "0")
   */
  private String groupTime;

  private PipelineExecutorParameters parameters;

  private String executionResultTargetTransform;

  private TransformMeta executionResultTargetTransformMeta;

  /**
   * The optional name of the output field that will contain the execution time of the pipeline in (Integer in ms)
   */
  private String executionTimeField;

  /**
   * The optional name of the output field that will contain the execution result (Boolean)
   */
  private String executionResultField;

  /**
   * The optional name of the output field that will contain the number of errors (Integer)
   */
  private String executionNrErrorsField;

  /**
   * The optional name of the output field that will contain the number of rows read (Integer)
   */
  private String executionLinesReadField;

  /**
   * The optional name of the output field that will contain the number of rows written (Integer)
   */
  private String executionLinesWrittenField;

  /**
   * The optional name of the output field that will contain the number of rows input (Integer)
   */
  private String executionLinesInputField;

  /**
   * The optional name of the output field that will contain the number of rows output (Integer)
   */
  private String executionLinesOutputField;

  /**
   * The optional name of the output field that will contain the number of rows rejected (Integer)
   */
  private String executionLinesRejectedField;

  /**
   * The optional name of the output field that will contain the number of rows updated (Integer)
   */
  private String executionLinesUpdatedField;

  /**
   * The optional name of the output field that will contain the number of rows deleted (Integer)
   */
  private String executionLinesDeletedField;

  /**
   * The optional name of the output field that will contain the number of files retrieved (Integer)
   */
  private String executionFilesRetrievedField;

  /**
   * The optional name of the output field that will contain the exit status of the last executed shell script (Integer)
   */
  private String executionExitStatusField;

  /**
   * The optional name of the output field that will contain the log text of the pipeline execution (String)
   */
  private String executionLogTextField;

  /**
   * The optional name of the output field that will contain the log channel ID of the pipeline execution (String)
   */
  private String executionLogChannelIdField;

  /**
   * The optional transform to send the result rows to
   */
  private String outputRowsSourceTransform;

  private TransformMeta outputRowsSourceTransformMeta;

  private String[] outputRowsField;

  private int[] outputRowsType;

  private int[] outputRowsLength;

  private int[] outputRowsPrecision;

  /**
   * The optional transform to send the result files to
   */
  private String resultFilesTargetTransform;

  private TransformMeta resultFilesTargetTransformMeta;

  private String resultFilesFileNameField;

  /**
   * These fields are related to executor transform's "basic" output stream, where a copy of input data will be placed
   */
  private String executorsOutputTransform;

  private TransformMeta executorsOutputTransformMeta;

  private IMetaStore metaStore;

  public PipelineExecutorMeta() {
    super(); // allocate BaseTransformMeta

    parameters = new PipelineExecutorParameters();
    this.allocate( 0 );
  }

  public void allocate( int nrFields ) {
    outputRowsField = new String[ nrFields ];
    outputRowsType = new int[ nrFields ];
    outputRowsLength = new int[ nrFields ];
    outputRowsPrecision = new int[ nrFields ];
  }

  public Object clone() {
    PipelineExecutorMeta retval = (PipelineExecutorMeta) super.clone();
    int nrFields = outputRowsField.length;
    retval.allocate( nrFields );
    System.arraycopy( outputRowsField, 0, retval.outputRowsField, 0, nrFields );
    System.arraycopy( outputRowsType, 0, retval.outputRowsType, 0, nrFields );
    System.arraycopy( outputRowsLength, 0, retval.outputRowsLength, 0, nrFields );
    System.arraycopy( outputRowsPrecision, 0, retval.outputRowsPrecision, 0, nrFields );
    return retval;
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder( 300 );

    retval.append( "    " ).append( XMLHandler.addTagValue( "filename", fileName ) );

    retval.append( "    " ).append( XMLHandler.addTagValue( "group_size", groupSize ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "group_field", groupField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "group_time", groupTime ) );

    // Add the mapping parameters too
    //
    retval.append( "      " ).append( parameters.getXML() ).append( Const.CR );

    // The output side...
    //
    retval.append( "    " ).append( XMLHandler.addTagValue( F_EXECUTION_RESULT_TARGET_TRANSFORM,
      executionResultTargetTransformMeta == null ? null : executionResultTargetTransformMeta.getName() ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "execution_time_field", executionTimeField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "execution_result_field", executionResultField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "execution_errors_field", executionNrErrorsField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "execution_lines_read_field", executionLinesReadField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "execution_lines_written_field",
      executionLinesWrittenField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "execution_lines_input_field", executionLinesInputField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "execution_lines_output_field",
      executionLinesOutputField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "execution_lines_rejected_field",
      executionLinesRejectedField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "execution_lines_updated_field",
      executionLinesUpdatedField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "execution_lines_deleted_field",
      executionLinesDeletedField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "execution_files_retrieved_field",
      executionFilesRetrievedField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "execution_exit_status_field", executionExitStatusField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "execution_log_text_field", executionLogTextField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "execution_log_channelid_field",
      executionLogChannelIdField ) );

    retval.append( "    " ).append( XMLHandler.addTagValue( "result_rows_target_transform", outputRowsSourceTransformMeta == null
      ? null : outputRowsSourceTransformMeta.getName() ) );
    for ( int i = 0; i < outputRowsField.length; i++ ) {
      retval.append( "      " ).append( XMLHandler.openTag( "result_rows_field" ) );
      retval.append( XMLHandler.addTagValue( "name", outputRowsField[ i ], false ) );
      retval
        .append( XMLHandler.addTagValue( "type", ValueMetaFactory.getValueMetaName( outputRowsType[ i ] ), false ) );
      retval.append( XMLHandler.addTagValue( "length", outputRowsLength[ i ], false ) );
      retval.append( XMLHandler.addTagValue( "precision", outputRowsPrecision[ i ], false ) );
      retval.append( XMLHandler.closeTag( "result_rows_field" ) ).append( Const.CR );
    }

    retval.append( "    " ).append( XMLHandler.addTagValue( F_RESULT_FILE_TARGET_TRANSFORM, resultFilesTargetTransformMeta == null
      ? null : resultFilesTargetTransformMeta.getName() ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "result_files_file_name_field",
      resultFilesFileNameField ) );

    retval.append( "    " ).append( XMLHandler.addTagValue( F_EXECUTOR_OUTPUT_TRANSFORM, executorsOutputTransformMeta == null
      ? null : executorsOutputTransformMeta.getName() ) );

    return retval.toString();
  }

  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    try {
      fileName = XMLHandler.getTagValue( transformNode, "filename" );

      groupSize = XMLHandler.getTagValue( transformNode, "group_size" );
      groupField = XMLHandler.getTagValue( transformNode, "group_field" );
      groupTime = XMLHandler.getTagValue( transformNode, "group_time" );

      // Load the mapping parameters too..
      //
      Node mappingParametersNode = XMLHandler.getSubNode( transformNode, PipelineExecutorParameters.XML_TAG );
      parameters = new PipelineExecutorParameters( mappingParametersNode );

      // The output side...
      //
      executionResultTargetTransform = XMLHandler.getTagValue( transformNode, F_EXECUTION_RESULT_TARGET_TRANSFORM );
      executionTimeField = XMLHandler.getTagValue( transformNode, "execution_time_field" );
      executionResultField = XMLHandler.getTagValue( transformNode, "execution_result_field" );
      executionNrErrorsField = XMLHandler.getTagValue( transformNode, "execution_errors_field" );
      executionLinesReadField = XMLHandler.getTagValue( transformNode, "execution_lines_read_field" );
      executionLinesWrittenField = XMLHandler.getTagValue( transformNode, "execution_lines_written_field" );
      executionLinesInputField = XMLHandler.getTagValue( transformNode, "execution_lines_input_field" );
      executionLinesOutputField = XMLHandler.getTagValue( transformNode, "execution_lines_output_field" );
      executionLinesRejectedField = XMLHandler.getTagValue( transformNode, "execution_lines_rejected_field" );
      executionLinesUpdatedField = XMLHandler.getTagValue( transformNode, "execution_lines_updated_field" );
      executionLinesDeletedField = XMLHandler.getTagValue( transformNode, "execution_lines_deleted_field" );
      executionFilesRetrievedField = XMLHandler.getTagValue( transformNode, "execution_files_retrieved_field" );
      executionExitStatusField = XMLHandler.getTagValue( transformNode, "execution_exit_status_field" );
      executionLogTextField = XMLHandler.getTagValue( transformNode, "execution_log_text_field" );
      executionLogChannelIdField = XMLHandler.getTagValue( transformNode, "execution_log_channelid_field" );

      outputRowsSourceTransform = XMLHandler.getTagValue( transformNode, "result_rows_target_transform" );

      int nrFields = XMLHandler.countNodes( transformNode, "result_rows_field" );
      allocate( nrFields );

      for ( int i = 0; i < nrFields; i++ ) {

        Node fieldNode = XMLHandler.getSubNodeByNr( transformNode, "result_rows_field", i );

        outputRowsField[ i ] = XMLHandler.getTagValue( fieldNode, "name" );
        outputRowsType[ i ] = ValueMetaFactory.getIdForValueMeta( XMLHandler.getTagValue( fieldNode, "type" ) );
        outputRowsLength[ i ] = Const.toInt( XMLHandler.getTagValue( fieldNode, "length" ), -1 );
        outputRowsPrecision[ i ] = Const.toInt( XMLHandler.getTagValue( fieldNode, "precision" ), -1 );
      }

      resultFilesTargetTransform = XMLHandler.getTagValue( transformNode, F_RESULT_FILE_TARGET_TRANSFORM );
      resultFilesFileNameField = XMLHandler.getTagValue( transformNode, "result_files_file_name_field" );
      executorsOutputTransform = XMLHandler.getTagValue( transformNode, F_EXECUTOR_OUTPUT_TRANSFORM );
    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString( PKG, "PipelineExecutorMeta.Exception.ErrorLoadingPipelineExecutorDetailsFromXML" ), e );
    }
  }

  public void setDefault() {
    parameters = new PipelineExecutorParameters();
    parameters.setInheritingAllVariables( true );

    groupSize = "1";
    groupField = "";
    groupTime = "";

    executionTimeField = "ExecutionTime";
    executionResultField = "ExecutionResult";
    executionNrErrorsField = "ExecutionNrErrors";
    executionLinesReadField = "ExecutionLinesRead";
    executionLinesWrittenField = "ExecutionLinesWritten";
    executionLinesInputField = "ExecutionLinesInput";
    executionLinesOutputField = "ExecutionLinesOutput";
    executionLinesRejectedField = "ExecutionLinesRejected";
    executionLinesUpdatedField = "ExecutionLinesUpdated";
    executionLinesDeletedField = "ExecutionLinesDeleted";
    executionFilesRetrievedField = "ExecutionFilesRetrieved";
    executionExitStatusField = "ExecutionExitStatus";
    executionLogTextField = "ExecutionLogText";
    executionLogChannelIdField = "ExecutionLogChannelId";

    resultFilesFileNameField = "FileName";
  }

  void prepareExecutionResultsFields( RowMetaInterface row, TransformMeta nextTransform ) throws HopTransformException {
    if ( nextTransform != null && executionResultTargetTransformMeta != null ) {
      addFieldToRow( row, executionTimeField, ValueMetaInterface.TYPE_INTEGER, 15, 0 );
      addFieldToRow( row, executionResultField, ValueMetaInterface.TYPE_BOOLEAN );
      addFieldToRow( row, executionNrErrorsField, ValueMetaInterface.TYPE_INTEGER, 9, 0 );
      addFieldToRow( row, executionLinesReadField, ValueMetaInterface.TYPE_INTEGER, 9, 0 );
      addFieldToRow( row, executionLinesWrittenField, ValueMetaInterface.TYPE_INTEGER, 9, 0 );
      addFieldToRow( row, executionLinesInputField, ValueMetaInterface.TYPE_INTEGER, 9, 0 );
      addFieldToRow( row, executionLinesOutputField, ValueMetaInterface.TYPE_INTEGER, 9, 0 );
      addFieldToRow( row, executionLinesRejectedField, ValueMetaInterface.TYPE_INTEGER, 9, 0 );
      addFieldToRow( row, executionLinesUpdatedField, ValueMetaInterface.TYPE_INTEGER, 9, 0 );
      addFieldToRow( row, executionLinesDeletedField, ValueMetaInterface.TYPE_INTEGER, 9, 0 );
      addFieldToRow( row, executionFilesRetrievedField, ValueMetaInterface.TYPE_INTEGER, 9, 0 );
      addFieldToRow( row, executionExitStatusField, ValueMetaInterface.TYPE_INTEGER, 3, 0 );
      addFieldToRow( row, executionLogTextField, ValueMetaInterface.TYPE_STRING );
      addFieldToRow( row, executionLogChannelIdField, ValueMetaInterface.TYPE_STRING, 50, 0 );

    }
  }

  protected void addFieldToRow( RowMetaInterface row, String fieldName, int type ) throws HopTransformException {
    addFieldToRow( row, fieldName, type, -1, -1 );
  }

  protected void addFieldToRow( RowMetaInterface row, String fieldName, int type, int length, int precision )
    throws HopTransformException {
    if ( !Utils.isEmpty( fieldName ) ) {
      try {
        ValueMetaInterface value = ValueMetaFactory.createValueMeta( fieldName, type, length, precision );
        value.setOrigin( getParentTransformMeta().getName() );
        row.addValueMeta( value );
      } catch ( HopPluginException e ) {
        throw new HopTransformException( BaseMessages.getString( PKG, "PipelineExecutorMeta.ValueMetaInterfaceCreation",
          fieldName ), e );
      }
    }
  }

  void prepareExecutionResultsFileFields( RowMetaInterface row, TransformMeta nextTransform ) throws HopTransformException {
    if ( nextTransform != null && resultFilesTargetTransformMeta != null && nextTransform.equals( resultFilesTargetTransformMeta ) ) {
      addFieldToRow( row, resultFilesFileNameField, ValueMetaInterface.TYPE_STRING );
    }
  }

  void prepareResultsRowsFields( RowMetaInterface row ) throws HopTransformException {
    for ( int i = 0; i < outputRowsField.length; i++ ) {
      addFieldToRow( row, outputRowsField[ i ], outputRowsType[ i ], outputRowsLength[ i ], outputRowsPrecision[ i ] );
    }
  }

  @Override
  public void getFields( RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, TransformMeta nextTransform,
                         VariableSpace space, IMetaStore metaStore ) throws HopTransformException {
    if ( nextTransform != null ) {
      if ( nextTransform.equals( executionResultTargetTransformMeta ) ) {
        inputRowMeta.clear();
        prepareExecutionResultsFields( inputRowMeta, nextTransform );
      } else if ( nextTransform.equals( resultFilesTargetTransformMeta ) ) {
        inputRowMeta.clear();
        prepareExecutionResultsFileFields( inputRowMeta, nextTransform );
      } else if ( nextTransform.equals( outputRowsSourceTransformMeta ) ) {
        inputRowMeta.clear();
        prepareResultsRowsFields( inputRowMeta );
      }
      // else don't call clear on inputRowMeta, it's the main output and should mimic the input
    }
  }

  public String[] getInfoTransforms() {
    String[] infoTransforms = getTransformIOMeta().getInfoTransformNames();
    // Return null instead of empty array to preserve existing behavior
    return infoTransforms.length == 0 ? null : infoTransforms;
  }

  @Deprecated
  public static synchronized PipelineMeta loadPipelineMeta( PipelineExecutorMeta executorMeta,
                                                         VariableSpace space ) throws HopException {
    return loadMappingMeta( executorMeta, null, space );
  }


  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transforminfo, RowMetaInterface prev,
                     String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                     IMetaStore metaStore ) {
    CheckResult cr;
    if ( prev == null || prev.size() == 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_WARNING, BaseMessages.getString( PKG,
          "PipelineExecutorMeta.CheckResult.NotReceivingAnyFields" ), transforminfo );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG,
          "PipelineExecutorMeta.CheckResult.TransformReceivingFields", prev.size() + "" ), transforminfo );
      remarks.add( cr );
    }

    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG,
          "PipelineExecutorMeta.CheckResult.TransformReceivingFieldsFromOtherTransforms" ), transforminfo );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString( PKG,
          "PipelineExecutorMeta.CheckResult.NoInputReceived" ), transforminfo );
      remarks.add( cr );
    }
  }

  public TransformInterface createTransform( TransformMeta transformMeta, PipelineExecutorData transformDataInterface, int cnr, PipelineMeta tr,
                                             Pipeline pipeline ) {
    return new PipelineExecutor( transformMeta, transformDataInterface, cnr, tr, pipeline );
  }

  @Override
  public List<ResourceReference> getResourceDependencies( PipelineMeta pipelineMeta, TransformMeta transformInfo ) {
    List<ResourceReference> references = new ArrayList<ResourceReference>( 5 );
    String realFilename = pipelineMeta.environmentSubstitute( fileName );
    ResourceReference reference = new ResourceReference( transformInfo );

    if ( StringUtils.isNotEmpty( realFilename ) ) {
      // Add the filename to the references, including a reference to this transform
      // meta data.
      //
      reference.getEntries().add( new ResourceEntry( realFilename, ResourceType.ACTIONFILE ) );
    }
    references.add( reference );
    return references;
  }

  public PipelineExecutorData getTransformData() {
    return new PipelineExecutorData();
  }

  @Override
  public TransformIOMetaInterface getTransformIOMeta() {
    TransformIOMetaInterface ioMeta = super.getTransformIOMeta( false );
    if ( ioMeta == null ) {

      ioMeta = new TransformIOMeta( true, true, true, false, true, false );

      ioMeta.addStream( new Stream( StreamType.TARGET, executionResultTargetTransformMeta, BaseMessages.getString( PKG,
        "PipelineExecutorMeta.ResultStream.Description" ), StreamIcon.TARGET, null ) );
      ioMeta.addStream( new Stream( StreamType.TARGET, outputRowsSourceTransformMeta, BaseMessages.getString( PKG,
        "PipelineExecutorMeta.ResultRowsStream.Description" ), StreamIcon.TARGET, null ) );
      ioMeta.addStream( new Stream( StreamType.TARGET, resultFilesTargetTransformMeta, BaseMessages.getString( PKG,
        "PipelineExecutorMeta.ResultFilesStream.Description" ), StreamIcon.TARGET, null ) );
      ioMeta.addStream( new Stream( StreamType.TARGET, executorsOutputTransformMeta, BaseMessages.getString( PKG,
        "PipelineExecutorMeta.ExecutorOutputStream.Description" ), StreamIcon.OUTPUT, null ) );
      setTransformIOMeta( ioMeta );
    }
    return ioMeta;
  }

  /**
   * When an optional stream is selected, this method is called to handled the ETL metadata implications of that.
   *
   * @param stream The optional stream to handle.
   */
  public void handleStreamSelection( StreamInterface stream ) {
    // This transform targets another transform.
    // Make sure that we don't specify the same transform for more than 1 target...
    List<StreamInterface> targets = getTransformIOMeta().getTargetStreams();
    int index = targets.indexOf( stream );
    TransformMeta transform = targets.get( index ).getTransformMeta();
    switch ( index ) {
      case 0:
        setExecutionResultTargetTransformMeta( transform );
        break;
      case 1:
        setOutputRowsSourceTransformMeta( transform );
        break;
      case 2:
        setResultFilesTargetTransformMeta( transform );
        break;
      case 3:
        setExecutorsOutputTransformMeta( transform );
      default:
        break;
    }

  }

  /**
   * Remove the cached {@link TransformIOMeta} so it is recreated when it is next accessed.
   */
  public void resetTransformIoMeta() {
  }

  @Override
  public void searchInfoAndTargetTransforms( List<TransformMeta> transforms ) {
    executionResultTargetTransformMeta = TransformMeta.findTransform( transforms, executionResultTargetTransform );
    outputRowsSourceTransformMeta = TransformMeta.findTransform( transforms, outputRowsSourceTransform );
    resultFilesTargetTransformMeta = TransformMeta.findTransform( transforms, resultFilesTargetTransform );
    executorsOutputTransformMeta = TransformMeta.findTransform( transforms, executorsOutputTransform );
  }

  public PipelineType[] getSupportedPipelineTypes() {
    return new PipelineType[] { PipelineType.Normal, };
  }

  /**
   * @return the mappingParameters
   */
  public PipelineExecutorParameters getMappingParameters() {
    return parameters;
  }

  /**
   * @param mappingParameters the mappingParameters to set
   */
  public void setMappingParameters( PipelineExecutorParameters mappingParameters ) {
    this.parameters = mappingParameters;
  }

  /**
   * @return the parameters
   */
  public PipelineExecutorParameters getParameters() {
    return parameters;
  }

  /**
   * @param parameters the parameters to set
   */
  public void setParameters( PipelineExecutorParameters parameters ) {
    this.parameters = parameters;
  }

  /**
   * @return the executionTimeField
   */
  public String getExecutionTimeField() {
    return executionTimeField;
  }

  /**
   * @param executionTimeField the executionTimeField to set
   */
  public void setExecutionTimeField( String executionTimeField ) {
    this.executionTimeField = executionTimeField;
  }

  /**
   * @return the executionResultField
   */
  public String getExecutionResultField() {
    return executionResultField;
  }

  /**
   * @param executionResultField the executionResultField to set
   */
  public void setExecutionResultField( String executionResultField ) {
    this.executionResultField = executionResultField;
  }

  /**
   * @return the executionNrErrorsField
   */
  public String getExecutionNrErrorsField() {
    return executionNrErrorsField;
  }

  /**
   * @param executionNrErrorsField the executionNrErrorsField to set
   */
  public void setExecutionNrErrorsField( String executionNrErrorsField ) {
    this.executionNrErrorsField = executionNrErrorsField;
  }

  /**
   * @return the executionLinesReadField
   */
  public String getExecutionLinesReadField() {
    return executionLinesReadField;
  }

  /**
   * @param executionLinesReadField the executionLinesReadField to set
   */
  public void setExecutionLinesReadField( String executionLinesReadField ) {
    this.executionLinesReadField = executionLinesReadField;
  }

  /**
   * @return the executionLinesWrittenField
   */
  public String getExecutionLinesWrittenField() {
    return executionLinesWrittenField;
  }

  /**
   * @param executionLinesWrittenField the executionLinesWrittenField to set
   */
  public void setExecutionLinesWrittenField( String executionLinesWrittenField ) {
    this.executionLinesWrittenField = executionLinesWrittenField;
  }

  /**
   * @return the executionLinesInputField
   */
  public String getExecutionLinesInputField() {
    return executionLinesInputField;
  }

  /**
   * @param executionLinesInputField the executionLinesInputField to set
   */
  public void setExecutionLinesInputField( String executionLinesInputField ) {
    this.executionLinesInputField = executionLinesInputField;
  }

  /**
   * @return the executionLinesOutputField
   */
  public String getExecutionLinesOutputField() {
    return executionLinesOutputField;
  }

  /**
   * @param executionLinesOutputField the executionLinesOutputField to set
   */
  public void setExecutionLinesOutputField( String executionLinesOutputField ) {
    this.executionLinesOutputField = executionLinesOutputField;
  }

  /**
   * @return the executionLinesRejectedField
   */
  public String getExecutionLinesRejectedField() {
    return executionLinesRejectedField;
  }

  /**
   * @param executionLinesRejectedField the executionLinesRejectedField to set
   */
  public void setExecutionLinesRejectedField( String executionLinesRejectedField ) {
    this.executionLinesRejectedField = executionLinesRejectedField;
  }

  /**
   * @return the executionLinesUpdatedField
   */
  public String getExecutionLinesUpdatedField() {
    return executionLinesUpdatedField;
  }

  /**
   * @param executionLinesUpdatedField the executionLinesUpdatedField to set
   */
  public void setExecutionLinesUpdatedField( String executionLinesUpdatedField ) {
    this.executionLinesUpdatedField = executionLinesUpdatedField;
  }

  /**
   * @return the executionLinesDeletedField
   */
  public String getExecutionLinesDeletedField() {
    return executionLinesDeletedField;
  }

  /**
   * @param executionLinesDeletedField the executionLinesDeletedField to set
   */
  public void setExecutionLinesDeletedField( String executionLinesDeletedField ) {
    this.executionLinesDeletedField = executionLinesDeletedField;
  }

  /**
   * @return the executionFilesRetrievedField
   */
  public String getExecutionFilesRetrievedField() {
    return executionFilesRetrievedField;
  }

  /**
   * @param executionFilesRetrievedField the executionFilesRetrievedField to set
   */
  public void setExecutionFilesRetrievedField( String executionFilesRetrievedField ) {
    this.executionFilesRetrievedField = executionFilesRetrievedField;
  }

  /**
   * @return the executionExitStatusField
   */
  public String getExecutionExitStatusField() {
    return executionExitStatusField;
  }

  /**
   * @param executionExitStatusField the executionExitStatusField to set
   */
  public void setExecutionExitStatusField( String executionExitStatusField ) {
    this.executionExitStatusField = executionExitStatusField;
  }

  /**
   * @return the executionLogTextField
   */
  public String getExecutionLogTextField() {
    return executionLogTextField;
  }

  /**
   * @param executionLogTextField the executionLogTextField to set
   */
  public void setExecutionLogTextField( String executionLogTextField ) {
    this.executionLogTextField = executionLogTextField;
  }

  /**
   * @return the executionLogChannelIdField
   */
  public String getExecutionLogChannelIdField() {
    return executionLogChannelIdField;
  }

  /**
   * @param executionLogChannelIdField the executionLogChannelIdField to set
   */
  public void setExecutionLogChannelIdField( String executionLogChannelIdField ) {
    this.executionLogChannelIdField = executionLogChannelIdField;
  }

  /**
   * @return the groupSize
   */
  public String getGroupSize() {
    return groupSize;
  }

  /**
   * @param groupSize the groupSize to set
   */
  public void setGroupSize( String groupSize ) {
    this.groupSize = groupSize;
  }

  /**
   * @return the groupField
   */
  public String getGroupField() {
    return groupField;
  }

  /**
   * @param groupField the groupField to set
   */
  public void setGroupField( String groupField ) {
    this.groupField = groupField;
  }

  /**
   * @return the groupTime
   */
  public String getGroupTime() {
    return groupTime;
  }

  /**
   * @param groupTime the groupTime to set
   */
  public void setGroupTime( String groupTime ) {
    this.groupTime = groupTime;
  }

  @Override
  public boolean excludeFromCopyDistributeVerification() {
    return true;
  }

  /**
   * @return the executionResultTargetTransform
   */
  public String getExecutionResultTargetTransform() {
    return executionResultTargetTransform;
  }

  /**
   * @param executionResultTargetTransform the executionResultTargetTransform to set
   */
  public void setExecutionResultTargetTransform( String executionResultTargetTransform ) {
    this.executionResultTargetTransform = executionResultTargetTransform;
  }

  /**
   * @return the executionResultTargetTransformMeta
   */
  public TransformMeta getExecutionResultTargetTransformMeta() {
    return executionResultTargetTransformMeta;
  }

  /**
   * @param executionResultTargetTransformMeta the executionResultTargetTransformMeta to set
   */
  public void setExecutionResultTargetTransformMeta( TransformMeta executionResultTargetTransformMeta ) {
    this.executionResultTargetTransformMeta = executionResultTargetTransformMeta;
  }

  /**
   * @return the resultFilesFileNameField
   */
  public String getResultFilesFileNameField() {
    return resultFilesFileNameField;
  }

  /**
   * @param resultFilesFileNameField the resultFilesFileNameField to set
   */
  public void setResultFilesFileNameField( String resultFilesFileNameField ) {
    this.resultFilesFileNameField = resultFilesFileNameField;
  }

  /**
   * @return The objects referenced in the transform, like a mapping, a pipeline, ...
   */
  public String[] getReferencedObjectDescriptions() {
    return new String[] { BaseMessages.getString( PKG, "PipelineExecutorMeta.ReferencedObject.Description" ), };
  }

  private boolean isPipelineDefined() {
    return StringUtils.isNotEmpty( fileName );
  }

  public boolean[] isReferencedObjectEnabled() {
    return new boolean[] { isPipelineDefined(), };
  }

  /**
   * Load the referenced object
   *
   * @param index the object index to load
   * @param space the variable space to use
   * @return the referenced object once loaded
   * @throws HopException
   */
  public Object loadReferencedObject( int index, IMetaStore metaStore, VariableSpace space )
    throws HopException {
    return loadMappingMeta( this, metaStore, space );
  }

  public IMetaStore getMetaStore() {
    return metaStore;
  }

  public void setMetaStore( IMetaStore metaStore ) {
    this.metaStore = metaStore;
  }

  public String getOutputRowsSourceTransform() {
    return outputRowsSourceTransform;
  }

  public void setOutputRowsSourceTransform( String outputRowsSourceTransform ) {
    this.outputRowsSourceTransform = outputRowsSourceTransform;
  }

  public TransformMeta getOutputRowsSourceTransformMeta() {
    return outputRowsSourceTransformMeta;
  }

  public void setOutputRowsSourceTransformMeta( TransformMeta outputRowsSourceTransformMeta ) {
    this.outputRowsSourceTransformMeta = outputRowsSourceTransformMeta;
  }

  public String[] getOutputRowsField() {
    return outputRowsField;
  }

  public void setOutputRowsField( String[] outputRowsField ) {
    this.outputRowsField = outputRowsField;
  }

  public int[] getOutputRowsType() {
    return outputRowsType;
  }

  public void setOutputRowsType( int[] outputRowsType ) {
    this.outputRowsType = outputRowsType;
  }

  public int[] getOutputRowsLength() {
    return outputRowsLength;
  }

  public void setOutputRowsLength( int[] outputRowsLength ) {
    this.outputRowsLength = outputRowsLength;
  }

  public int[] getOutputRowsPrecision() {
    return outputRowsPrecision;
  }

  public void setOutputRowsPrecision( int[] outputRowsPrecision ) {
    this.outputRowsPrecision = outputRowsPrecision;
  }

  public String getResultFilesTargetTransform() {
    return resultFilesTargetTransform;
  }

  public void setResultFilesTargetTransform( String resultFilesTargetTransform ) {
    this.resultFilesTargetTransform = resultFilesTargetTransform;
  }

  public TransformMeta getResultFilesTargetTransformMeta() {
    return resultFilesTargetTransformMeta;
  }

  public void setResultFilesTargetTransformMeta( TransformMeta resultFilesTargetTransformMeta ) {
    this.resultFilesTargetTransformMeta = resultFilesTargetTransformMeta;
  }

  public String getExecutorsOutputTransform() {
    return executorsOutputTransform;
  }

  public void setExecutorsOutputTransform( String executorsOutputTransform ) {
    this.executorsOutputTransform = executorsOutputTransform;
  }

  public TransformMeta getExecutorsOutputTransformMeta() {
    return executorsOutputTransformMeta;
  }

  public void setExecutorsOutputTransformMeta( TransformMeta executorsOutputTransformMeta ) {
    this.executorsOutputTransformMeta = executorsOutputTransformMeta;
  }

  @Override
  public boolean cleanAfterHopFromRemove() {

    setExecutionResultTargetTransformMeta( null );
    setOutputRowsSourceTransformMeta( null );
    setResultFilesTargetTransformMeta( null );
    setExecutorsOutputTransformMeta( null );
    return true;

  }

  @Override
  public boolean cleanAfterHopFromRemove( TransformMeta toTransform ) {
    if ( null == toTransform || null == toTransform.getName() ) {
      return false;
    }

    boolean hasChanged = false;
    String toTransformName = toTransform.getName();

    if ( getExecutionResultTargetTransformMeta() != null
      && toTransformName.equals( getExecutionResultTargetTransformMeta().getName() ) ) {
      setExecutionResultTargetTransformMeta( null );
      hasChanged = true;
    } else if ( getOutputRowsSourceTransformMeta() != null
      && toTransformName.equals( getOutputRowsSourceTransformMeta().getName() ) ) {
      setOutputRowsSourceTransformMeta( null );
      hasChanged = true;
    } else if ( getResultFilesTargetTransformMeta() != null
      && toTransformName.equals( getResultFilesTargetTransformMeta().getName() ) ) {
      setResultFilesTargetTransformMeta( null );
      hasChanged = true;
    } else if ( getExecutorsOutputTransformMeta() != null
      && toTransformName.equals( getExecutorsOutputTransformMeta().getName() ) ) {
      setExecutorsOutputTransformMeta( null );
      hasChanged = true;
    }
    return hasChanged;
  }
}
