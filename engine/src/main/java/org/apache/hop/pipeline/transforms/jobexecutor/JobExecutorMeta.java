/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.jobexecutor;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNone;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.CurrentDirectoryResolver;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.JobMeta;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.TransformWithMappingMeta;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInterface;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceNamingInterface;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineMeta.PipelineType;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformIOMetaInterface;
import org.apache.hop.pipeline.transform.TransformInterface;
import org.apache.hop.pipeline.transform.errorhandling.Stream;
import org.apache.hop.pipeline.transform.errorhandling.StreamIcon;
import org.apache.hop.pipeline.transform.errorhandling.StreamInterface;
import org.apache.hop.pipeline.transform.errorhandling.StreamInterface.StreamType;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Meta-data for the Job executor transform.
 *
 * @author Matt
 * @since 29-AUG-2011
 */

public class JobExecutorMeta extends BaseTransformMeta implements TransformMetaInterface<JobExecutor, JobExecutorData> {

  private static Class<?> PKG = JobExecutorMeta.class; // for i18n purposes, needed by Translator!!
  private String fileName;

  /**
   * The number of input rows that are sent as result rows to the job in one go, defaults to "1"
   */
  private String groupSize;

  /**
   * Optional name of a field to group rows together that are sent together to the job
   * as result rows (empty default)
   */
  private String groupField;

  /**
   * Optional time in ms that is spent waiting and accumulating rows before they are given to the job as result rows
   * (empty default, "0")
   */
  private String groupTime;

  private JobExecutorParameters parameters;

  private String executionResultTargetTransform;
  private TransformMeta executionResultTargetTransformMeta;

  /**
   * The optional name of the output field that will contain the execution time of the job in (Integer in ms)
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
   * The optional name of the output field that will contain the log text of the job execution (String)
   */
  private String executionLogTextField;

  /**
   * The optional name of the output field that will contain the log channel ID of the job execution (String)
   */
  private String executionLogChannelIdField;

  /**
   * The optional transform to send the result rows to
   */
  private String resultRowsTargetTransform;
  private TransformMeta resultRowsTargetTransformMeta;
  private String[] resultRowsField;
  private int[] resultRowsType;
  private int[] resultRowsLength;
  private int[] resultRowsPrecision;

  /**
   * The optional transform to send the result files to
   */
  private String resultFilesTargetTransform;
  private TransformMeta resultFilesTargetTransformMeta;
  private String resultFilesFileNameField;

  private IMetaStore metaStore;

  public JobExecutorMeta() {
    super(); // allocate BaseTransformMeta

    parameters = new JobExecutorParameters();
    resultRowsField = new String[ 0 ];
  }

  @Override
  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder( 300 );

    // Export a little bit of extra information regarding the reference
    //
    retval.append( "    " ).append( XMLHandler.addTagValue( "filename", fileName ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "group_size", groupSize ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "group_field", groupField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "group_time", groupTime ) );

    // Add the mapping parameters too
    //
    retval.append( parameters.getXML() );

    // The output side...
    //
    retval.append( "    " ).append(
      XMLHandler.addTagValue( "execution_result_target_transform", executionResultTargetTransformMeta == null
        ? null : executionResultTargetTransformMeta.getName() ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "execution_time_field", executionTimeField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "execution_result_field", executionResultField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "execution_errors_field", executionNrErrorsField ) );
    retval
      .append( "    " ).append( XMLHandler.addTagValue( "execution_lines_read_field", executionLinesReadField ) );
    retval.append( "    " ).append(
      XMLHandler.addTagValue( "execution_lines_written_field", executionLinesWrittenField ) );
    retval.append( "    " ).append(
      XMLHandler.addTagValue( "execution_lines_input_field", executionLinesInputField ) );
    retval.append( "    " ).append(
      XMLHandler.addTagValue( "execution_lines_output_field", executionLinesOutputField ) );
    retval.append( "    " ).append(
      XMLHandler.addTagValue( "execution_lines_rejected_field", executionLinesRejectedField ) );
    retval.append( "    " ).append(
      XMLHandler.addTagValue( "execution_lines_updated_field", executionLinesUpdatedField ) );
    retval.append( "    " ).append(
      XMLHandler.addTagValue( "execution_lines_deleted_field", executionLinesDeletedField ) );
    retval.append( "    " ).append(
      XMLHandler.addTagValue( "execution_files_retrieved_field", executionFilesRetrievedField ) );
    retval.append( "    " ).append(
      XMLHandler.addTagValue( "execution_exit_status_field", executionExitStatusField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "execution_log_text_field", executionLogTextField ) );
    retval.append( "    " ).append(
      XMLHandler.addTagValue( "execution_log_channelid_field", executionLogChannelIdField ) );

    retval.append( "    " ).append(
      XMLHandler.addTagValue( "result_rows_target_transform", resultRowsTargetTransformMeta == null
        ? null : resultRowsTargetTransformMeta.getName() ) );
    for ( int i = 0; i < resultRowsField.length; i++ ) {
      retval.append( "    " ).append( XMLHandler.openTag( "result_rows_field" ) ).append( Const.CR );
      retval.append( "      " ).append( XMLHandler.addTagValue( "name", resultRowsField[ i ] ) );
      retval.append( "      " ).append( XMLHandler.addTagValue( "type",
        ValueMetaFactory.getValueMetaName( resultRowsType[ i ] ) ) );
      retval.append( "      " ).append( XMLHandler.addTagValue( "length", resultRowsLength[ i ] ) );
      retval.append( "      " ).append( XMLHandler.addTagValue( "precision", resultRowsPrecision[ i ] ) );
      retval.append( "    " ).append( XMLHandler.closeTag( "result_rows_field" ) ).append( Const.CR );
    }

    retval.append( "    " ).append(
      XMLHandler.addTagValue( "result_files_target_transform", resultFilesTargetTransformMeta == null
        ? null : resultFilesTargetTransformMeta.getName() ) );
    retval.append( "    " ).append(
      XMLHandler.addTagValue( "result_files_file_name_field", resultFilesFileNameField ) );

    return retval.toString();
  }

  @Override
  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    try {
      fileName = XMLHandler.getTagValue( transformNode, "filename" );

      groupSize = XMLHandler.getTagValue( transformNode, "group_size" );
      groupField = XMLHandler.getTagValue( transformNode, "group_field" );
      groupTime = XMLHandler.getTagValue( transformNode, "group_time" );

      // Load the mapping parameters too..
      //
      Node mappingParametersNode = XMLHandler.getSubNode( transformNode, JobExecutorParameters.XML_TAG );
      parameters = new JobExecutorParameters( mappingParametersNode );

      // The output side...
      //
      executionResultTargetTransform = XMLHandler.getTagValue( transformNode, "execution_result_target_transform" );
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

      resultRowsTargetTransform = XMLHandler.getTagValue( transformNode, "result_rows_target_transform" );

      int nrFields = XMLHandler.countNodes( transformNode, "result_rows_field" );
      resultRowsField = new String[ nrFields ];
      resultRowsType = new int[ nrFields ];
      resultRowsLength = new int[ nrFields ];
      resultRowsPrecision = new int[ nrFields ];

      for ( int i = 0; i < nrFields; i++ ) {

        Node fieldNode = XMLHandler.getSubNodeByNr( transformNode, "result_rows_field", i );

        resultRowsField[ i ] = XMLHandler.getTagValue( fieldNode, "name" );
        resultRowsType[ i ] = ValueMetaFactory.getIdForValueMeta( XMLHandler.getTagValue( fieldNode, "type" ) );
        resultRowsLength[ i ] = Const.toInt( XMLHandler.getTagValue( fieldNode, "length" ), -1 );
        resultRowsPrecision[ i ] = Const.toInt( XMLHandler.getTagValue( fieldNode, "precision" ), -1 );
      }

      resultFilesTargetTransform = XMLHandler.getTagValue( transformNode, "result_files_target_transform" );
      resultFilesFileNameField = XMLHandler.getTagValue( transformNode, "result_files_file_name_field" );
    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString(
        PKG, "JobExecutorMeta.Exception.ErrorLoadingJobExecutorDetailsFromXML" ), e );
    }
  }

  @Override
  public void setDefault() {
    parameters = new JobExecutorParameters();
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

  @Override
  public void getFields( RowMetaInterface row, String origin, RowMetaInterface[] info, TransformMeta nextTransform,
                         VariableSpace space, IMetaStore metaStore ) throws HopTransformException {

    row.clear();

    if ( nextTransform != null && resultRowsTargetTransformMeta != null && nextTransform.equals( resultRowsTargetTransformMeta ) ) {
      for ( int i = 0; i < resultRowsField.length; i++ ) {
        ValueMetaInterface value;
        try {
          value = ValueMetaFactory.createValueMeta( resultRowsField[ i ], resultRowsType[ i ],
            resultRowsLength[ i ], resultRowsPrecision[ i ] );
        } catch ( HopPluginException e ) {
          value = new ValueMetaNone( resultRowsField[ i ] );
          value.setLength( resultRowsLength[ i ], resultRowsPrecision[ i ] );
        }
        row.addValueMeta( value );
      }
    } else if ( nextTransform != null
      && resultFilesTargetTransformMeta != null && nextTransform.equals( resultFilesTargetTransformMeta ) ) {
      if ( !Utils.isEmpty( resultFilesFileNameField ) ) {
        ValueMetaInterface value = new ValueMetaString( "filename", 255, 0 );
        row.addValueMeta( value );
      }
    } else if ( nextTransform != null
      && executionResultTargetTransformMeta != null && nextTransform.equals( executionResultTargetTransformMeta ) ) {
      if ( !Utils.isEmpty( executionTimeField ) ) {
        ValueMetaInterface value = new ValueMetaInteger( executionTimeField, 15, 0 );
        row.addValueMeta( value );
      }
      if ( !Utils.isEmpty( executionResultField ) ) {
        ValueMetaInterface value = new ValueMetaBoolean( executionResultField );
        row.addValueMeta( value );
      }
      if ( !Utils.isEmpty( executionNrErrorsField ) ) {
        ValueMetaInterface value = new ValueMetaInteger( executionNrErrorsField, 9, 0 );
        row.addValueMeta( value );
      }
      if ( !Utils.isEmpty( executionLinesReadField ) ) {
        ValueMetaInterface value = new ValueMetaInteger( executionLinesReadField, 9, 0 );
        row.addValueMeta( value );
      }
      if ( !Utils.isEmpty( executionLinesWrittenField ) ) {
        ValueMetaInterface value = new ValueMetaInteger( executionLinesWrittenField, 9, 0 );
        row.addValueMeta( value );
      }
      if ( !Utils.isEmpty( executionLinesInputField ) ) {
        ValueMetaInterface value = new ValueMetaInteger( executionLinesInputField, 9, 0 );
        row.addValueMeta( value );
      }
      if ( !Utils.isEmpty( executionLinesOutputField ) ) {
        ValueMetaInterface value = new ValueMetaInteger( executionLinesOutputField, 9, 0 );
        row.addValueMeta( value );
      }
      if ( !Utils.isEmpty( executionLinesRejectedField ) ) {
        ValueMetaInterface value = new ValueMetaInteger( executionLinesRejectedField, 9, 0 );
        row.addValueMeta( value );
      }
      if ( !Utils.isEmpty( executionLinesUpdatedField ) ) {
        ValueMetaInterface value = new ValueMetaInteger( executionLinesUpdatedField, 9, 0 );
        row.addValueMeta( value );
      }
      if ( !Utils.isEmpty( executionLinesDeletedField ) ) {
        ValueMetaInterface value = new ValueMetaInteger( executionLinesDeletedField, 9, 0 );
        row.addValueMeta( value );
      }
      if ( !Utils.isEmpty( executionFilesRetrievedField ) ) {
        ValueMetaInterface value = new ValueMetaInteger( executionFilesRetrievedField, 9, 0 );
        row.addValueMeta( value );
      }
      if ( !Utils.isEmpty( executionExitStatusField ) ) {
        ValueMetaInterface value = new ValueMetaInteger( executionExitStatusField, 3, 0 );
        row.addValueMeta( value );
      }
      if ( !Utils.isEmpty( executionLogTextField ) ) {
        ValueMetaInterface value = new ValueMetaString( executionLogTextField );
        value.setLargeTextField( true );
        row.addValueMeta( value );
      }
      if ( !Utils.isEmpty( executionLogChannelIdField ) ) {
        ValueMetaInterface value = new ValueMetaString( executionLogChannelIdField, 50, 0 );
        row.addValueMeta( value );
      }
    }
  }

  public String[] getInfoTransforms() {
    String[] infoTransforms = getTransformIOMeta().getInfoTransformNames();
    // Return null instead of empty array to preserve existing behavior
    return infoTransforms.length == 0 ? null : infoTransforms;
  }

  public String[] getTargetTransforms() {

    List<String> targetTransforms = new ArrayList<>();

    if ( !Utils.isEmpty( resultFilesTargetTransform ) ) {
      targetTransforms.add( resultFilesTargetTransform );
    }
    if ( !Utils.isEmpty( resultRowsTargetTransform ) ) {
      targetTransforms.add( resultRowsTargetTransform );
    }

    if ( targetTransforms.isEmpty() ) {
      return null;
    }

    return targetTransforms.toArray( new String[ targetTransforms.size() ] );
  }

  public static final synchronized JobMeta loadJobMeta( JobExecutorMeta executorMeta, VariableSpace space ) throws HopException {
    return loadJobMeta( executorMeta, null, space );
  }

  public static final synchronized JobMeta loadJobMeta( JobExecutorMeta executorMeta, IMetaStore metaStore, VariableSpace space ) throws HopException {
    JobMeta mappingJobMeta = null;

    CurrentDirectoryResolver r = new CurrentDirectoryResolver();
    VariableSpace tmpSpace = r.resolveCurrentDirectory( space, executorMeta.getParentTransformMeta(), executorMeta.getFileName() );

    String realFilename = tmpSpace.environmentSubstitute( executorMeta.getFileName() );

    // OK, load the meta-data from file...
    //
    // Don't set internal variables: they belong to the parent thread!
    //
    mappingJobMeta = new JobMeta( null, realFilename, metaStore );
    LogChannel.GENERAL.logDetailed( "Loaded job", "Job was loaded from XML file ["
      + realFilename + "]" );

    // Pass some important information to the mapping pipeline metadata:

    //  When the child parameter does exist in the parent parameters, overwrite the child parameter by the
    // parent parameter.
    TransformWithMappingMeta.replaceVariableValues( mappingJobMeta, space, "Job" );
    if ( executorMeta.getParameters().isInheritingAllVariables() ) {
      // All other parent parameters need to get copied into the child parameters  (when the 'Inherit all
      // variables from the pipeline?' option is checked)
      TransformWithMappingMeta.addMissingVariables( mappingJobMeta, space );
    }
    mappingJobMeta.setMetaStore( metaStore );
    mappingJobMeta.setFilename( mappingJobMeta.getFilename() );

    return mappingJobMeta;
  }

  @Override
  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                     IMetaStore metaStore ) {
    CheckResult cr;
    if ( prev == null || prev.size() == 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_WARNING, BaseMessages.getString(
          PKG, "JobExecutorMeta.CheckResult.NotReceivingAnyFields" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "JobExecutorMeta.CheckResult.TransformReceivingFields", prev.size() + "" ), transformMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "JobExecutorMeta.CheckResult.TransformReceivingFieldsFromOtherTransforms" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "JobExecutorMeta.CheckResult.NoInputReceived" ), transformMeta );
      remarks.add( cr );
    }
  }

  @Override
  public TransformInterface createTransform( TransformMeta transformMeta, JobExecutorData transformDataInterface, int cnr, PipelineMeta tr,
                                             Pipeline pipeline ) {
    return new JobExecutor( transformMeta, transformDataInterface, cnr, tr, pipeline );
  }

  @Override
  public List<ResourceReference> getResourceDependencies( PipelineMeta pipelineMeta, TransformMeta transformInfo ) {
    List<ResourceReference> references = new ArrayList<ResourceReference>( 5 );
    String realFilename = pipelineMeta.environmentSubstitute( fileName );
    ResourceReference reference = new ResourceReference( transformInfo );
    references.add( reference );

    if ( StringUtils.isNotEmpty( realFilename ) ) {
      // Add the filename to the references, including a reference to this transform
      // meta data.
      //
      reference.getEntries().add( new ResourceEntry( realFilename, ResourceType.ACTIONFILE ) );
    }
    return references;
  }

  /**
   * This method was created exclusively for tests
   * to bypass the mock static final method
   * without using PowerMock
   *
   * @param executorMeta
   * @param space
   * @return JobMeta
   * @throws HopException
   */
  JobMeta loadJobMetaProxy( JobExecutorMeta executorMeta,
                            VariableSpace space ) throws HopException {
    return loadJobMeta( executorMeta, space );
  }

  @Override
  public String exportResources( VariableSpace space, Map<String, ResourceDefinition> definitions,
                                 ResourceNamingInterface resourceNamingInterface, IMetaStore metaStore ) throws HopException {
    try {
      // Try to load the pipeline from a file.
      // Modify this recursively too...
      //
      // NOTE: there is no need to clone this transform because the caller is
      // responsible for this.
      //
      // First load the executor job metadata...
      //
      JobMeta executorJobMeta = loadJobMetaProxy( this, space );

      // Also go down into the mapping pipeline and export the files
      // there. (mapping recursively down)
      //
      String proposedNewFilename =
        executorJobMeta.exportResources(
          executorJobMeta, definitions, resourceNamingInterface, metaStore );

      // To get a relative path to it, we inject
      // ${Internal.Entry.Current.Directory}
      //
      String newFilename =
        "${" + Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY + "}/" + proposedNewFilename;

      // Set the correct filename inside the XML.
      //
      executorJobMeta.setFilename( newFilename );

      // change it in the job entry
      //
      fileName = newFilename;

      return proposedNewFilename;
    } catch ( Exception e ) {
      throw new HopException( BaseMessages.getString(
        PKG, "JobExecutorMeta.Exception.UnableToLoadJob", fileName ) );
    }
  }

  @Override
  public JobExecutorData getTransformData() {
    return new JobExecutorData();
  }

  @Override
  public TransformIOMetaInterface getTransformIOMeta() {
    TransformIOMetaInterface ioMeta = super.getTransformIOMeta( false );
    if ( ioMeta == null ) {

      ioMeta = new TransformIOMeta( true, true, true, false, true, false );

      ioMeta.addStream( new Stream( StreamType.TARGET, executionResultTargetTransformMeta, BaseMessages.getString(
        PKG, "JobExecutorMeta.ResultStream.Description" ), StreamIcon.TARGET, null ) );
      ioMeta.addStream( new Stream( StreamType.TARGET, resultRowsTargetTransformMeta, BaseMessages.getString(
        PKG, "JobExecutorMeta.ResultRowsStream.Description" ), StreamIcon.TARGET, null ) );
      ioMeta.addStream( new Stream( StreamType.TARGET, resultFilesTargetTransformMeta, BaseMessages.getString(
        PKG, "JobExecutorMeta.ResultFilesStream.Description" ), StreamIcon.TARGET, null ) );
      setTransformIOMeta( ioMeta );
    }
    return ioMeta;
  }

  /**
   * When an optional stream is selected, this method is called to handled the ETL metadata implications of that.
   *
   * @param stream The optional stream to handle.
   */
  @Override
  public void handleStreamSelection( StreamInterface stream ) {
    // This transform targets another transform.
    // Make sure that we don't specify the same transform for more than 1 target...
    //
    List<StreamInterface> targets = getTransformIOMeta().getTargetStreams();
    int index = targets.indexOf( stream );
    TransformMeta transform = targets.get( index ).getTransformMeta();
    switch ( index ) {
      case 0:
        setExecutionResultTargetTransformMeta( transform );
        break;
      case 1:
        setResultRowsTargetTransformMeta( transform );
        break;
      case 2:
        setResultFilesTargetTransformMeta( transform );
        break;
      default:
        break;
    }

  }

  /**
   * Remove the cached {@link TransformIOMeta} so it is recreated when it is next accessed.
   */
  @Override
  public void resetTransformIoMeta() {
  }

  @Override
  public void searchInfoAndTargetTransforms( List<TransformMeta> transforms ) {
    executionResultTargetTransformMeta = TransformMeta.findTransform( transforms, executionResultTargetTransform );
    resultRowsTargetTransformMeta = TransformMeta.findTransform( transforms, resultRowsTargetTransform );
    resultFilesTargetTransformMeta = TransformMeta.findTransform( transforms, resultFilesTargetTransform );
  }

  @Override
  public PipelineType[] getSupportedPipelineTypes() {
    return new PipelineType[] { PipelineType.Normal, };
  }

  /**
   * @return the fileName
   */
  public String getFileName() {
    return fileName;
  }

  /**
   * @param fileName the fileName to set
   */
  public void setFileName( String fileName ) {
    this.fileName = fileName;
  }

  /**
   * @return the mappingParameters
   */
  public JobExecutorParameters getMappingParameters() {
    return parameters;
  }

  /**
   * @param mappingParameters the mappingParameters to set
   */
  public void setMappingParameters( JobExecutorParameters mappingParameters ) {
    this.parameters = mappingParameters;
  }

  /**
   * @return the parameters
   */
  public JobExecutorParameters getParameters() {
    return parameters;
  }

  /**
   * @param parameters the parameters to set
   */
  public void setParameters( JobExecutorParameters parameters ) {
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
   * @return the resultRowsTargetTransform
   */
  public String getResultRowsTargetTransform() {
    return resultRowsTargetTransform;
  }

  /**
   * @param resultRowsTargetTransform the result Rows Target Transform to set
   */
  public void setResultRowsTargetTransform( String resultRowsTargetTransform ) {
    this.resultRowsTargetTransform = resultRowsTargetTransform;
  }

  /**
   * @return the resultRowsField
   */
  public String[] getResultRowsField() {
    return resultRowsField;
  }

  /**
   * @param resultRowsField the resultRowsField to set
   */
  public void setResultRowsField( String[] resultRowsField ) {
    this.resultRowsField = resultRowsField;
  }

  /**
   * @return the resultRowsType
   */
  public int[] getResultRowsType() {
    return resultRowsType;
  }

  /**
   * @param resultRowsType the resultRowsType to set
   */
  public void setResultRowsType( int[] resultRowsType ) {
    this.resultRowsType = resultRowsType;
  }

  /**
   * @return the resultRowsLength
   */
  public int[] getResultRowsLength() {
    return resultRowsLength;
  }

  /**
   * @param resultRowsLength the resultRowsLength to set
   */
  public void setResultRowsLength( int[] resultRowsLength ) {
    this.resultRowsLength = resultRowsLength;
  }

  /**
   * @return the resultRowsPrecision
   */
  public int[] getResultRowsPrecision() {
    return resultRowsPrecision;
  }

  /**
   * @param resultRowsPrecision the resultRowsPrecision to set
   */
  public void setResultRowsPrecision( int[] resultRowsPrecision ) {
    this.resultRowsPrecision = resultRowsPrecision;
  }

  /**
   * @return the result Files Target Transform
   */
  public String getResultFilesTargetTransform() {
    return resultFilesTargetTransform;
  }

  /**
   * @param resultFilesTargetTransform the result Files Target Transform to set
   */
  public void setResultFilesTargetTransform( String resultFilesTargetTransform ) {
    this.resultFilesTargetTransform = resultFilesTargetTransform;
  }

  /**
   * @return the resultRowsTargetTransformMeta
   */
  public TransformMeta getResultRowsTargetTransformMeta() {
    return resultRowsTargetTransformMeta;
  }

  /**
   * @param resultRowsTargetTransformMeta the resultRowsTargetTransformMeta to set
   */
  public void setResultRowsTargetTransformMeta( TransformMeta resultRowsTargetTransformMeta ) {
    this.resultRowsTargetTransformMeta = resultRowsTargetTransformMeta;
  }

  /**
   * @return the resultFilesTargetTransformMeta
   */
  public TransformMeta getResultFilesTargetTransformMeta() {
    return resultFilesTargetTransformMeta;
  }

  /**
   * @param resultFilesTargetTransformMeta the resultFilesTargetTransformMeta to set
   */
  public void setResultFilesTargetTransformMeta( TransformMeta resultFilesTargetTransformMeta ) {
    this.resultFilesTargetTransformMeta = resultFilesTargetTransformMeta;
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
   * @return the execution Result Target Transform
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
   * @return The objects referenced in the transform, like a mapping, a pipeline, a job, ...
   */
  @Override
  public String[] getReferencedObjectDescriptions() {
    return new String[] { BaseMessages.getString( PKG, "JobExecutorMeta.ReferencedObject.Description" ), };
  }

  private boolean isJobDefined() {
    return StringUtils.isNotEmpty( fileName );
  }

  @Override
  public boolean[] isReferencedObjectEnabled() {
    return new boolean[] { isJobDefined(), };
  }

  /**
   * Load the referenced object
   *
   * @param index     the object index to load
   * @param metaStore the metaStore
   * @param space     the variable space to use
   * @return the referenced object once loaded
   * @throws HopException
   */
  @Override
  public Object loadReferencedObject( int index, IMetaStore metaStore, VariableSpace space ) throws HopException {
    return loadJobMeta( this, metaStore, space );
  }

  public void setMetaStore( IMetaStore metaStore ) {
    this.metaStore = metaStore;
  }

  public IMetaStore getMetaStore() {
    return metaStore;
  }

  @Override
  public boolean cleanAfterHopFromRemove() {
    setExecutionResultTargetTransformMeta( null );
    setResultRowsTargetTransformMeta( null );
    setResultFilesTargetTransformMeta( null );
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
    } else if ( getResultRowsTargetTransformMeta() != null
      && toTransformName.equals( getResultRowsTargetTransformMeta().getName() ) ) {
      setResultRowsTargetTransformMeta( null );
      hasChanged = true;
    } else if ( getResultFilesTargetTransformMeta() != null
      && toTransformName.equals( getResultFilesTargetTransformMeta().getName() ) ) {
      setResultFilesTargetTransformMeta( null );
      hasChanged = true;
    }
    return hasChanged;
  }
}
