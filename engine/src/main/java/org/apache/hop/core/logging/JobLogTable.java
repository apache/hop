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

package org.apache.hop.core.logging;

import java.util.ArrayList;
import java.util.List;

import org.apache.hop.core.Const;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.Job;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.trans.HasDatabasesInterface;
import org.apache.hop.trans.step.StepMeta;
import org.w3c.dom.Node;

/**
 * This class describes a job logging table
 *
 * @author matt
 *
 */
public class JobLogTable extends BaseLogTable implements Cloneable, LogTableInterface {

  private static Class<?> PKG = JobLogTable.class; // for i18n purposes, needed by Translator2!!

  public static final String XML_TAG = "job-log-table";

  public enum ID {

    ID_JOB( "ID_JOB" ), CHANNEL_ID( "CHANNEL_ID" ), JOBNAME( "JOBNAME" ), STATUS( "STATUS" ), LINES_READ(
      "LINES_READ" ), LINES_WRITTEN( "LINES_WRITTEN" ), LINES_UPDATED( "LINES_UPDATED" ), LINES_INPUT(
      "LINES_INPUT" ), LINES_OUTPUT( "LINES_OUTPUT" ), LINES_REJECTED( "LINES_REJECTED" ), ERRORS( "ERRORS" ),
      STARTDATE( "STARTDATE" ), ENDDATE( "ENDDATE" ), LOGDATE( "LOGDATE" ), DEPDATE( "DEPDATE" ), REPLAYDATE(
        "REPLAYDATE" ), LOG_FIELD( "LOG_FIELD" ), EXECUTING_SERVER( "EXECUTING_SERVER" ), EXECUTING_USER(
        "EXECUTING_USER" ), START_JOB_ENTRY( "START_JOB_ENTRY" ), CLIENT( "CLIENT" );

    private String id;

    private ID( String id ) {
      this.id = id;
    }

    public String toString() {
      return id;
    }
  }

  private String logInterval;

  private String logSizeLimit;

  private JobLogTable( VariableSpace space, IMetaStore metaStore ) {
    super( space, metaStore, null, null, null );
  }

  @Override
  public Object clone() {
    try {
      JobLogTable table = (JobLogTable) super.clone();
      table.fields = new ArrayList<LogTableField>();
      for ( LogTableField field : this.fields ) {
        table.fields.add( (LogTableField) field.clone() );
      }
      return table;
    } catch ( CloneNotSupportedException e ) {
      return null;
    }
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder();

    retval.append( "      " ).append( XMLHandler.openTag( XML_TAG ) ).append( Const.CR );
    retval.append( "        " ).append( XMLHandler.addTagValue( "connection", connectionName ) );
    retval.append( "        " ).append( XMLHandler.addTagValue( "schema", schemaName ) );
    retval.append( "        " ).append( XMLHandler.addTagValue( "table", tableName ) );
    retval.append( "        " ).append( XMLHandler.addTagValue( "size_limit_lines", logSizeLimit ) );
    retval.append( "        " ).append( XMLHandler.addTagValue( "interval", logInterval ) );
    retval.append( "        " ).append( XMLHandler.addTagValue( "timeout_days", timeoutInDays ) );
    retval.append( super.getFieldsXML() );
    retval.append( "      " ).append( XMLHandler.closeTag( XML_TAG ) ).append( Const.CR );

    return retval.toString();
  }

  public void loadXML( Node jobNode, List<StepMeta> steps ) {
    connectionName = XMLHandler.getTagValue( jobNode, "connection" );
    schemaName = XMLHandler.getTagValue( jobNode, "schema" );
    tableName = XMLHandler.getTagValue( jobNode, "table" );
    logSizeLimit = XMLHandler.getTagValue( jobNode, "size_limit_lines" );
    logInterval = XMLHandler.getTagValue( jobNode, "interval" );
    timeoutInDays = XMLHandler.getTagValue( jobNode, "timeout_days" );

    super.loadFieldsXML( jobNode );
  }

  @Override
  public void replaceMeta( LogTableCoreInterface logTableInterface ) {
    if ( !( logTableInterface instanceof JobLogTable ) ) {
      return;
    }

    JobLogTable logTable = (JobLogTable) logTableInterface;
    super.replaceMeta( logTable );
    logInterval = logTable.logInterval;
    logSizeLimit = logTable.logSizeLimit;
  }

  //CHECKSTYLE:LineLength:OFF
  public static JobLogTable getDefault( VariableSpace space, IMetaStore metaStore ) {
    JobLogTable table = new JobLogTable( space, metaStore );

    table.fields.add( new LogTableField( ID.ID_JOB.id, true, false, "ID_JOB", BaseMessages.getString( PKG, "JobLogTable.FieldName.BatchID" ), BaseMessages.getString( PKG, "JobLogTable.FieldDescription.BatchID" ), ValueMetaInterface.TYPE_INTEGER, 8 ) );
    table.fields.add( new LogTableField( ID.CHANNEL_ID.id, true, false, "CHANNEL_ID", BaseMessages.getString( PKG, "JobLogTable.FieldName.ChannelID" ), BaseMessages.getString( PKG, "JobLogTable.FieldDescription.ChannelID" ), ValueMetaInterface.TYPE_STRING, 255 ) );
    table.fields.add( new LogTableField( ID.JOBNAME.id, true, false, "JOBNAME", BaseMessages.getString( PKG, "JobLogTable.FieldName.JobName" ), BaseMessages.getString( PKG, "JobLogTable.FieldDescription.JobName" ), ValueMetaInterface.TYPE_STRING, 255 ) );
    table.fields.add( new LogTableField( ID.STATUS.id, true, false, "STATUS", BaseMessages.getString( PKG, "JobLogTable.FieldName.Status" ), BaseMessages.getString( PKG, "JobLogTable.FieldDescription.Status" ), ValueMetaInterface.TYPE_STRING, 15 ) );
    table.fields.add( new LogTableField( ID.LINES_READ.id, true, false, "LINES_READ", BaseMessages.getString( PKG, "JobLogTable.FieldName.LinesRead" ), BaseMessages.getString( PKG, "JobLogTable.FieldDescription.LinesRead" ), ValueMetaInterface.TYPE_INTEGER, 18 ) );
    table.fields.add( new LogTableField( ID.LINES_WRITTEN.id, true, false, "LINES_WRITTEN", BaseMessages.getString( PKG, "JobLogTable.FieldName.LinesWritten" ), BaseMessages.getString( PKG, "JobLogTable.FieldDescription.LinesWritten" ), ValueMetaInterface.TYPE_INTEGER, 18 ) );
    table.fields.add( new LogTableField( ID.LINES_UPDATED.id, true, false, "LINES_UPDATED", BaseMessages.getString( PKG, "JobLogTable.FieldName.LinesUpdated" ), BaseMessages.getString( PKG, "JobLogTable.FieldDescription.LinesUpdated" ), ValueMetaInterface.TYPE_INTEGER, 18 ) );
    table.fields.add( new LogTableField( ID.LINES_INPUT.id, true, false, "LINES_INPUT", BaseMessages.getString( PKG, "JobLogTable.FieldName.LinesInput" ), BaseMessages.getString( PKG, "JobLogTable.FieldDescription.LinesInput" ), ValueMetaInterface.TYPE_INTEGER, 18 ) );
    table.fields.add( new LogTableField( ID.LINES_OUTPUT.id, true, false, "LINES_OUTPUT", BaseMessages.getString( PKG, "JobLogTable.FieldName.LinesOutput" ), BaseMessages.getString( PKG, "JobLogTable.FieldDescription.LinesOutput" ), ValueMetaInterface.TYPE_INTEGER, 18 ) );
    table.fields.add( new LogTableField( ID.LINES_REJECTED.id, true, false, "LINES_REJECTED", BaseMessages.getString( PKG, "JobLogTable.FieldName.LinesRejected" ), BaseMessages.getString( PKG, "JobLogTable.FieldDescription.LinesRejected" ), ValueMetaInterface.TYPE_INTEGER, 18 ) );
    table.fields.add( new LogTableField( ID.ERRORS.id, true, false, "ERRORS", BaseMessages.getString( PKG, "JobLogTable.FieldName.Errors" ), BaseMessages.getString( PKG, "JobLogTable.FieldDescription.Errors" ), ValueMetaInterface.TYPE_INTEGER, 18 ) );
    table.fields.add( new LogTableField( ID.STARTDATE.id, true, false, "STARTDATE", BaseMessages.getString( PKG, "JobLogTable.FieldName.StartDateRange" ), BaseMessages.getString( PKG, "JobLogTable.FieldDescription.StartDateRange" ), ValueMetaInterface.TYPE_DATE, -1 ) );
    table.fields.add( new LogTableField( ID.ENDDATE.id, true, false, "ENDDATE", BaseMessages.getString( PKG, "JobLogTable.FieldName.EndDateRange" ), BaseMessages.getString( PKG, "JobLogTable.FieldDescription.EndDateRange" ), ValueMetaInterface.TYPE_DATE, -1 ) );
    table.fields.add( new LogTableField( ID.LOGDATE.id, true, false, "LOGDATE", BaseMessages.getString( PKG, "JobLogTable.FieldName.LogDate" ), BaseMessages.getString( PKG, "JobLogTable.FieldDescription.LogDate" ), ValueMetaInterface.TYPE_DATE, -1 ) );
    table.fields.add( new LogTableField( ID.DEPDATE.id, true, false, "DEPDATE", BaseMessages.getString( PKG, "JobLogTable.FieldName.DepDate" ), BaseMessages.getString( PKG, "JobLogTable.FieldDescription.DepDate" ), ValueMetaInterface.TYPE_DATE, -1 ) );
    table.fields.add( new LogTableField( ID.REPLAYDATE.id, true, false, "REPLAYDATE", BaseMessages.getString( PKG, "JobLogTable.FieldName.ReplayDate" ), BaseMessages.getString( PKG, "JobLogTable.FieldDescription.ReplayDate" ), ValueMetaInterface.TYPE_DATE, -1 ) );
    table.fields.add( new LogTableField( ID.LOG_FIELD.id, true, false, "LOG_FIELD", BaseMessages.getString( PKG, "JobLogTable.FieldName.LogField" ), BaseMessages.getString( PKG, "JobLogTable.FieldDescription.LogField" ), ValueMetaInterface.TYPE_STRING, DatabaseMeta.CLOB_LENGTH ) );
    table.fields.add( new LogTableField( ID.EXECUTING_SERVER.id, false, false, "EXECUTING_SERVER", BaseMessages.getString( PKG, "JobLogTable.FieldName.ExecutingServer" ), BaseMessages.getString( PKG, "JobLogTable.FieldDescription.ExecutingServer" ), ValueMetaInterface.TYPE_STRING, 255 ) );
    table.fields.add( new LogTableField( ID.EXECUTING_USER.id, false, false, "EXECUTING_USER", BaseMessages.getString( PKG, "JobLogTable.FieldName.ExecutingUser" ), BaseMessages.getString( PKG, "JobLogTable.FieldDescription.ExecutingUser" ), ValueMetaInterface.TYPE_STRING, 255 ) );
    table.fields.add( new LogTableField( ID.START_JOB_ENTRY.id, false, false, "START_JOB_ENTRY", BaseMessages.getString( PKG, "JobLogTable.FieldName.StartingJobEntry" ), BaseMessages.getString( PKG, "JobLogTable.FieldDescription.StartingJobEntry" ), ValueMetaInterface.TYPE_STRING, 255 ) );
    table.fields.add( new LogTableField( ID.CLIENT.id, false, false, "CLIENT", BaseMessages.getString( PKG, "JobLogTable.FieldName.Client" ), BaseMessages.getString( PKG, "JobLogTable.FieldDescription.Client" ), ValueMetaInterface.TYPE_STRING, 255 ) );

    table.findField( ID.ID_JOB ).setKey( true );
    table.findField( ID.LOGDATE ).setLogDateField( true );
    table.findField( ID.LOG_FIELD ).setLogField( true );
    table.findField( ID.CHANNEL_ID ).setVisible( false );
    table.findField( ID.JOBNAME ).setVisible( false );
    table.findField( ID.STATUS ).setStatusField( true );
    table.findField( ID.ERRORS ).setErrorsField( true );
    table.findField( ID.JOBNAME ).setNameField( true );

    return table;
  }

  public LogTableField findField( ID id ) {
    return super.findField( id.id );
  }

  public Object getSubject( ID id ) {
    return super.getSubject( id.id );
  }

  public String getSubjectString( ID id ) {
    return super.getSubjectString( id.id );
  }

  public void setBatchIdUsed( boolean use ) {
    findField( ID.ID_JOB ).setEnabled( use );
  }

  public boolean isBatchIdUsed() {
    return findField( ID.ID_JOB ).isEnabled();
  }

  public void setLogFieldUsed( boolean use ) {
    findField( ID.LOG_FIELD ).setEnabled( use );
  }

  public boolean isLogFieldUsed() {
    return findField( ID.LOG_FIELD ).isEnabled();
  }

  public String getStepnameRead() {
    return getSubjectString( ID.LINES_READ );
  }

  public String getStepnameWritten() {
    return getSubjectString( ID.LINES_WRITTEN );
  }

  public String getStepnameInput() {
    return getSubjectString( ID.LINES_INPUT );
  }

  public String getStepnameOutput() {
    return getSubjectString( ID.LINES_OUTPUT );
  }

  public String getStepnameUpdated() {
    return getSubjectString( ID.LINES_UPDATED );
  }

  public String getStepnameRejected() {
    return getSubjectString( ID.LINES_REJECTED );
  }

  /**
   * Sets the logging interval in seconds. Disabled if the logging interval is <=0.
   *
   * @param logInterval
   *          The log interval value. A value higher than 0 means that the log table is updated every 'logInterval'
   *          seconds.
   */
  public void setLogInterval( String logInterval ) {
    this.logInterval = logInterval;
  }

  /**
   * Get the logging interval in seconds. Disabled if the logging interval is <=0. A value higher than 0 means that the
   * log table is updated every 'logInterval' seconds.
   *
   * @return The log interval,
   */
  public String getLogInterval() {
    return logInterval;
  }

  /**
   * @return the logSizeLimit
   */
  public String getLogSizeLimit() {
    return logSizeLimit;
  }

  /**
   * @param logSizeLimit
   *          the logSizeLimit to set
   */
  public void setLogSizeLimit( String logSizeLimit ) {
    this.logSizeLimit = logSizeLimit;
  }

  /**
   * This method calculates all the values that are required for the logging table
   *
   * @param status
   *          the log status to use
   * @param subject
   * @param parent
   */
  public RowMetaAndData getLogRecord( LogStatus status, Object subject, Object parent ) {
    if ( subject == null || subject instanceof Job ) {
      Job job = (Job) subject;
      Result result = null;
      if ( job != null ) {
        result = job.getResult();
      }

      RowMetaAndData row = new RowMetaAndData();

      for ( LogTableField field : fields ) {
        if ( field.isEnabled() ) {
          Object value = null;
          if ( job != null ) {

            switch ( ID.valueOf( field.getId() ) ) {
              case ID_JOB:
                value = new Long( job.getBatchId() );
                break;
              case CHANNEL_ID:
                value = job.getLogChannelId();
                break;
              case JOBNAME:
                value = job.getJobname();
                break;
              case STATUS:
                value = status.getStatus();
                break;
              case LINES_READ:
                value = result == null ? null : new Long( result.getNrLinesRead() );
                break;
              case LINES_WRITTEN:
                value = result == null ? null : new Long( result.getNrLinesWritten() );
                break;
              case LINES_INPUT:
                value = result == null ? null : new Long( result.getNrLinesInput() );
                break;
              case LINES_OUTPUT:
                value = result == null ? null : new Long( result.getNrLinesOutput() );
                break;
              case LINES_UPDATED:
                value = result == null ? null : new Long( result.getNrLinesUpdated() );
                break;
              case LINES_REJECTED:
                value = result == null ? null : new Long( result.getNrLinesRejected() );
                break;
              case ERRORS:
                value = result == null ? null : new Long( result.getNrErrors() );
                break;
              case STARTDATE:
                value = job.getStartDate();
                break;
              case LOGDATE:
                value = job.getLogDate();
                break;
              case ENDDATE:
                value = job.getEndDate();
                break;
              case DEPDATE:
                value = job.getDepDate();
                break;
              case REPLAYDATE:
                value = job.getCurrentDate();
                break;
              case LOG_FIELD:
                value = getLogBuffer( job, job.getLogChannelId(), status, logSizeLimit );
                break;
              case EXECUTING_SERVER:
                value = job.getExecutingServer();
                break;
              case EXECUTING_USER:
                value = job.getExecutingUser();
                break;
              case START_JOB_ENTRY:
                value = job.getStartJobEntryCopy() != null ? job.getStartJobEntryCopy().getName() : null;
                break;
              case CLIENT:
                value =
                  HopClientEnvironment.getInstance().getClient() != null ? HopClientEnvironment
                    .getInstance().getClient().toString() : "unknown";
                break;
              default:
                break;
            }
          }

          row.addValue( field.getFieldName(), field.getDataType(), value );
          row.getRowMeta().getValueMeta( row.size() - 1 ).setLength( field.getLength() );
        }
      }

      return row;
    } else {
      return null;
    }
  }

  public String getLogTableCode() {
    return "JOB";
  }

  public String getLogTableType() {
    return BaseMessages.getString( PKG, "JobLogTable.Type.Description" );
  }

  public String getConnectionNameVariable() {
    return Const.HOP_JOB_LOG_DB;
  }

  public String getSchemaNameVariable() {
    return Const.HOP_JOB_LOG_SCHEMA;
  }

  public String getTableNameVariable() {
    return Const.HOP_JOB_LOG_TABLE;
  }

  public List<RowMetaInterface> getRecommendedIndexes() {
    List<RowMetaInterface> indexes = new ArrayList<RowMetaInterface>();

    // First index : ID_JOB if any is used.
    //
    if ( isBatchIdUsed() ) {
      RowMetaInterface batchIndex = new RowMeta();
      LogTableField keyField = getKeyField();

      ValueMetaInterface keyMeta = new ValueMetaBase( keyField.getFieldName(), keyField.getDataType() );
      keyMeta.setLength( keyField.getLength() );
      batchIndex.addValueMeta( keyMeta );

      indexes.add( batchIndex );
    }

    // The next index includes : ERRORS, STATUS, JOBNAME:

    RowMetaInterface lookupIndex = new RowMeta();
    LogTableField errorsField = findField( ID.ERRORS );
    if ( errorsField != null ) {
      ValueMetaInterface valueMeta = new ValueMetaBase( errorsField.getFieldName(), errorsField.getDataType() );
      valueMeta.setLength( errorsField.getLength() );
      lookupIndex.addValueMeta( valueMeta );
    }
    LogTableField statusField = findField( ID.STATUS );
    if ( statusField != null ) {
      ValueMetaInterface valueMeta = new ValueMetaBase( statusField.getFieldName(), statusField.getDataType() );
      valueMeta.setLength( statusField.getLength() );
      lookupIndex.addValueMeta( valueMeta );
    }
    LogTableField transNameField = findField( ID.JOBNAME );
    if ( transNameField != null ) {
      ValueMetaInterface valueMeta = new ValueMetaBase( transNameField.getFieldName(), transNameField.getDataType() );
      valueMeta.setLength( transNameField.getLength() );
      lookupIndex.addValueMeta( valueMeta );
    }

    indexes.add( lookupIndex );

    return indexes;
  }

  @Override
  public void setAllGlobalParametersToNull() {
    boolean clearGlobalVariables = Boolean.valueOf( System.getProperties().getProperty( Const.HOP_GLOBAL_LOG_VARIABLES_CLEAR_ON_EXPORT, "false" ) );
    if ( clearGlobalVariables ) {
      super.setAllGlobalParametersToNull();

      logInterval = isGlobalParameter( logInterval ) ? null : logInterval;
      logSizeLimit = isGlobalParameter( logSizeLimit ) ? null : logSizeLimit;
    }
  }
}
