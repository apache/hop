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

package org.apache.hop.core.logging;

import org.apache.hop.core.Const;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

/**
 * This class describes a pipeline logging table
 *
 * @author matt
 */
public class PipelineLogTable extends BaseLogTable implements Cloneable, ILogTable {

  private static Class<?> PKG = PipelineLogTable.class; // for i18n purposes, needed by Translator!!

  public static final String XML_TAG = "pipeline-log-table";

  /**
   * The client executing the pipeline
   */
  // private String client;

  public enum ID {

    ID_BATCH( "ID_BATCH" ), CHANNEL_ID( "CHANNEL_ID" ), PIPELINE_NAME( "PIPELINE_NAME" ), STATUS( "STATUS" ), LINES_READ(
      "LINES_READ" ), LINES_WRITTEN( "LINES_WRITTEN" ), LINES_UPDATED( "LINES_UPDATED" ), LINES_INPUT(
      "LINES_INPUT" ), LINES_OUTPUT( "LINES_OUTPUT" ), LINES_REJECTED( "LINES_REJECTED" ), ERRORS( "ERRORS" ),
    STARTDATE( "STARTDATE" ), ENDDATE( "ENDDATE" ), LOGDATE( "LOGDATE" ), DEPDATE( "DEPDATE" ), REPLAYDATE(
      "REPLAYDATE" ), LOG_FIELD( "LOG_FIELD" ), EXECUTING_SERVER( "EXECUTING_SERVER" ), EXECUTING_USER(
      "EXECUTING_USER" ), CLIENT( "CLIENT" );

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

  private List<TransformMeta> transforms;

  public PipelineLogTable( IVariables variables, IMetaStore metaStore, List<TransformMeta> transforms ) {
    super( variables, metaStore, null, null, null );
    this.transforms = transforms;
  }

  @Override
  public Object clone() {
    try {
      PipelineLogTable table = (PipelineLogTable) super.clone();
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

  public void loadXML( Node node, List<TransformMeta> transforms ) {
    connectionName = XMLHandler.getTagValue( node, "connection" );
    schemaName = XMLHandler.getTagValue( node, "schema" );
    tableName = XMLHandler.getTagValue( node, "table" );
    logSizeLimit = XMLHandler.getTagValue( node, "size_limit_lines" );
    logInterval = XMLHandler.getTagValue( node, "interval" );
    timeoutInDays = XMLHandler.getTagValue( node, "timeout_days" );

    int nr = XMLHandler.countNodes( node, BaseLogTable.XML_TAG );
    for ( int i = 0; i < nr; i++ ) {
      Node fieldNode = XMLHandler.getSubNodeByNr( node, BaseLogTable.XML_TAG, i );
      String id = XMLHandler.getTagValue( fieldNode, "id" );
      LogTableField field = findField( id );
      if ( field == null ) {
        field = fields.get( i );
      }
      if ( field != null ) {
        field.setFieldName( XMLHandler.getTagValue( fieldNode, "name" ) );
        field.setEnabled( "Y".equalsIgnoreCase( XMLHandler.getTagValue( fieldNode, "enabled" ) ) );
        field.setSubject( TransformMeta.findTransform( transforms, XMLHandler.getTagValue( fieldNode, "subject" ) ) );
      }
    }
  }

  @Override
  public void replaceMeta( ILogTableCore logTableInterface ) {
    if ( !( logTableInterface instanceof PipelineLogTable ) ) {
      return;
    }

    PipelineLogTable logTable = (PipelineLogTable) logTableInterface;
    super.replaceMeta( logTable );
  }

  //CHECKSTYLE:LineLength:OFF
  public static PipelineLogTable getDefault( IVariables variables, IMetaStore metaStore,
                                             List<TransformMeta> transforms ) {
    PipelineLogTable table = new PipelineLogTable( variables, metaStore, transforms );

    table.fields.add( new LogTableField( ID.ID_BATCH.id, true, false, "ID_BATCH", BaseMessages.getString( PKG, "PipelineLogTable.FieldName.BatchID" ),
      BaseMessages.getString( PKG, "PipelineLogTable.FieldDescription.BatchID" ), IValueMeta.TYPE_INTEGER, 8 ) );
    table.fields.add( new LogTableField( ID.CHANNEL_ID.id, true, false, "CHANNEL_ID", BaseMessages.getString( PKG, "PipelineLogTable.FieldName.ChannelID" ),
      BaseMessages.getString( PKG, "PipelineLogTable.FieldDescription.ChannelID" ), IValueMeta.TYPE_STRING, 255 ) );
    table.fields.add( new LogTableField( ID.PIPELINE_NAME.id, true, false, "PIPELINE_NAME", BaseMessages.getString( PKG, "PipelineLogTable.FieldName.PipelineName" ),
      BaseMessages.getString( PKG, "PipelineLogTable.FieldDescription.PipelineName" ), IValueMeta.TYPE_STRING, 255 ) );
    table.fields.add( new LogTableField( ID.STATUS.id, true, false, "STATUS", BaseMessages.getString( PKG, "PipelineLogTable.FieldName.Status" ),
      BaseMessages.getString( PKG, "PipelineLogTable.FieldDescription.Status" ), IValueMeta.TYPE_STRING, 15 ) );
    table.fields.add( new LogTableField( ID.LINES_READ.id, true, true, "LINES_READ", BaseMessages.getString( PKG, "PipelineLogTable.FieldName.LinesRead" ),
      BaseMessages.getString( PKG, "PipelineLogTable.FieldDescription.LinesRead" ), IValueMeta.TYPE_INTEGER, 18 ) );
    table.fields.add( new LogTableField( ID.LINES_WRITTEN.id, true, true, "LINES_WRITTEN", BaseMessages.getString( PKG, "PipelineLogTable.FieldName.LinesWritten" ),
      BaseMessages.getString( PKG, "PipelineLogTable.FieldDescription.LinesWritten" ), IValueMeta.TYPE_INTEGER, 18 ) );
    table.fields.add( new LogTableField( ID.LINES_UPDATED.id, true, true, "LINES_UPDATED", BaseMessages.getString( PKG, "PipelineLogTable.FieldName.LinesUpdated" ),
      BaseMessages.getString( PKG, "PipelineLogTable.FieldDescription.LinesUpdated" ), IValueMeta.TYPE_INTEGER, 18 ) );
    table.fields.add( new LogTableField( ID.LINES_INPUT.id, true, true, "LINES_INPUT", BaseMessages.getString( PKG, "PipelineLogTable.FieldName.LinesInput" ),
      BaseMessages.getString( PKG, "PipelineLogTable.FieldDescription.LinesInput" ), IValueMeta.TYPE_INTEGER, 18 ) );
    table.fields.add( new LogTableField( ID.LINES_OUTPUT.id, true, true, "LINES_OUTPUT", BaseMessages.getString( PKG, "PipelineLogTable.FieldName.LinesOutput" ),
      BaseMessages.getString( PKG, "PipelineLogTable.FieldDescription.LinesOutput" ), IValueMeta.TYPE_INTEGER, 18 ) );
    table.fields.add( new LogTableField( ID.LINES_REJECTED.id, true, true, "LINES_REJECTED", BaseMessages.getString( PKG, "PipelineLogTable.FieldName.LinesRejected" ),
      BaseMessages.getString( PKG, "PipelineLogTable.FieldDescription.LinesRejected" ), IValueMeta.TYPE_INTEGER, 18 ) );
    table.fields.add( new LogTableField( ID.ERRORS.id, true, false, "ERRORS", BaseMessages.getString( PKG, "PipelineLogTable.FieldName.Errors" ),
      BaseMessages.getString( PKG, "PipelineLogTable.FieldDescription.Errors" ), IValueMeta.TYPE_INTEGER, 18 ) );
    table.fields.add( new LogTableField( ID.STARTDATE.id, true, false, "STARTDATE", BaseMessages.getString( PKG, "PipelineLogTable.FieldName.StartDateRange" ),
      BaseMessages.getString( PKG, "PipelineLogTable.FieldDescription.StartDateRange" ), IValueMeta.TYPE_DATE, -1 ) );
    table.fields.add( new LogTableField( ID.ENDDATE.id, true, false, "ENDDATE", BaseMessages.getString( PKG, "PipelineLogTable.FieldName.EndDateRange" ),
      BaseMessages.getString( PKG, "PipelineLogTable.FieldDescription.EndDateRange" ), IValueMeta.TYPE_DATE, -1 ) );
    table.fields.add( new LogTableField( ID.LOGDATE.id, true, false, "LOGDATE", BaseMessages.getString( PKG, "PipelineLogTable.FieldName.LogDate" ),
      BaseMessages.getString( PKG, "PipelineLogTable.FieldDescription.LogDate" ), IValueMeta.TYPE_DATE, -1 ) );
    table.fields.add( new LogTableField( ID.DEPDATE.id, true, false, "DEPDATE", BaseMessages.getString( PKG, "PipelineLogTable.FieldName.DepDate" ),
      BaseMessages.getString( PKG, "PipelineLogTable.FieldDescription.DepDate" ), IValueMeta.TYPE_DATE, -1 ) );
    table.fields.add( new LogTableField( ID.REPLAYDATE.id, true, false, "REPLAYDATE", BaseMessages.getString( PKG, "PipelineLogTable.FieldName.ReplayDate" ),
      BaseMessages.getString( PKG, "PipelineLogTable.FieldDescription.ReplayDate" ), IValueMeta.TYPE_DATE, -1 ) );
    table.fields.add( new LogTableField( ID.LOG_FIELD.id, true, false, "LOG_FIELD", BaseMessages.getString( PKG, "PipelineLogTable.FieldName.LogField" ),
      BaseMessages.getString( PKG, "PipelineLogTable.FieldDescription.LogField" ), IValueMeta.TYPE_STRING, DatabaseMeta.CLOB_LENGTH ) );
    table.fields.add( new LogTableField( ID.EXECUTING_SERVER.id, false, false, "EXECUTING_SERVER", BaseMessages.getString( PKG, "PipelineLogTable.FieldName.ExecutingServer" ),
      BaseMessages.getString( PKG, "PipelineLogTable.FieldDescription.ExecutingServer" ), IValueMeta.TYPE_STRING, 255 ) );
    table.fields.add( new LogTableField( ID.EXECUTING_USER.id, false, false, "EXECUTING_USER", BaseMessages.getString( PKG, "PipelineLogTable.FieldName.ExecutingUser" ),
      BaseMessages.getString( PKG, "PipelineLogTable.FieldDescription.ExecutingUser" ), IValueMeta.TYPE_STRING, 255 ) );
    table.fields.add( new LogTableField( ID.CLIENT.id, false, false, "CLIENT", BaseMessages.getString( PKG, "PipelineLogTable.FieldName.Client" ),
        BaseMessages.getString( PKG, "PipelineLogTable.FieldDescription.Client" ), IValueMeta.TYPE_STRING, 255 ) );

    table.findField( ID.ID_BATCH ).setKey( true );
    table.findField( ID.LOGDATE ).setLogDateField( true );
    table.findField( ID.LOG_FIELD ).setLogField( true );
    table.findField( ID.CHANNEL_ID ).setVisible( false );
    table.findField( ID.PIPELINE_NAME ).setVisible( false );
    table.findField( ID.STATUS ).setStatusField( true );
    table.findField( ID.ERRORS ).setErrorsField( true );
    table.findField( ID.PIPELINE_NAME ).setNameField( true );

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
    findField( ID.ID_BATCH ).setEnabled( use );
  }

  public boolean isBatchIdUsed() {
    return findField( ID.ID_BATCH ).isEnabled();
  }

  public void setLogFieldUsed( boolean use ) {
    findField( ID.LOG_FIELD ).setEnabled( use );
  }

  public boolean isLogFieldUsed() {
    return findField( ID.LOG_FIELD ).isEnabled();
  }

  public String getTransformNameRead() {
    return getSubjectString( ID.LINES_READ );
  }

  public void setTransformRead( TransformMeta read ) {
    findField( ID.LINES_READ ).setSubject( read );
  }

  public String getTransformNameWritten() {
    return getSubjectString( ID.LINES_WRITTEN );
  }

  public void setTransformWritten( TransformMeta written ) {
    findField( ID.LINES_WRITTEN ).setSubject( written );
  }

  public String getTransformNameInput() {
    return getSubjectString( ID.LINES_INPUT );
  }

  public void setTransformInput( TransformMeta input ) {
    findField( ID.LINES_INPUT ).setSubject( input );
  }

  public String getTransformNameOutput() {
    return getSubjectString( ID.LINES_OUTPUT );
  }

  public void setTransformOutput( TransformMeta output ) {
    findField( ID.LINES_OUTPUT ).setSubject( output );
  }

  public String getTransformNameUpdated() {
    return getSubjectString( ID.LINES_UPDATED );
  }

  public void setTransformUpdate( TransformMeta update ) {
    findField( ID.LINES_UPDATED ).setSubject( update );
  }

  public String getTransformNameRejected() {
    return getSubjectString( ID.LINES_REJECTED );
  }

  public void setTransformRejected( TransformMeta rejected ) {
    findField( ID.LINES_REJECTED ).setSubject( rejected );
  }

  /**
   * Sets the logging interval in seconds. Disabled if the logging interval is <=0.
   *
   * @param logInterval The log interval value. A value higher than 0 means that the log table is updated every 'logInterval'
   *                    seconds.
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
   * @param logSizeLimit the logSizeLimit to set
   */
  public void setLogSizeLimit( String logSizeLimit ) {
    this.logSizeLimit = logSizeLimit;
  }

  /**
   * This method calculates all the values that are required
   *
   * @param status  the log status to use
   * @param subject the subject to query, in this case a Pipeline object
   * @param parent
   */
  public RowMetaAndData getLogRecord( LogStatus status, Object subject, Object parent ) {
    if ( subject == null || subject instanceof Pipeline ) {
      Pipeline pipeline = (Pipeline) subject;
      Result result = null;
      if ( pipeline != null ) {
        result = pipeline.getResult();
      }

      RowMetaAndData row = new RowMetaAndData();

      for ( LogTableField field : fields ) {
        if ( field.isEnabled() ) {
          Object value = null;
          if ( pipeline != null ) {

            switch ( ID.valueOf( field.getId() ) ) {
              case ID_BATCH:
                value = new Long( pipeline.getBatchId() );
                break;
              case CHANNEL_ID:
                value = pipeline.getLogChannelId();
                break;
              case PIPELINE_NAME:
                value = pipeline.getName();
                break;
              case STATUS:
                value = status.getStatus();
                break;
              case LINES_READ:
                value = new Long( result.getNrLinesRead() );
                break;
              case LINES_WRITTEN:
                value = new Long( result.getNrLinesWritten() );
                break;
              case LINES_INPUT:
                value = new Long( result.getNrLinesInput() );
                break;
              case LINES_OUTPUT:
                value = new Long( result.getNrLinesOutput() );
                break;
              case LINES_UPDATED:
                value = new Long( result.getNrLinesUpdated() );
                break;
              case LINES_REJECTED:
                value = new Long( result.getNrLinesRejected() );
                break;
              case ERRORS:
                value = new Long( result.getNrErrors() );
                break;
              case STARTDATE:
                value = pipeline.getStartDate();
                break;
              case LOGDATE:
                value = pipeline.getLogDate();
                break;
              case ENDDATE:
                value = pipeline.getEndDate();
                break;
              case DEPDATE:
                value = pipeline.getDepDate();
                break;
              case REPLAYDATE:
                value = pipeline.getCurrentDate();
                break;
              case LOG_FIELD:
                value = getLogBuffer( pipeline, pipeline.getLogChannelId(), status, logSizeLimit );
                break;
              case EXECUTING_SERVER:
                value = pipeline.getExecutingServer();
                break;
              case EXECUTING_USER:
                value = pipeline.getExecutingUser();
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
    return "PIPELINE";
  }

  public String getLogTableType() {
    return BaseMessages.getString( PKG, "PipelineLogTable.Type.Description" );
  }

  public String getConnectionNameVariable() {
    return Const.HOP_PIPELINE_LOG_DB;
  }

  public String getSchemaNameVariable() {
    return Const.HOP_PIPELINE_LOG_SCHEMA;
  }

  public String getTableNameVariable() {
    return Const.HOP_PIPELINE_LOG_TABLE;
  }

  public List<IRowMeta> getRecommendedIndexes() {
    List<IRowMeta> indexes = new ArrayList<IRowMeta>();

    // First index : ID_BATCH if any is used.
    //
    if ( isBatchIdUsed() ) {
      IRowMeta batchIndex = new RowMeta();
      LogTableField keyField = getKeyField();

      IValueMeta keyMeta = new ValueMetaBase( keyField.getFieldName(), keyField.getDataType() );
      keyMeta.setLength( keyField.getLength() );
      batchIndex.addValueMeta( keyMeta );

      indexes.add( batchIndex );
    }

    // The next index includes : ERRORS, STATUS, PIPELINE_NAME:

    IRowMeta lookupIndex = new RowMeta();
    LogTableField errorsField = findField( ID.ERRORS );
    if ( errorsField != null ) {
      IValueMeta valueMeta = new ValueMetaBase( errorsField.getFieldName(), errorsField.getDataType() );
      valueMeta.setLength( errorsField.getLength() );
      lookupIndex.addValueMeta( valueMeta );
    }
    LogTableField statusField = findField( ID.STATUS );
    if ( statusField != null ) {
      IValueMeta valueMeta = new ValueMetaBase( statusField.getFieldName(), statusField.getDataType() );
      valueMeta.setLength( statusField.getLength() );
      lookupIndex.addValueMeta( valueMeta );
    }
    LogTableField pipelineNameField = findField( ID.PIPELINE_NAME );
    if ( pipelineNameField != null ) {
      IValueMeta valueMeta = new ValueMetaBase( pipelineNameField.getFieldName(), pipelineNameField.getDataType() );
      valueMeta.setLength( pipelineNameField.getLength() );
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
