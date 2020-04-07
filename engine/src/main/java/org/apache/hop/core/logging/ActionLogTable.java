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
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.gui.WorkflowTracker;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.ActionResult;
import org.apache.hop.workflow.Workflow;
import org.apache.hop.workflow.action.ActionCopy;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * This class describes a action logging table
 *
 * @author matt
 */
public class ActionLogTable extends BaseLogTable implements Cloneable, ILogTable {

  private static Class<?> PKG = ActionLogTable.class; // for i18n purposes, needed by Translator!!

  public static final String XML_TAG = "action-log-table";

  public enum ID {

    ID_BATCH( "ID_BATCH" ), CHANNEL_ID( "CHANNEL_ID" ), LOG_DATE( "LOG_DATE" ), WORKFLOW_NAME( "WORKFLOW_NAME" ),
    ACTION_NAME( "ACTION_NAME" ), LINES_READ( "LINES_READ" ), LINES_WRITTEN( "LINES_WRITTEN" ), LINES_UPDATED(
      "LINES_UPDATED" ), LINES_INPUT( "LINES_INPUT" ), LINES_OUTPUT( "LINES_OUTPUT" ), LINES_REJECTED(
      "LINES_REJECTED" ), ERRORS( "ERRORS" ), RESULT( "RESULT" ), NR_RESULT_ROWS( "NR_RESULT_ROWS" ),
    NR_RESULT_FILES( "NR_RESULT_FILES" ), LOG_FIELD( "LOG_FIELD" ), COPY_NR( "COPY_NR" );

    private String id;

    private ID( String id ) {
      this.id = id;
    }

    public String toString() {
      return id;
    }
  }

  private ActionLogTable( IVariables variables, IMetaStore metaStore ) {
    super( variables, metaStore, null, null, null );
  }

  @Override
  public Object clone() {
    try {
      ActionLogTable table = (ActionLogTable) super.clone();
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
    retval.append( "        " ).append( XMLHandler.addTagValue( "timeout_days", timeoutInDays ) );
    retval.append( super.getFieldsXML() );
    retval.append( "      " ).append( XMLHandler.closeTag( XML_TAG ) ).append( Const.CR );

    return retval.toString();
  }

  @Override
  public void replaceMeta( ILogTableCore logTableInterface ) {
    if ( !( logTableInterface instanceof ActionLogTable ) ) {
      return;
    }

    ActionLogTable logTable = (ActionLogTable) logTableInterface;
    super.replaceMeta( logTable );
  }

  public void loadXML( Node jobnode, List<TransformMeta> transforms ) {
    Node node = XMLHandler.getSubNode( jobnode, XML_TAG );
    if ( node == null ) {
      return;
    }

    connectionName = XMLHandler.getTagValue( node, "connection" );
    schemaName = XMLHandler.getTagValue( node, "schema" );
    tableName = XMLHandler.getTagValue( node, "table" );
    timeoutInDays = XMLHandler.getTagValue( node, "timeout_days" );

    super.loadFieldsXML( node );
  }

  //CHECKSTYLE:LineLength:OFF
  public static ActionLogTable getDefault( IVariables variables, IMetaStore metaStore ) {
    ActionLogTable table = new ActionLogTable( variables, metaStore );

    table.fields.add( new LogTableField( ID.ID_BATCH.id, true, false, "ID_BATCH", BaseMessages.getString( PKG, "ActionLogTable.FieldName.IdBatch" ),
      BaseMessages.getString( PKG, "ActionLogTable.FieldDescription.IdBatch" ), IValueMeta.TYPE_INTEGER, 8 ) );
    table.fields.add( new LogTableField( ID.CHANNEL_ID.id, true, false, "CHANNEL_ID", BaseMessages.getString( PKG, "ActionLogTable.FieldName.ChannelId" ),
      BaseMessages.getString( PKG, "ActionLogTable.FieldDescription.ChannelId" ), IValueMeta.TYPE_STRING, 255 ) );
    table.fields.add( new LogTableField( ID.LOG_DATE.id, true, false, "LOG_DATE", BaseMessages.getString( PKG, "ActionLogTable.FieldName.LogDate" ),
      BaseMessages.getString( PKG, "ActionLogTable.FieldDescription.LogDate" ), IValueMeta.TYPE_DATE, -1 ) );
    table.fields.add( new LogTableField( ID.WORKFLOW_NAME.id, true, false, "PIPELINE_NAME", BaseMessages.getString( PKG, "ActionLogTable.FieldName.JobName" ),
      BaseMessages.getString( PKG, "ActionLogTable.FieldDescription.JobName" ), IValueMeta.TYPE_STRING, 255 ) );
    table.fields.add( new LogTableField( ID.ACTION_NAME.id, true, false, "ACTION_NAME", BaseMessages.getString( PKG, "ActionLogTable.FieldName.ActionName" ),
      BaseMessages.getString( PKG, "ActionLogTable.FieldDescription.ActionName" ), IValueMeta.TYPE_STRING, 255 ) );
    table.fields.add( new LogTableField( ID.LINES_READ.id, true, false, "LINES_READ", BaseMessages.getString( PKG, "ActionLogTable.FieldName.LinesRead" ),
      BaseMessages.getString( PKG, "ActionLogTable.FieldDescription.LinesRead" ), IValueMeta.TYPE_INTEGER, 18 ) );
    table.fields.add( new LogTableField( ID.LINES_WRITTEN.id, true, false, "LINES_WRITTEN", BaseMessages.getString( PKG, "ActionLogTable.FieldName.LinesWritten" ),
      BaseMessages.getString( PKG, "ActionLogTable.FieldDescription.LinesWritten" ), IValueMeta.TYPE_INTEGER, 18 ) );
    table.fields.add( new LogTableField( ID.LINES_UPDATED.id, true, false, "LINES_UPDATED", BaseMessages.getString( PKG, "ActionLogTable.FieldName.LinesUpdated" ),
      BaseMessages.getString( PKG, "ActionLogTable.FieldDescription.LinesUpdated" ), IValueMeta.TYPE_INTEGER, 18 ) );
    table.fields.add( new LogTableField( ID.LINES_INPUT.id, true, false, "LINES_INPUT", BaseMessages.getString( PKG, "ActionLogTable.FieldName.LinesInput" ),
      BaseMessages.getString( PKG, "ActionLogTable.FieldDescription.LinesInput" ), IValueMeta.TYPE_INTEGER, 18 ) );
    table.fields.add( new LogTableField( ID.LINES_OUTPUT.id, true, false, "LINES_OUTPUT", BaseMessages.getString( PKG, "ActionLogTable.FieldName.LinesOutput" ),
      BaseMessages.getString( PKG, "ActionLogTable.FieldDescription.LinesOutput" ), IValueMeta.TYPE_INTEGER, 18 ) );
    table.fields.add( new LogTableField( ID.LINES_REJECTED.id, true, false, "LINES_REJECTED", BaseMessages.getString( PKG, "ActionLogTable.FieldName.LinesRejected" ),
      BaseMessages.getString( PKG, "ActionLogTable.FieldDescription.LinesRejected" ), IValueMeta.TYPE_INTEGER, 18 ) );
    table.fields.add( new LogTableField( ID.ERRORS.id, true, false, "ERRORS", BaseMessages.getString( PKG, "ActionLogTable.FieldName.Errors" ),
      BaseMessages.getString( PKG, "ActionLogTable.FieldDescription.Errors" ), IValueMeta.TYPE_INTEGER, 18 ) );
    table.fields.add( new LogTableField( ID.RESULT.id, true, false, "RESULT", BaseMessages.getString( PKG, "ActionLogTable.FieldName.Result" ),
      BaseMessages.getString( PKG, "ActionLogTable.FieldDescription.Result" ), IValueMeta.TYPE_BOOLEAN, -1 ) );
    table.fields.add( new LogTableField( ID.NR_RESULT_ROWS.id, true, false, "NR_RESULT_ROWS", BaseMessages.getString( PKG, "ActionLogTable.FieldName.NrResultRows" ),
      BaseMessages.getString( PKG, "ActionLogTable.FieldDescription.NrResultRows" ), IValueMeta.TYPE_INTEGER, 18 ) );
    table.fields.add( new LogTableField( ID.NR_RESULT_FILES.id, true, false, "NR_RESULT_FILES", BaseMessages.getString( PKG, "ActionLogTable.FieldName.NrResultFiles" ),
      BaseMessages.getString( PKG, "ActionLogTable.FieldDescription.NrResultFiles" ), IValueMeta.TYPE_INTEGER, 18 ) );
    table.fields.add( new LogTableField( ID.LOG_FIELD.id, false, false, "LOG_FIELD", BaseMessages.getString( PKG, "ActionLogTable.FieldName.LogField" ),
      BaseMessages.getString( PKG, "ActionLogTable.FieldDescription.LogField" ), IValueMeta.TYPE_STRING, DatabaseMeta.CLOB_LENGTH ) );
    table.fields.add( new LogTableField( ID.COPY_NR.id, false, false, "COPY_NR", BaseMessages.getString( PKG, "ActionLogTable.FieldName.CopyNr" ),
      BaseMessages.getString( PKG, "ActionLogTable.FieldDescription.CopyNr" ), IValueMeta.TYPE_INTEGER, 8 ) );

    table.findField( ID.WORKFLOW_NAME.id ).setNameField( true );
    table.findField( ID.LOG_DATE.id ).setLogDateField( true );
    table.findField( ID.ID_BATCH.id ).setKey( true );
    table.findField( ID.CHANNEL_ID.id ).setVisible( false );
    table.findField( ID.LOG_FIELD.id ).setLogField( true );
    table.findField( ID.ERRORS.id ).setErrorsField( true );

    return table;
  }

  /**
   * This method calculates all the values that are required
   *
   * @param status  the log status to use
   * @param subject the object to log
   * @param parent  the parent to which the object belongs
   */
  public RowMetaAndData getLogRecord( LogStatus status, Object subject, Object parent ) {
    if ( subject == null || subject instanceof ActionCopy ) {

      ActionCopy actionCopy = (ActionCopy) subject;
      Workflow parentWorkflow = (Workflow) parent;

      RowMetaAndData row = new RowMetaAndData();

      for ( LogTableField field : fields ) {
        if ( field.isEnabled() ) {
          Object value = null;
          if ( subject != null ) {

            IAction jobEntry = actionCopy.getEntry();
            WorkflowTracker workflowTracker = parentWorkflow.getWorkflowTracker();
            WorkflowTracker entryTracker = workflowTracker.findWorkflowTracker( actionCopy );
            ActionResult actionResult = null;
            if ( entryTracker != null ) {
              actionResult = entryTracker.getActionResult();
            }
            Result result = null;
            if ( actionResult != null ) {
              result = actionResult.getResult();
            }

            switch ( ID.valueOf( field.getId() ) ) {

              case ID_BATCH:
                value = new Long( parentWorkflow.getBatchId() );
                break;
              case CHANNEL_ID:
                value = jobEntry.getLogChannel().getLogChannelId();
                break;
              case LOG_DATE:
                value = new Date();
                break;
              case WORKFLOW_NAME:
                value = parentWorkflow.getJobname();
                break;
              case ACTION_NAME:
                value = jobEntry.getName();
                break;
              case LINES_READ:
                value = new Long( result != null ? result.getNrLinesRead() : 0 );
                break;
              case LINES_WRITTEN:
                value = new Long( result != null ? result.getNrLinesWritten() : 0 );
                break;
              case LINES_UPDATED:
                value = new Long( result != null ? result.getNrLinesUpdated() : 0 );
                break;
              case LINES_INPUT:
                value = new Long( result != null ? result.getNrLinesInput() : 0 );
                break;
              case LINES_OUTPUT:
                value = new Long( result != null ? result.getNrLinesOutput() : 0 );
                break;
              case LINES_REJECTED:
                value = new Long( result != null ? result.getNrLinesRejected() : 0 );
                break;
              case ERRORS:
                value = new Long( result != null ? result.getNrErrors() : 0 );
                break;
              case RESULT:
                value = new Boolean( result != null ? result.getResult() : false );
                break;
              case NR_RESULT_FILES:
                value =
                  new Long( result != null && result.getResultFiles() != null
                    ? result.getResultFiles().size() : 0 );
                break;
              case NR_RESULT_ROWS:
                value = new Long( result != null && result.getRows() != null ? result.getRows().size() : 0 );
                break;
              case LOG_FIELD:
                if ( result != null ) {
                  value = result.getLogText();
                }
                break;
              case COPY_NR:
                value = new Long( actionCopy.getNr() );
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
    return "JOB_ENTRY";
  }

  public String getLogTableType() {
    return BaseMessages.getString( PKG, "ActionLogTable.Type.Description" );
  }

  public String getConnectionNameVariable() {
    return Const.HOP_ACTION_LOG_DB;
  }

  public String getSchemaNameVariable() {
    return Const.HOP_ACTION_LOG_SCHEMA;
  }

  public String getTableNameVariable() {
    return Const.HOP_ACTION_LOG_TABLE;
  }

  public List<IRowMeta> getRecommendedIndexes() {
    List<IRowMeta> indexes = new ArrayList<IRowMeta>();
    LogTableField keyField = getKeyField();

    if ( keyField.isEnabled() ) {
      IRowMeta batchIndex = new RowMeta();

      IValueMeta keyMeta = new ValueMetaBase( keyField.getFieldName(), keyField.getDataType() );
      keyMeta.setLength( keyField.getLength() );
      batchIndex.addValueMeta( keyMeta );

      indexes.add( batchIndex );
    }

    return indexes;
  }
}
