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

import org.apache.hop.core.Const;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaDataCombi;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * This class describes a transform logging table
 *
 * @author matt
 */
public class TransformLogTable extends BaseLogTable implements Cloneable, LogTableInterface {

  private static Class<?> PKG = TransformLogTable.class; // for i18n purposes, needed by Translator!!

  public static final String XML_TAG = "transform-log-table";

  public enum ID {

    ID_BATCH( "ID_BATCH" ), CHANNEL_ID( "CHANNEL_ID" ), LOG_DATE( "LOG_DATE" ), PIPELINE_NAME( "PIPELINE_NAME" ),
    TRANSFORM_NAME( "TRANSFORM_NAME" ), TRANSFORM_COPY( "TRANSFORM_COPY" ), LINES_READ( "LINES_READ" ),
    LINES_WRITTEN( "LINES_WRITTEN" ), LINES_UPDATED( "LINES_UPDATED" ), LINES_INPUT( "LINES_INPUT" ),
    LINES_OUTPUT( "LINES_OUTPUT" ), LINES_REJECTED( "LINES_REJECTED" ), ERRORS( "ERRORS" ),
    LOG_FIELD( "LOG_FIELD" );

    private String id;

    private ID( String id ) {
      this.id = id;
    }

    public String toString() {
      return id;
    }
  }

  private TransformLogTable( VariableSpace space, IMetaStore metaStore ) {
    super( space, metaStore, null, null, null );
  }

  @Override
  public Object clone() {
    try {
      TransformLogTable table = (TransformLogTable) super.clone();
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

  public void loadXML( Node node, List<TransformMeta> transforms ) {
    connectionName = XMLHandler.getTagValue( node, "connection" );
    schemaName = XMLHandler.getTagValue( node, "schema" );
    tableName = XMLHandler.getTagValue( node, "table" );
    timeoutInDays = XMLHandler.getTagValue( node, "timeout_days" );

    super.loadFieldsXML( node );
  }

  @Override
  public void replaceMeta( LogTableCoreInterface logTableInterface ) {
    if ( !( logTableInterface instanceof TransformLogTable ) ) {
      return;
    }

    TransformLogTable logTable = (TransformLogTable) logTableInterface;
    super.replaceMeta( logTable );
  }

  //CHECKSTYLE:LineLength:OFF
  public static TransformLogTable getDefault( VariableSpace space, IMetaStore metaStore ) {
    TransformLogTable table = new TransformLogTable( space, metaStore );

    table.fields.add( new LogTableField( ID.ID_BATCH.id, true, false, "ID_BATCH", BaseMessages.getString( PKG, "TransformLogTable.FieldName.IdBatch" ),
      BaseMessages.getString( PKG, "TransformLogTable.FieldDescription.IdBatch" ), ValueMetaInterface.TYPE_INTEGER, 8 ) );
    table.fields.add( new LogTableField( ID.CHANNEL_ID.id, true, false, "CHANNEL_ID", BaseMessages.getString( PKG, "TransformLogTable.FieldName.ChannelId" ),
      BaseMessages.getString( PKG, "TransformLogTable.FieldDescription.ChannelId" ), ValueMetaInterface.TYPE_STRING, 255 ) );
    table.fields.add( new LogTableField( ID.LOG_DATE.id, true, false, "LOG_DATE", BaseMessages.getString( PKG, "TransformLogTable.FieldName.LogDate" ),
      BaseMessages.getString( PKG, "TransformLogTable.FieldDescription.LogDate" ), ValueMetaInterface.TYPE_DATE, -1 ) );
    table.fields.add( new LogTableField( ID.PIPELINE_NAME.id, true, false, "PIPELINE_NAME", BaseMessages.getString( PKG, "TransformLogTable.FieldName.PipelineName" ),
      BaseMessages.getString( PKG, "TransformLogTable.FieldDescription.PipelineName" ), ValueMetaInterface.TYPE_STRING, 255 ) );
    table.fields.add( new LogTableField( ID.TRANSFORM_NAME.id, true, false, "TRANSFORM_NAME", BaseMessages.getString( PKG, "TransformLogTable.FieldName.TransformName" ),
      BaseMessages.getString( PKG, "TransformLogTable.FieldDescription.TransformName" ), ValueMetaInterface.TYPE_STRING, 255 ) );
    table.fields.add( new LogTableField( ID.TRANSFORM_COPY.id, true, false, "TRANSFORM_COPY", BaseMessages.getString( PKG, "TransformLogTable.FieldName.TransformCopy" ),
      BaseMessages.getString( PKG, "TransformLogTable.FieldDescription.TransformCopy" ), ValueMetaInterface.TYPE_INTEGER, 3 ) );
    table.fields.add( new LogTableField( ID.LINES_READ.id, true, false, "LINES_READ", BaseMessages.getString( PKG, "TransformLogTable.FieldName.LinesRead" ),
      BaseMessages.getString( PKG, "TransformLogTable.FieldDescription.LinesRead" ), ValueMetaInterface.TYPE_INTEGER, 18 ) );
    table.fields.add( new LogTableField( ID.LINES_WRITTEN.id, true, false, "LINES_WRITTEN", BaseMessages.getString( PKG, "TransformLogTable.FieldName.LinesWritten" ),
      BaseMessages.getString( PKG, "TransformLogTable.FieldDescription.LinesWritten" ), ValueMetaInterface.TYPE_INTEGER, 18 ) );
    table.fields.add( new LogTableField( ID.LINES_UPDATED.id, true, false, "LINES_UPDATED", BaseMessages.getString( PKG, "TransformLogTable.FieldName.LinesUpdated" ),
      BaseMessages.getString( PKG, "TransformLogTable.FieldDescription.LinesUpdated" ), ValueMetaInterface.TYPE_INTEGER, 18 ) );
    table.fields.add( new LogTableField( ID.LINES_INPUT.id, true, false, "LINES_INPUT", BaseMessages.getString( PKG, "TransformLogTable.FieldName.LinesInput" ),
      BaseMessages.getString( PKG, "TransformLogTable.FieldDescription.LinesInput" ), ValueMetaInterface.TYPE_INTEGER, 18 ) );
    table.fields.add( new LogTableField( ID.LINES_OUTPUT.id, true, false, "LINES_OUTPUT", BaseMessages.getString( PKG, "TransformLogTable.FieldName.LinesOutput" ),
      BaseMessages.getString( PKG, "TransformLogTable.FieldDescription.LinesOutput" ), ValueMetaInterface.TYPE_INTEGER, 18 ) );
    table.fields.add( new LogTableField( ID.LINES_REJECTED.id, true, false, "LINES_REJECTED", BaseMessages.getString( PKG, "TransformLogTable.FieldName.LinesRejected" ),
      BaseMessages.getString( PKG, "TransformLogTable.FieldDescription.LinesRejected" ), ValueMetaInterface.TYPE_INTEGER, 18 ) );
    table.fields.add(
      new LogTableField( ID.ERRORS.id, true, false, "ERRORS", BaseMessages.getString( PKG, "TransformLogTable.FieldName.Errors" ), BaseMessages.getString( PKG, "TransformLogTable.FieldDescription.Errors" ),
        ValueMetaInterface.TYPE_INTEGER, 18 ) );
    table.fields.add( new LogTableField( ID.LOG_FIELD.id, false, false, "LOG_FIELD", BaseMessages.getString( PKG, "TransformLogTable.FieldName.LogField" ),
      BaseMessages.getString( PKG, "TransformLogTable.FieldDescription.LogField" ), ValueMetaInterface.TYPE_STRING, DatabaseMeta.CLOB_LENGTH ) );

    table.findField( ID.PIPELINE_NAME.id ).setNameField( true );
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
   * @param subject
   * @param parent
   */
  public RowMetaAndData getLogRecord( LogStatus status, Object subject, Object parent ) {
    if ( subject == null || subject instanceof TransformMetaDataCombi ) {

      TransformMetaDataCombi combi = (TransformMetaDataCombi) subject;

      RowMetaAndData row = new RowMetaAndData();

      for ( LogTableField field : fields ) {
        if ( field.isEnabled() ) {
          Object value = null;
          if ( subject != null ) {
            switch ( ID.valueOf( field.getId() ) ) {

              case ID_BATCH:
                value = new Long( combi.transform.getPipeline().getBatchId() );
                break;
              case CHANNEL_ID:
                value = combi.transform.getLogChannel().getLogChannelId();
                break;
              case LOG_DATE:
                value = new Date();
                break;
              case PIPELINE_NAME:
                value = combi.transform.getPipeline().getName();
                break;
              case TRANSFORM_NAME:
                value = combi.transformName;
                break;
              case TRANSFORM_COPY:
                value = new Long( combi.copy );
                break;
              case LINES_READ:
                value = new Long( combi.transform.getLinesRead() );
                break;
              case LINES_WRITTEN:
                value = new Long( combi.transform.getLinesWritten() );
                break;
              case LINES_UPDATED:
                value = new Long( combi.transform.getLinesUpdated() );
                break;
              case LINES_INPUT:
                value = new Long( combi.transform.getLinesInput() );
                break;
              case LINES_OUTPUT:
                value = new Long( combi.transform.getLinesOutput() );
                break;
              case LINES_REJECTED:
                value = new Long( combi.transform.getLinesRejected() );
                break;
              case ERRORS:
                value = new Long( combi.transform.getErrors() );
                break;
              case LOG_FIELD:
                value = getLogBuffer( combi.transform, combi.transform.getLogChannel().getLogChannelId(), status, null );
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
    return "TRANSFORM";
  }

  public String getLogTableType() {
    return BaseMessages.getString( PKG, "TransformLogTable.Type.Description" );
  }

  public String getConnectionNameVariable() {
    return Const.HOP_TRANSFORM_LOG_DB;
  }

  public String getSchemaNameVariable() {
    return Const.HOP_TRANSFORM_LOG_SCHEMA;
  }

  public String getTableNameVariable() {
    return Const.HOP_TRANSFORM_LOG_TABLE;
  }

  public List<RowMetaInterface> getRecommendedIndexes() {
    List<RowMetaInterface> indexes = new ArrayList<RowMetaInterface>();
    return indexes;
  }
}
