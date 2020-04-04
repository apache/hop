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
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.metrics.IMetricsSnapshot;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * This class describes a logging channel logging table
 *
 * @author matt
 */
public class MetricsLogTable extends BaseLogTable implements Cloneable, ILogTable {

  private static Class<?> PKG = MetricsLogTable.class; // for i18n purposes, needed by Translator!!

  public static final String XML_TAG = "metrics-log-table";

  public enum ID {

    ID_BATCH( "ID_BATCH" ), CHANNEL_ID( "CHANNEL_ID" ), LOG_DATE( "LOG_DATE" ), // The date this record got logged
    METRICS_DATE( "METRICS_DATE" ), // For snapshot: the date/time of snapshot
    METRICS_CODE( "METRICS_CODE" ), // The unique code of the metric
    METRICS_DESCRIPTION( "METRICS_DESCRIPTION" ), // The description of the metric
    METRICS_SUBJECT( "METRICS_SUBJECT" ), // The subject of the metric
    METRICS_TYPE( "METRICS_TYPE" ), // For snapshots: START or STOP, for metrics: MAX, MIN, SUM
    METRICS_VALUE( "METRICS_VALUE" ); // For metrics: the value measured (max, min, sum)

    private String id;

    private ID( String id ) {
      this.id = id;
    }

    public String toString() {
      return id;
    }
  }

  private MetricsLogTable( IVariables variables, IMetaStore metaStore ) {
    super( variables, metaStore, null, null, null );
  }

  @Override
  public Object clone() {
    try {
      MetricsLogTable table = (MetricsLogTable) super.clone();
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
  public void replaceMeta( ILogTableCore logTableInterface ) {
    if ( !( logTableInterface instanceof MetricsLogTable ) ) {
      return;
    }

    MetricsLogTable logTable = (MetricsLogTable) logTableInterface;
    super.replaceMeta( logTable );
  }

  public static MetricsLogTable getDefault( IVariables variables, IMetaStore metaStore ) {
    MetricsLogTable table = new MetricsLogTable( variables, metaStore );

    //CHECKSTYLE:LineLength:OFF
    table.fields.add( new LogTableField( ID.ID_BATCH.id, true, false, "ID_BATCH", BaseMessages.getString( PKG, "MetricsLogTable.FieldName.IdBatch" ),
      BaseMessages.getString( PKG, "MetricsLogTable.FieldDescription.IdBatch" ), IValueMeta.TYPE_INTEGER, 8 ) );
    table.fields.add( new LogTableField( ID.CHANNEL_ID.id, true, false, "CHANNEL_ID", BaseMessages.getString( PKG, "MetricsLogTable.FieldName.ChannelId" ),
      BaseMessages.getString( PKG, "MetricsLogTable.FieldDescription.ChannelId" ), IValueMeta.TYPE_STRING, 255 ) );
    table.fields.add( new LogTableField( ID.LOG_DATE.id, true, false, "LOG_DATE", BaseMessages.getString( PKG, "MetricsLogTable.FieldName.LogDate" ),
      BaseMessages.getString( PKG, "MetricsLogTable.FieldDescription.LogDate" ), IValueMeta.TYPE_DATE, -1 ) );
    table.fields.add( new LogTableField( ID.METRICS_DATE.id, true, false, "METRICS_DATE", BaseMessages.getString( PKG, "MetricsLogTable.FieldName.MetricsDate" ),
      BaseMessages.getString( PKG, "MetricsLogTable.FieldDescription.MetricsDate" ), IValueMeta.TYPE_DATE, -1 ) );
    table.fields.add( new LogTableField( ID.METRICS_CODE.id, true, false, "METRICS_CODE", BaseMessages.getString( PKG, "MetricsLogTable.FieldName.MetricsDescription" ),
      BaseMessages.getString( PKG, "MetricsLogTable.FieldDescription.MetricsCode" ), IValueMeta.TYPE_STRING, 255 ) );
    table.fields.add( new LogTableField( ID.METRICS_DESCRIPTION.id, true, false, "METRICS_DESCRIPTION", BaseMessages.getString( PKG, "MetricsLogTable.FieldName.MetricsDescription" ),
      BaseMessages.getString( PKG, "MetricsLogTable.FieldDescription.MetricsDescription" ), IValueMeta.TYPE_STRING, 255 ) );
    table.fields.add( new LogTableField( ID.METRICS_SUBJECT.id, true, false, "METRICS_SUBJECT", BaseMessages.getString( PKG, "MetricsLogTable.FieldName.MetricsSubject" ),
      BaseMessages.getString( PKG, "MetricsLogTable.FieldDescription.MetricsSubject" ), IValueMeta.TYPE_STRING, 255 ) );
    table.fields.add( new LogTableField( ID.METRICS_TYPE.id, true, false, "METRICS_TYPE", BaseMessages.getString( PKG, "MetricsLogTable.FieldName.MetricsType" ),
      BaseMessages.getString( PKG, "MetricsLogTable.FieldDescription.MetricsType" ), IValueMeta.TYPE_STRING, 255 ) );
    table.fields.add( new LogTableField( ID.METRICS_VALUE.id, true, false, "METRICS_VALUE", BaseMessages.getString( PKG, "MetricsLogTable.FieldName.MetricsValue" ),
      BaseMessages.getString( PKG, "MetricsLogTable.FieldDescription.MetricsValue" ), IValueMeta.TYPE_INTEGER, 12 ) );

    table.findField( ID.LOG_DATE.id ).setLogDateField( true );
    table.findField( ID.ID_BATCH.id ).setKey( true );

    return table;
  }

  /**
   * This method calculates all the values that are required
   *
   * @param status  the log status to use
   * @param subject
   * @param parent  *
   */
  public RowMetaAndData getLogRecord( LogStatus status, Object subject, Object parent ) {
    if ( subject == null || subject instanceof LoggingMetric ) {

      LoggingMetric loggingMetric = (LoggingMetric) subject;
      IMetricsSnapshot snapshot = null;
      if ( subject != null ) {
        snapshot = loggingMetric.getSnapshot();
      }

      RowMetaAndData row = new RowMetaAndData();

      for ( LogTableField field : fields ) {
        if ( field.isEnabled() ) {
          Object value = null;
          if ( subject != null ) {
            switch ( ID.valueOf( field.getId() ) ) {
              case ID_BATCH:
                value = new Long( loggingMetric.getBatchId() );
                break;
              case CHANNEL_ID:
                value = snapshot.getLogChannelId();
                break;
              case LOG_DATE:
                value = new Date();
                break;
              case METRICS_DATE:
                value = snapshot.getDate();
                break;
              case METRICS_CODE:
                value = snapshot.getMetric().getCode();
                break;
              case METRICS_DESCRIPTION:
                value = snapshot.getMetric().getDescription();
                break;
              case METRICS_SUBJECT:
                value = snapshot.getSubject();
                break;
              case METRICS_TYPE:
                value = snapshot.getMetric().getType().name();
                break;
              case METRICS_VALUE:
                value = snapshot.getValue();
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
    return "METRICS";
  }

  public String getLogTableType() {
    return BaseMessages.getString( PKG, "MetricsLogTable.Type.Description" );
  }

  public String getConnectionNameVariable() {
    return Const.HOP_METRICS_LOG_DB;
  }

  public String getSchemaNameVariable() {
    return Const.HOP_METRICS_LOG_SCHEMA;
  }

  public String getTableNameVariable() {
    return Const.HOP_METRICS_LOG_TABLE;
  }

  public List<IRowMeta> getRecommendedIndexes() {
    List<IRowMeta> indexes = new ArrayList<IRowMeta>();
    return indexes;
  }

}
