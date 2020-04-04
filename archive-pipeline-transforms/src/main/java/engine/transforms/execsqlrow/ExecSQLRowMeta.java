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

package org.apache.hop.pipeline.transforms.execsqlrow;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.iVariables;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInterface;
import org.apache.hop.pipeline.transforms.sql.ExecSQL;
import org.w3c.dom.Node;

import java.util.List;

/***
 * Contains meta-data to execute arbitrary SQL from a specified field.
 *
 * Created on 10-sep-2008
 */
@InjectionSupported( localizationPrefix = "ExecSQLRowMeta.Injection.", groups = "OUTPUT_FIELDS" )
public class ExecSQLRowMeta extends BaseTransformMeta implements TransformMetaInterface {
  private static Class<?> PKG = ExecSQLRowMeta.class; // for i18n purposes, needed by Translator!!

  private IMetaStore metaStore;

  private DatabaseMeta databaseMeta;

  @Injection( name = "SQL_FIELD_NAME" )
  private String sqlField;

  @Injection( name = "UPDATE_STATS", group = "OUTPUT_FIELDS" )
  private String updateField;

  @Injection( name = "INSERT_STATS", group = "OUTPUT_FIELDS" )
  private String insertField;

  @Injection( name = "DELETE_STATS", group = "OUTPUT_FIELDS" )
  private String deleteField;

  @Injection( name = "READ_STATS", group = "OUTPUT_FIELDS" )
  private String readField;

  /**
   * Commit size for inserts/updates
   */
  @Injection( name = "COMMIT_SIZE" )
  private int commitSize;

  @Injection( name = "READ_SQL_FROM_FILE" )
  private boolean sqlFromfile;

  /**
   * Send SQL as single statement
   **/
  @Injection( name = "SEND_SINGLE_STATEMENT" )
  private boolean sendOneStatement;

  public ExecSQLRowMeta() {
    super();
  }

  @Injection( name = "CONNECTION_NAME" )
  public void setConnection( String connectionName ) {
    try {
      databaseMeta = DatabaseMeta.loadDatabase( metaStore, connectionName );
    } catch ( Exception e ) {
      throw new RuntimeException( "Unable to load connection '" + connectionName + "'", e );
    }
  }

  /**
   * @return Returns the sqlFromfile.
   */
  public boolean IsSendOneStatement() {
    return sendOneStatement;
  }

  /**
   * @param sendOneStatement The sendOneStatement to set.
   */
  public void SetSendOneStatement( boolean sendOneStatement ) {
    this.sendOneStatement = sendOneStatement;
  }

  /**
   * @return Returns the sqlFromfile.
   */
  public boolean isSqlFromfile() {
    return sqlFromfile;
  }

  /**
   * @param sqlFromfile The sqlFromfile to set.
   */
  public void setSqlFromfile( boolean sqlFromfile ) {
    this.sqlFromfile = sqlFromfile;
  }

  /**
   * @return Returns the database.
   */
  public DatabaseMeta getDatabaseMeta() {
    return databaseMeta;
  }

  /**
   * @param database The database to set.
   */
  public void setDatabaseMeta( DatabaseMeta database ) {
    this.databaseMeta = database;
  }

  /**
   * @return Returns the sqlField.
   */
  public String getSqlFieldName() {
    return sqlField;
  }

  /**
   * @param sqlField The sqlField to sqlField.
   */
  public void setSqlFieldName( String sqlField ) {
    this.sqlField = sqlField;
  }

  /**
   * @return Returns the commitSize.
   */
  public int getCommitSize() {
    return commitSize;
  }

  /**
   * @param commitSize The commitSize to set.
   */
  public void setCommitSize( int commitSize ) {
    this.commitSize = commitSize;
  }

  /**
   * @return Returns the deleteField.
   */
  public String getDeleteField() {
    return deleteField;
  }

  /**
   * @param deleteField The deleteField to set.
   */
  public void setDeleteField( String deleteField ) {
    this.deleteField = deleteField;
  }

  /**
   * @return Returns the insertField.
   */
  public String getInsertField() {
    return insertField;
  }

  /**
   * @param insertField The insertField to set.
   */
  public void setInsertField( String insertField ) {
    this.insertField = insertField;
  }

  /**
   * @return Returns the readField.
   */
  public String getReadField() {
    return readField;
  }

  /**
   * @param readField The readField to set.
   */
  public void setReadField( String readField ) {
    this.readField = readField;
  }

  /**
   * @return Returns the updateField.
   */
  public String getUpdateField() {
    return updateField;
  }

  /**
   * @param updateField The updateField to set.
   */
  public void setUpdateField( String updateField ) {
    this.updateField = updateField;
  }

  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode, metaStore );
  }

  public Object clone() {
    ExecSQLRowMeta retval = (ExecSQLRowMeta) super.clone();
    return retval;
  }

  private void readData( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    this.metaStore = metaStore;
    try {
      String csize;
      String con = XMLHandler.getTagValue( transformNode, "connection" );
      databaseMeta = DatabaseMeta.loadDatabase( metaStore, con );
      csize = XMLHandler.getTagValue( transformNode, "commit" );
      commitSize = Const.toInt( csize, 0 );
      sqlField = XMLHandler.getTagValue( transformNode, "sql_field" );

      insertField = XMLHandler.getTagValue( transformNode, "insert_field" );
      updateField = XMLHandler.getTagValue( transformNode, "update_field" );
      deleteField = XMLHandler.getTagValue( transformNode, "delete_field" );
      readField = XMLHandler.getTagValue( transformNode, "read_field" );
      sqlFromfile = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "sqlFromfile" ) );

      sendOneStatement =
        "Y".equalsIgnoreCase( Const.NVL( XMLHandler.getTagValue( transformNode, "sendOneStatement" ), "Y" ) );
    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString(
        PKG, "ExecSQLRowMeta.Exception.UnableToLoadTransformMetaFromXML" ), e );
    }
  }

  public void setDefault() {
    sqlFromfile = false;
    commitSize = 1;
    databaseMeta = null;
    sqlField = null;
    sendOneStatement = true;
  }

  public void getFields( IRowMeta r, String name, IRowMeta[] info, TransformMeta nextTransform,
                         iVariables variables, IMetaStore metaStore ) throws HopTransformException {
    RowMetaAndData add =
      ExecSQL.getResultRow( new Result(), getUpdateField(), getInsertField(), getDeleteField(), getReadField() );

    r.mergeRowMeta( add.getRowMeta() );
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder( 300 );
    retval.append( "    " ).append( XMLHandler.addTagValue( "commit", commitSize ) );
    retval
      .append( "    " ).append(
      XMLHandler.addTagValue( "connection", databaseMeta == null ? "" : databaseMeta.getName() ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "sql_field", sqlField ) );

    retval.append( "    " ).append( XMLHandler.addTagValue( "insert_field", insertField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "update_field", updateField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "delete_field", deleteField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "read_field", readField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "sqlFromfile", sqlFromfile ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "sendOneStatement", sendOneStatement ) );
    return retval.toString();
  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, iVariables variables,
                     IMetaStore metaStore ) {
    CheckResult cr;

    if ( databaseMeta != null ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "ExecSQLRowMeta.CheckResult.ConnectionExists" ), transformMeta );
      remarks.add( cr );

      Database db = new Database( loggingObject, databaseMeta );
      databases = new Database[] { db }; // keep track of it for cancelling purposes...

      try {
        db.connect();
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
            PKG, "ExecSQLRowMeta.CheckResult.DBConnectionOK" ), transformMeta );
        remarks.add( cr );

        if ( sqlField != null && sqlField.length() != 0 ) {
          cr =
            new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
              PKG, "ExecSQLRowMeta.CheckResult.SQLFieldNameEntered" ), transformMeta );
        } else {
          cr =
            new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
              PKG, "ExecSQLRowMeta.CheckResult.SQLFieldNameMissing" ), transformMeta );
        }
        remarks.add( cr );
      } catch ( HopException e ) {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
            PKG, "ExecSQLRowMeta.CheckResult.ErrorOccurred" )
            + e.getMessage(), transformMeta );
        remarks.add( cr );
      } finally {
        db.disconnect();
      }
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "ExecSQLRowMeta.CheckResult.ConnectionNeeded" ), transformMeta );
      remarks.add( cr );
    }

    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "ExecSQLRowMeta.CheckResult.TransformReceivingInfoOK" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "ExecSQLRowMeta.CheckResult.NoInputReceivedError" ), transformMeta );
      remarks.add( cr );
    }

  }

  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr,
                                PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new ExecSQLRow( transformMeta, this, data, cnr, pipelineMeta, pipeline );
  }

  public ITransformData getTransformData() {
    return new ExecSQLRowData();
  }

  public DatabaseMeta[] getUsedDatabaseConnections() {
    if ( databaseMeta != null ) {
      return new DatabaseMeta[] { databaseMeta };
    } else {
      return super.getUsedDatabaseConnections();
    }
  }

  public boolean supportsErrorHandling() {
    return true;
  }

  /**
   * Gets metaStore
   *
   * @return value of metaStore
   */
  public IMetaStore getMetaStore() {
    return metaStore;
  }

  /**
   * @param metaStore The metaStore to set
   */
  public void setMetaStore( IMetaStore metaStore ) {
    this.metaStore = metaStore;
  }
}
