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

package org.apache.hop.pipeline.transforms.dynamicsqlrow;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.iVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.DatabaseImpact;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.w3c.dom.Node;

import java.util.List;

public class DynamicSqlRowMeta extends BaseTransformMeta implements ITransform {
  private static Class<?> PKG = DynamicSQLRowMeta.class; // for i18n purposes, needed by Translator!!

  /**
   * database connection
   */
  private DatabaseMeta databaseMeta;

  /**
   * SQL Statement
   */
  private String sql;

  private String sqlfieldname;

  /**
   * Number of rows to return (0=ALL)
   */
  private int rowLimit;

  /**
   * false: don't return rows where nothing is found true: at least return one source row, the rest is NULL
   */
  private boolean outerJoin;

  private boolean replacevars;

  public boolean queryonlyonchange;

  public DynamicSqlRowMeta() {
    super(); // allocate BaseTransformMeta
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
   * @return Returns the outerJoin.
   */
  public boolean isOuterJoin() {
    return outerJoin;
  }

  /**
   * @param outerJoin The outerJoin to set.
   */
  public void setOuterJoin( boolean outerJoin ) {
    this.outerJoin = outerJoin;
  }

  /**
   * @return Returns the replacevars.
   */
  public boolean isVariableReplace() {
    return replacevars;
  }

  /**
   * @param replacevars The replacevars to set.
   */
  public void setVariableReplace( boolean replacevars ) {
    this.replacevars = replacevars;
  }

  /**
   * @return Returns the queryonlyonchange.
   */
  public boolean isQueryOnlyOnChange() {
    return queryonlyonchange;
  }

  /**
   * @param queryonlyonchange The queryonlyonchange to set.
   */
  public void setQueryOnlyOnChange( boolean queryonlyonchange ) {
    this.queryonlyonchange = queryonlyonchange;
  }

  /**
   * @return Returns the rowLimit.
   */
  public int getRowLimit() {
    return rowLimit;
  }

  /**
   * @param rowLimit The rowLimit to set.
   */
  public void setRowLimit( int rowLimit ) {
    this.rowLimit = rowLimit;
  }

  /**
   * @return Returns the sql.
   */
  public String getSql() {
    return sql;
  }

  /**
   * @param sql The sql to set.
   */
  public void setSql( String sql ) {
    this.sql = sql;
  }

  /**
   * @return Returns the sqlfieldname.
   */
  public String getSqlFieldName() {
    return sqlfieldname;
  }

  /**
   * @param sqlfieldname The sqlfieldname to set.
   */
  public void setSqlFieldName( String sqlfieldname ) {
    this.sqlfieldname = sqlfieldname;
  }

  public void loadXml( Node transformNode, IMetaStore metaStore ) throws HopXmlException {
    readData( transformNode, metaStore );
  }

  public Object clone() {
    DynamicSQLRowMeta retval = (DynamicSQLRowMeta) super.clone();

    return retval;
  }

  private void readData( Node transformNode, IMetaStore metaStore ) throws HopXmlException {
    try {
      String con = XmlHandler.getTagValue( transformNode, "connection" );
      databaseMeta = DatabaseMeta.loadDatabase( metaStore, con );
      sql = XmlHandler.getTagValue( transformNode, "sql" );
      outerJoin = "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, "outer_join" ) );
      replacevars = "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, "replace_vars" ) );
      queryonlyonchange = "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, "query_only_on_change" ) );

      rowLimit = Const.toInt( XmlHandler.getTagValue( transformNode, "rowlimit" ), 0 );
      sqlfieldname = XmlHandler.getTagValue( transformNode, "sql_fieldname" );

    } catch ( Exception e ) {
      throw new HopXmlException( BaseMessages.getString(
        PKG, "DynamicSQLRowMeta.Exception.UnableToLoadTransformMeta" ), e );
    }
  }

  public void setDefault() {
    databaseMeta = null;
    rowLimit = 0;
    sql = "";
    outerJoin = false;
    replacevars = false;
    sqlfieldname = null;
    queryonlyonchange = false;
  }

  public void getFields( IRowMeta row, String name, IRowMeta[] info, TransformMeta nextTransform,
                         iVariables variables, IMetaStore metaStore ) throws HopTransformException {

    if ( databaseMeta == null ) {
      return;
    }

    Database db = new Database( loggingObject, databaseMeta );
    databases = new Database[] { db }; // Keep track of this one for cancelQuery

    // First try without connecting to the database... (can be S L O W)
    // See if it's in the cache...
    IRowMeta add = null;
    String realSQL = sql;
    if ( replacevars ) {
      realSQL = variables.environmentSubstitute( realSQL );
    }
    try {
      add = db.getQueryFields( realSQL, false );
    } catch ( HopDatabaseException dbe ) {
      throw new HopTransformException( BaseMessages.getString(
        PKG, "DynamicSQLRowMeta.Exception.UnableToDetermineQueryFields" )
        + Const.CR + sql, dbe );
    }

    if ( add != null ) { // ICache hit, just return it this...
      for ( int i = 0; i < add.size(); i++ ) {
        IValueMeta v = add.getValueMeta( i );
        v.setOrigin( name );
      }
      row.addRowMeta( add );
    } else {
      // No cache hit, connect to the database, do it the hard way...
      try {
        db.connect();
        add = db.getQueryFields( realSQL, false );
        for ( int i = 0; i < add.size(); i++ ) {
          IValueMeta v = add.getValueMeta( i );
          v.setOrigin( name );
        }
        row.addRowMeta( add );
        db.disconnect();
      } catch ( HopDatabaseException dbe ) {
        throw new HopTransformException( BaseMessages.getString(
          PKG, "DynamicSQLRowMeta.Exception.ErrorObtainingFields" ), dbe );
      }
    }
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder();

    retval
      .append( "    " + XmlHandler.addTagValue( "connection", databaseMeta == null ? "" : databaseMeta.getName() ) );
    retval.append( "    " + XmlHandler.addTagValue( "rowlimit", rowLimit ) );
    retval.append( "    " + XmlHandler.addTagValue( "sql", sql ) );
    retval.append( "    " + XmlHandler.addTagValue( "outer_join", outerJoin ) );
    retval.append( "    " + XmlHandler.addTagValue( "replace_vars", replacevars ) );
    retval.append( "    " + XmlHandler.addTagValue( "sql_fieldname", sqlfieldname ) );
    retval.append( "    " + XmlHandler.addTagValue( "query_only_on_change", queryonlyonchange ) );

    return retval.toString();
  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, iVariables variables,
                     IMetaStore metaStore ) {
    CheckResult cr;
    String error_message = "";

    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "DynamicSQLRowMeta.CheckResult.ReceivingInfo" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "DynamicSQLRowMeta.CheckResult.NoInputReceived" ), transformMeta );
      remarks.add( cr );
    }

    // Check for SQL field
    if ( Utils.isEmpty( sqlfieldname ) ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "DynamicSQLRowMeta.CheckResult.SQLFieldNameMissing" ), transformMeta );
      remarks.add( cr );
    } else {
      IValueMeta vfield = prev.searchValueMeta( sqlfieldname );
      if ( vfield == null ) {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
            PKG, "DynamicSQLRowMeta.CheckResult.SQLFieldNotFound", sqlfieldname ), transformMeta );
      } else {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
            PKG, "DynamicSQLRowMeta.CheckResult.SQLFieldFound", sqlfieldname, vfield.getOrigin() ), transformMeta );
      }
      remarks.add( cr );
    }

    if ( databaseMeta != null ) {
      Database db = new Database( loggingObject, databaseMeta );
      databases = new Database[] { db }; // Keep track of this one for cancelQuery

      try {
        db.connect();
        if ( sql != null && sql.length() != 0 ) {

          error_message = "";

          IRowMeta r = db.getQueryFields( sql, true );
          if ( r != null ) {
            cr =
              new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
                PKG, "DynamicSQLRowMeta.CheckResult.QueryOK" ), transformMeta );
            remarks.add( cr );
          } else {
            error_message = BaseMessages.getString( PKG, "DynamicSQLRowMeta.CheckResult.InvalidDBQuery" );
            cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, transformMeta );
            remarks.add( cr );
          }
        }
      } catch ( HopException e ) {
        error_message =
          BaseMessages.getString( PKG, "DynamicSQLRowMeta.CheckResult.ErrorOccurred" ) + e.getMessage();
        cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, transformMeta );
        remarks.add( cr );
      } finally {
        db.disconnect();
      }
    } else {
      error_message = BaseMessages.getString( PKG, "DynamicSQLRowMeta.CheckResult.InvalidConnection" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, transformMeta );
      remarks.add( cr );
    }
  }

  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr, PipelineMeta tr,
                                Pipeline pipeline ) {
    return new DynamicSQLRow( transformMeta, this, data, cnr, tr, pipeline );
  }

  public ITransformData getTransformData() {
    return new DynamicSQLRowData();
  }

  public void analyseImpact( List<DatabaseImpact> impact, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                             IRowMeta prev, String[] input, String[] output, IRowMeta info,
                             IMetaStore metaStore ) throws HopTransformException {

    IRowMeta out = prev.clone();
    getFields( out, transformMeta.getName(), new IRowMeta[] { info, }, null, pipelineMeta, metaStore );
    if ( out != null ) {
      for ( int i = 0; i < out.size(); i++ ) {
        IValueMeta outvalue = out.getValueMeta( i );
        DatabaseImpact di =
          new DatabaseImpact(
            DatabaseImpact.TYPE_IMPACT_READ, pipelineMeta.getName(), transformMeta.getName(), databaseMeta
            .getDatabaseName(), "", outvalue.getName(), outvalue.getName(), transformMeta.getName(), sql,
            BaseMessages.getString( PKG, "DynamicSQLRowMeta.DatabaseImpact.Title" ) );
        impact.add( di );

      }
    }
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
}
