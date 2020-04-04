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

package org.apache.hop.pipeline.transforms.tableexists;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.util.Utils;
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
import org.w3c.dom.Node;

import java.util.List;

/*
 * Created on 03-Juin-2008
 *
 */

public class TableExistsMeta extends BaseTransformMeta implements TransformMetaInterface {
  private static Class<?> PKG = TableExistsMeta.class; // for i18n purposes, needed by Translator!!

  /**
   * database connection
   */
  private DatabaseMeta database;

  /**
   * dynamuc tablename
   */
  private String tablenamefield;

  /**
   * function result: new value name
   */
  private String resultfieldname;

  private String schemaname;

  public TableExistsMeta() {
    super(); // allocate BaseTransformMeta
  }

  /**
   * @return Returns the database.
   */
  public DatabaseMeta getDatabase() {
    return database;
  }

  /**
   * @param database The database to set.
   */
  public void setDatabase( DatabaseMeta database ) {
    this.database = database;
  }

  /**
   * @return Returns the tablenamefield.
   */
  public String getDynamicTablenameField() {
    return tablenamefield;
  }

  /**
   * @param tablenamefield The tablenamefield to set.
   */
  public void setDynamicTablenameField( String tablenamefield ) {
    this.tablenamefield = tablenamefield;
  }

  /**
   * @return Returns the resultName.
   */
  public String getResultFieldName() {
    return resultfieldname;
  }

  /**
   * @param resultfieldname The resultfieldname to set.
   */
  public void setResultFieldName( String resultfieldname ) {
    this.resultfieldname = resultfieldname;
  }

  public String getSchemaname() {
    return schemaname;
  }

  public void setSchemaname( String schemaname ) {
    this.schemaname = schemaname;
  }

  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode, metaStore );
  }

  public Object clone() {
    TableExistsMeta retval = (TableExistsMeta) super.clone();

    return retval;
  }

  public void setDefault() {
    database = null;
    schemaname = null;
    resultfieldname = "result";
  }

  public void getFields( IRowMeta inputRowMeta, String name, IRowMeta[] info, TransformMeta nextTransform,
                         iVariables variables, IMetaStore metaStore ) throws HopTransformException {
    // Output field (String)
    if ( !Utils.isEmpty( resultfieldname ) ) {
      IValueMeta v =
        new ValueMetaBoolean( variables.environmentSubstitute( resultfieldname ) );
      v.setOrigin( name );
      inputRowMeta.addValueMeta( v );
    }
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder();

    retval.append( "    " + XMLHandler.addTagValue( "connection", database == null ? "" : database.getName() ) );
    retval.append( "    " + XMLHandler.addTagValue( "tablenamefield", tablenamefield ) );
    retval.append( "    " + XMLHandler.addTagValue( "resultfieldname", resultfieldname ) );
    retval.append( "    " + XMLHandler.addTagValue( "schemaname", schemaname ) );

    return retval.toString();
  }

  private void readData( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    try {
      String con = XMLHandler.getTagValue( transformNode, "connection" );
      database = DatabaseMeta.loadDatabase( metaStore, con );
      tablenamefield = XMLHandler.getTagValue( transformNode, "tablenamefield" );
      resultfieldname = XMLHandler.getTagValue( transformNode, "resultfieldname" );
      schemaname = XMLHandler.getTagValue( transformNode, "schemaname" );

    } catch ( Exception e ) {
      throw new HopXMLException(
        BaseMessages.getString( PKG, "TableExistsMeta.Exception.UnableToReadTransformMeta" ), e );
    }
  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, iVariables variables,
                     IMetaStore metaStore ) {
    CheckResult cr;
    String error_message = "";

    if ( database == null ) {
      error_message = BaseMessages.getString( PKG, "TableExistsMeta.CheckResult.InvalidConnection" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, transformMeta );
      remarks.add( cr );
    }
    if ( Utils.isEmpty( resultfieldname ) ) {
      error_message = BaseMessages.getString( PKG, "TableExistsMeta.CheckResult.ResultFieldMissing" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, transformMeta );
      remarks.add( cr );
    } else {
      error_message = BaseMessages.getString( PKG, "TableExistsMeta.CheckResult.ResultFieldOK" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK, error_message, transformMeta );
      remarks.add( cr );
    }
    if ( Utils.isEmpty( tablenamefield ) ) {
      error_message = BaseMessages.getString( PKG, "TableExistsMeta.CheckResult.TableFieldMissing" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, transformMeta );
      remarks.add( cr );
    } else {
      error_message = BaseMessages.getString( PKG, "TableExistsMeta.CheckResult.TableFieldOK" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK, error_message, transformMeta );
      remarks.add( cr );
    }
    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "TableExistsMeta.CheckResult.ReceivingInfoFromOtherTransforms" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "TableExistsMeta.CheckResult.NoInpuReceived" ), transformMeta );
      remarks.add( cr );
    }

  }

  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr,
                                PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new TableExists( transformMeta, this, data, cnr, pipelineMeta, pipeline );
  }

  public ITransformData getTransformData() {
    return new TableExistsData();
  }

  public DatabaseMeta[] getUsedDatabaseConnections() {
    if ( database != null ) {
      return new DatabaseMeta[] { database };
    } else {
      return super.getUsedDatabaseConnections();
    }
  }

  public boolean supportsErrorHandling() {
    return true;
  }
}
