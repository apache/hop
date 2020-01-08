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

package org.apache.hop.trans.steps.columnexists;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.annotations.Step;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStepMeta;
import org.apache.hop.trans.step.StepDataInterface;
import org.apache.hop.trans.step.StepInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.StepMetaInterface;
import org.w3c.dom.Node;

import java.util.List;

/*
 * Created on 03-Juin-2008
 *
 */

@Step( id = "ColumnExists", i18nPackageName = "org.apache.hop.trans.steps.columnexists", name = "ColumnExists.Name",
  description = "ColumnExists.Description",
  categoryDescription = "i18n:org.apache.hop.trans.step:BaseStep.Category.Lookup" )
public class ColumnExistsMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = ColumnExistsMeta.class; // for i18n purposes, needed by Translator2!!

  /**
   * database connection
   */
  private DatabaseMeta database;

  private String schemaname;

  private String tablename;

  /**
   * dynamic tablename
   */
  private String tablenamefield;

  /**
   * dynamic columnname
   */
  private String columnnamefield;

  /**
   * function result: new value name
   */
  private String resultfieldname;

  private boolean istablenameInfield;

  public ColumnExistsMeta() {
    super(); // allocate BaseStepMeta
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
   * @return Returns the tablename.
   */
  public String getTablename() {
    return tablename;
  }

  /**
   * @param tablename The tablename to set.
   */
  public void setTablename( String tablename ) {
    this.tablename = tablename;
  }

  /**
   * @return Returns the schemaname.
   */
  public String getSchemaname() {
    return schemaname;
  }

  /**
   * @param schemaname The schemaname to set.
   */
  public void setSchemaname( String schemaname ) {
    this.schemaname = schemaname;
  }

  /**
   * @param tablenamefield The tablenamefield to set.
   */
  public void setDynamicTablenameField( String tablenamefield ) {
    this.tablenamefield = tablenamefield;
  }

  /**
   * @return Returns the columnnamefield.
   */
  public String getDynamicColumnnameField() {
    return columnnamefield;
  }

  /**
   * @param columnnamefield The columnnamefield to set.
   */
  public void setDynamicColumnnameField( String columnnamefield ) {
    this.columnnamefield = columnnamefield;
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

  public boolean isTablenameInField() {
    return istablenameInfield;
  }

  /**
   * @param isTablenameInField the isTablenameInField to set
   */
  public void setTablenameInField( boolean isTablenameInField ) {
    this.istablenameInfield = isTablenameInField;
  }

  public void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    readData( stepnode, metaStore );
  }

  public Object clone() {
    ColumnExistsMeta retval = (ColumnExistsMeta) super.clone();

    return retval;
  }

  public void setDefault() {
    database = null;
    schemaname = null;
    tablename = null;
    istablenameInfield = false;
    resultfieldname = "result";
  }

  public void getFields( RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space, IMetaStore metaStore )
    throws HopStepException {
    // Output field (String)
    if ( !Utils.isEmpty( resultfieldname ) ) {
      ValueMetaInterface v = new ValueMetaBoolean( space.environmentSubstitute( resultfieldname ) );
      v.setOrigin( name );
      inputRowMeta.addValueMeta( v );
    }
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder();
    retval.append( "    " + XMLHandler.addTagValue( "connection", database == null ? "" : database.getName() ) );
    retval.append( "    " + XMLHandler.addTagValue( "tablename", tablename ) );
    retval.append( "    " + XMLHandler.addTagValue( "schemaname", schemaname ) );
    retval.append( "    " + XMLHandler.addTagValue( "istablenameInfield", istablenameInfield ) );
    retval.append( "    " + XMLHandler.addTagValue( "tablenamefield", tablenamefield ) );
    retval.append( "    " + XMLHandler.addTagValue( "columnnamefield", columnnamefield ) );
    retval.append( "      " + XMLHandler.addTagValue( "resultfieldname", resultfieldname ) );
    return retval.toString();
  }

  private void readData( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    try {
      String con = XMLHandler.getTagValue( stepnode, "connection" );
      database = DatabaseMeta.loadDatabase( metaStore, con );
      tablename = XMLHandler.getTagValue( stepnode, "tablename" );
      schemaname = XMLHandler.getTagValue( stepnode, "schemaname" );
      istablenameInfield = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "istablenameInfield" ) );
      tablenamefield = XMLHandler.getTagValue( stepnode, "tablenamefield" );
      columnnamefield = XMLHandler.getTagValue( stepnode, "columnnamefield" );
      resultfieldname = XMLHandler.getTagValue( stepnode, "resultfieldname" ); // Optional, can be null
    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString( PKG, "ColumnExistsMeta.Exception.UnableToReadStepInfo" ),
        e );
    }
  }

  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev,
                     String[] input, String[] output, RowMetaInterface info, VariableSpace space, IMetaStore metaStore ) {
    CheckResult cr;
    String error_message = "";

    if ( database == null ) {
      error_message = BaseMessages.getString( PKG, "ColumnExistsMeta.CheckResult.InvalidConnection" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, stepMeta );
      remarks.add( cr );
    }
    if ( Utils.isEmpty( resultfieldname ) ) {
      error_message = BaseMessages.getString( PKG, "ColumnExistsMeta.CheckResult.ResultFieldMissing" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, stepMeta );
    } else {
      error_message = BaseMessages.getString( PKG, "ColumnExistsMeta.CheckResult.ResultFieldOK" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK, error_message, stepMeta );
    }
    remarks.add( cr );
    if ( istablenameInfield ) {
      if ( Utils.isEmpty( tablenamefield ) ) {
        error_message = BaseMessages.getString( PKG, "ColumnExistsMeta.CheckResult.TableFieldMissing" );
        cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, stepMeta );
      } else {
        error_message = BaseMessages.getString( PKG, "ColumnExistsMeta.CheckResult.TableFieldOK" );
        cr = new CheckResult( CheckResult.TYPE_RESULT_OK, error_message, stepMeta );
      }
      remarks.add( cr );
    } else {
      if ( Utils.isEmpty( tablename ) ) {
        error_message = BaseMessages.getString( PKG, "ColumnExistsMeta.CheckResult.TablenameMissing" );
        cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, stepMeta );
      } else {
        error_message = BaseMessages.getString( PKG, "ColumnExistsMeta.CheckResult.TablenameOK" );
        cr = new CheckResult( CheckResult.TYPE_RESULT_OK, error_message, stepMeta );
      }
      remarks.add( cr );
    }

    if ( Utils.isEmpty( columnnamefield ) ) {
      error_message = BaseMessages.getString( PKG, "ColumnExistsMeta.CheckResult.ColumnNameFieldMissing" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, stepMeta );
    } else {
      error_message = BaseMessages.getString( PKG, "ColumnExistsMeta.CheckResult.ColumnNameFieldOK" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK, error_message, stepMeta );
    }
    remarks.add( cr );

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK,
          BaseMessages.getString( PKG, "ColumnExistsMeta.CheckResult.ReceivingInfoFromOtherSteps" ), stepMeta );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR,
          BaseMessages.getString( PKG, "ColumnExistsMeta.CheckResult.NoInpuReceived" ), stepMeta );
    }
    remarks.add( cr );
  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta,
                                Trans trans ) {
    return new ColumnExists( stepMeta, stepDataInterface, cnr, transMeta, trans );
  }

  public StepDataInterface getStepData() {
    return new ColumnExistsData();
  }

  public DatabaseMeta[] getUsedDatabaseConnections() {
    if ( database != null ) {
      return new DatabaseMeta[] {
        database };
    } else {
      return super.getUsedDatabaseConnections();
    }
  }

  public boolean supportsErrorHandling() {
    return true;
  }
}
