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

package org.apache.hop.pipeline.steps.dbproc;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.Step;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.step.*;
import org.w3c.dom.Node;

import java.util.List;

/*
 * Created on 26-apr-2003
 *
 */

@Step(
        id = "DBProc",
        image = "ui/images/PRC.svg",
        i18nPackageName = "i18n:org.apache.hop.pipeline.steps.dbproc",
        name = "BaseStep.TypeLongDesc.CallDBProcedure",
        description = "BaseStep.TypeTooltipDesc.CallDBProcedure",
        categoryDescription = "i18n:org.apache.hop.pipeline.step:BaseStep.Category.Lookup",
        documentationUrl = ""
)
public class DBProcMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = DBProcMeta.class; // for i18n purposes, needed by Translator!!

  /**
   * database connection
   */
  private DatabaseMeta database;

  /**
   * proc.-name to be called
   */
  private String procedure;

  /**
   * function arguments
   */
  private String[] argument;

  /**
   * IN / OUT / INOUT
   */
  private String[] argumentDirection;

  /**
   * value type for OUT
   */
  private int[] argumentType;

  /**
   * function result: new value name
   */
  private String resultName;

  /**
   * function result: new value type
   */
  private int resultType;

  /**
   * The flag to set auto commit on or off on the connection
   */
  private boolean autoCommit;

  public DBProcMeta() {
    super(); // allocate BaseStepMeta
  }

  /**
   * @return Returns the argument.
   */
  public String[] getArgument() {
    return argument;
  }

  /**
   * @param argument The argument to set.
   */
  public void setArgument( String[] argument ) {
    this.argument = argument;
  }

  /**
   * @return Returns the argumentDirection.
   */
  public String[] getArgumentDirection() {
    return argumentDirection;
  }

  /**
   * @param argumentDirection The argumentDirection to set.
   */
  public void setArgumentDirection( String[] argumentDirection ) {
    this.argumentDirection = argumentDirection;
  }

  /**
   * @return Returns the argumentType.
   */
  public int[] getArgumentType() {
    return argumentType;
  }

  /**
   * @param argumentType The argumentType to set.
   */
  public void setArgumentType( int[] argumentType ) {
    this.argumentType = argumentType;
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
   * @return Returns the procedure.
   */
  public String getProcedure() {
    return procedure;
  }

  /**
   * @param procedure The procedure to set.
   */
  public void setProcedure( String procedure ) {
    this.procedure = procedure;
  }

  /**
   * @return Returns the resultName.
   */
  public String getResultName() {
    return resultName;
  }

  /**
   * @param resultName The resultName to set.
   */
  public void setResultName( String resultName ) {
    this.resultName = resultName;
  }

  /**
   * @return Returns the resultType.
   */
  public int getResultType() {
    return resultType;
  }

  /**
   * @param resultType The resultType to set.
   */
  public void setResultType( int resultType ) {
    this.resultType = resultType;
  }

  /**
   * @return Returns the autoCommit.
   */
  public boolean isAutoCommit() {
    return autoCommit;
  }

  /**
   * @param autoCommit The autoCommit to set.
   */
  public void setAutoCommit( boolean autoCommit ) {
    this.autoCommit = autoCommit;
  }

  public void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    readData( stepnode, metaStore );
  }

  public void allocate( int nrargs ) {
    argument = new String[ nrargs ];
    argumentDirection = new String[ nrargs ];
    argumentType = new int[ nrargs ];
  }

  public Object clone() {
    DBProcMeta retval = (DBProcMeta) super.clone();
    int nrargs = argument.length;

    retval.allocate( nrargs );

    System.arraycopy( argument, 0, retval.argument, 0, nrargs );
    System.arraycopy( argumentDirection, 0, retval.argumentDirection, 0, nrargs );
    System.arraycopy( argumentType, 0, retval.argumentType, 0, nrargs );

    return retval;
  }

  public void setDefault() {
    int i;
    int nrargs;

    database = null;

    nrargs = 0;

    allocate( nrargs );

    for ( i = 0; i < nrargs; i++ ) {
      argument[ i ] = "arg" + i;
      argumentDirection[ i ] = "IN";
      argumentType[ i ] = ValueMetaInterface.TYPE_NUMBER;
    }

    resultName = "result";
    resultType = ValueMetaInterface.TYPE_NUMBER;
    autoCommit = true;
  }

  @Override
  public void getFields( RowMetaInterface r, String name, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space, IMetaStore metaStore ) throws HopStepException {

    if ( !Utils.isEmpty( resultName ) ) {
      ValueMetaInterface v;
      try {
        v = ValueMetaFactory.createValueMeta( resultName, resultType );
        v.setOrigin( name );
        r.addValueMeta( v );
      } catch ( HopPluginException e ) {
        throw new HopStepException( e );
      }
    }

    for ( int i = 0; i < argument.length; i++ ) {
      if ( argumentDirection[ i ].equalsIgnoreCase( "OUT" ) ) {
        ValueMetaInterface v;
        try {
          v = ValueMetaFactory.createValueMeta( argument[ i ], argumentType[ i ] );
          v.setOrigin( name );
          r.addValueMeta( v );
        } catch ( HopPluginException e ) {
          throw new HopStepException( e );
        }
      }
    }

    return;
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder( 500 );

    retval
      .append( "    " ).append( XMLHandler.addTagValue( "connection", database == null ? "" : database.getName() ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "procedure", procedure ) );
    retval.append( "    <lookup>" ).append( Const.CR );

    for ( int i = 0; i < argument.length; i++ ) {
      retval.append( "      <arg>" ).append( Const.CR );
      retval.append( "        " ).append( XMLHandler.addTagValue( "name", argument[ i ] ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "direction", argumentDirection[ i ] ) );
      retval.append( "        " ).append(
        XMLHandler.addTagValue( "type", ValueMetaFactory.getValueMetaName( argumentType[ i ] ) ) );
      retval.append( "      </arg>" ).append( Const.CR );
    }

    retval.append( "    </lookup>" ).append( Const.CR );

    retval.append( "    <result>" ).append( Const.CR );
    retval.append( "      " ).append( XMLHandler.addTagValue( "name", resultName ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "type",
      ValueMetaFactory.getValueMetaName( resultType ) ) );
    retval.append( "    </result>" ).append( Const.CR );

    retval.append( "    " ).append( XMLHandler.addTagValue( "auto_commit", autoCommit ) );

    return retval.toString();
  }

  private void readData( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    try {
      int i;
      int nrargs;

      String con = XMLHandler.getTagValue( stepnode, "connection" );
      database = DatabaseMeta.loadDatabase( metaStore, con );
      procedure = XMLHandler.getTagValue( stepnode, "procedure" );

      Node lookup = XMLHandler.getSubNode( stepnode, "lookup" );
      nrargs = XMLHandler.countNodes( lookup, "arg" );

      allocate( nrargs );

      for ( i = 0; i < nrargs; i++ ) {
        Node anode = XMLHandler.getSubNodeByNr( lookup, "arg", i );

        argument[ i ] = XMLHandler.getTagValue( anode, "name" );
        argumentDirection[ i ] = XMLHandler.getTagValue( anode, "direction" );
        argumentType[ i ] = ValueMetaFactory.getIdForValueMeta( XMLHandler.getTagValue( anode, "type" ) );
      }

      resultName = XMLHandler.getTagValue( stepnode, "result", "name" ); // Optional, can be null
      //
      resultType = ValueMetaFactory.getIdForValueMeta( XMLHandler.getTagValue( stepnode, "result", "type" ) );
      autoCommit = !"N".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "auto_commit" ) );
    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString( PKG, "DBProcMeta.Exception.UnableToReadStepInfo" ), e );
    }
  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, StepMeta stepMeta,
                     RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                     IMetaStore metaStore ) {
    CheckResult cr;
    String error_message = "";

    if ( database != null ) {
      Database db = new Database( pipelineMeta, database );
      try {
        db.connect();

        // Look up fields in the input stream <prev>
        if ( prev != null && prev.size() > 0 ) {
          boolean first = true;
          error_message = "";
          boolean error_found = false;

          for ( int i = 0; i < argument.length; i++ ) {
            ValueMetaInterface v = prev.searchValueMeta( argument[ i ] );
            if ( v == null ) {
              if ( first ) {
                first = false;
                error_message +=
                  BaseMessages.getString( PKG, "DBProcMeta.CheckResult.MissingArguments" ) + Const.CR;
              }
              error_found = true;
              error_message += "\t\t" + argument[ i ] + Const.CR;
            } else {
              // Argument exists in input stream: same type?

              if ( v.getType() != argumentType[ i ] && !( v.isNumeric() && ValueMetaBase.isNumeric( argumentType[ i ] ) ) ) {
                error_found = true;
                error_message +=
                  "\t\t"
                    + argument[ i ]
                    + BaseMessages.getString(
                    PKG, "DBProcMeta.CheckResult.WrongTypeArguments", v.getTypeDesc(),
                    ValueMetaFactory.getValueMetaName( argumentType[ i ] ) ) + Const.CR;
              }
            }
          }
          if ( error_found ) {
            cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta );
          } else {
            cr =
              new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
                PKG, "DBProcMeta.CheckResult.AllArgumentsOK" ), stepMeta );
          }
          remarks.add( cr );
        } else {
          error_message = BaseMessages.getString( PKG, "DBProcMeta.CheckResult.CouldNotReadFields" ) + Const.CR;
          cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta );
          remarks.add( cr );
        }
      } catch ( HopException e ) {
        error_message = BaseMessages.getString( PKG, "DBProcMeta.CheckResult.ErrorOccurred" ) + e.getMessage();
        cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta );
        remarks.add( cr );
      }
    } else {
      error_message = BaseMessages.getString( PKG, "DBProcMeta.CheckResult.InvalidConnection" );
      cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "DBProcMeta.CheckResult.ReceivingInfoFromOtherSteps" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "DBProcMeta.CheckResult.NoInpuReceived" ), stepMeta );
      remarks.add( cr );
    }

  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr,
                                PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new DBProc( stepMeta, stepDataInterface, cnr, pipelineMeta, pipeline );
  }

  public StepDataInterface getStepData() {
    return new DBProcData();
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

  @Override
  public String getDialogClassName(){
    return DBProcDialog.class.getName();
  }
}
