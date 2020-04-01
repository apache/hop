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

package org.apache.hop.pipeline.steps.terafast;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.annotations.Step;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.util.*;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.step.StepDataInterface;
import org.apache.hop.pipeline.step.StepInterface;
import org.apache.hop.pipeline.step.StepMeta;

import java.util.List;

/**
 * @author <a href="mailto:michael.gugerell@aschauer-edv.at">Michael Gugerell(asc145)</a>
 */
@Step(
		  id = "TeraFast",
		  i18nPackageName = "org.apache.hop.pipeline.steps.terafast",
		  description = "TeraFast.Description",
		  name = "TeraFast.Name",
		  categoryDescription = "BaseStep.Category.Bulk",
		  image = "TeraFast.svg",
		  documentationUrl = "http://wiki.pentaho.com/display/EAI/Teradata+Fastload+Bulk+Loader"
)
public class TeraFastMeta extends AbstractStepMeta {

  public static final PluginMessages MESSAGES = PluginMessages.getMessages( TeraFastMeta.class );

  /**
   * Default fast load path.
   */
  public static final String DEFAULT_FASTLOAD_PATH = "/usr/bin/fastload";

  /**
   * Default data file.
   */
  public static final String DEFAULT_DATA_FILE = "${Internal.Step.CopyNr}.dat";

  public static final String DEFAULT_TARGET_TABLE = "${TARGET_TABLE}_${RUN_ID}";

  /**
   * Default session.
   */
  public static final int DEFAULT_SESSIONS = 2;

  public static final boolean DEFAULT_TRUNCATETABLE = true;

  public static final boolean DEFAULT_VARIABLE_SUBSTITUTION = true;

  /**
   * Default error limit.
   */
  public static final int DEFAULT_ERROR_LIMIT = 25;

  /* custom xml values */
  private static final String FASTLOADPATH = "fastload_path";

  private static final String CONTROLFILE = "controlfile_path";

  private static final String DATAFILE = "datafile_path";

  private static final String LOGFILE = "logfile_path";

  private static final String SESSIONS = "sessions";

  private static final String ERRORLIMIT = "error_limit";

  private static final String USECONTROLFILE = "use_control_file";

  private static final String TARGETTABLE = "target_table";

  private static final String TRUNCATETABLE = "truncate_table";

  private static final String TABLE_FIELD_LIST = "table_field_list";

  private static final String STREAM_FIELD_LIST = "stream_field_list";

  private static final String VARIABLE_SUBSTITUTION = "variable_substitution";

  /**
   * available options.
   **/
  private StringPluginProperty fastloadPath;

  private StringPluginProperty controlFile;

  private StringPluginProperty dataFile;

  private StringPluginProperty logFile;

  private IntegerPluginProperty sessions;

  private IntegerPluginProperty errorLimit;

  private BooleanPluginProperty useControlFile;

  private BooleanPluginProperty variableSubstitution;

  private BooleanPluginProperty truncateTable;

  private StringPluginProperty targetTable;

  private StringListPluginProperty tableFieldList;

  private StringListPluginProperty streamFieldList;

  /**
   *
   */
  public TeraFastMeta() {
    super();
    this.fastloadPath = this.getPropertyFactory().createString( FASTLOADPATH );
    this.controlFile = this.getPropertyFactory().createString( CONTROLFILE );
    this.dataFile = this.getPropertyFactory().createString( DATAFILE );
    this.logFile = this.getPropertyFactory().createString( LOGFILE );
    this.sessions = this.getPropertyFactory().createInteger( SESSIONS );
    this.errorLimit = this.getPropertyFactory().createInteger( ERRORLIMIT );
    this.targetTable = this.getPropertyFactory().createString( TARGETTABLE );
    this.useControlFile = this.getPropertyFactory().createBoolean( USECONTROLFILE );
    this.truncateTable = this.getPropertyFactory().createBoolean( TRUNCATETABLE );
    this.tableFieldList = this.getPropertyFactory().createStringList( TABLE_FIELD_LIST );
    this.streamFieldList = this.getPropertyFactory().createStringList( STREAM_FIELD_LIST );
    this.variableSubstitution = this.getPropertyFactory().createBoolean( VARIABLE_SUBSTITUTION );
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.hop.pipeline.step.StepMetaInterface#check(List, PipelineMeta,
   * StepMeta, RowMetaInterface, String[],
   * String[], RowMetaInterface)
   */
  public void check( final List<CheckResultInterface> remarks, final PipelineMeta transmeta, final StepMeta stepMeta,
                     final RowMetaInterface prev, final String[] input, final String[] output, final RowMetaInterface info,
                     VariableSpace space, IMetaStore metaStore ) {
    CheckResult checkResult;
    try {
      RowMetaInterface tableFields = getRequiredFields( transmeta );
      checkResult =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, MESSAGES
          .getString( "TeraFastMeta.Message.ConnectionEstablished" ), stepMeta );
      remarks.add( checkResult );

      checkResult =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, MESSAGES
          .getString( "TeraFastMeta.Message.TableExists" ), stepMeta );
      remarks.add( checkResult );

      boolean error = false;
      for ( String field : this.tableFieldList.getValue() ) {
        if ( tableFields.searchValueMeta( field ) == null ) {
          error = true;
          checkResult =
            new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, MESSAGES
              .getString( "TeraFastMeta.Exception.TableFieldNotFound" ), stepMeta );
          remarks.add( checkResult );
        }
      }
      if ( !error ) {
        checkResult =
          new CheckResult( CheckResultInterface.TYPE_RESULT_OK, MESSAGES
            .getString( "TeraFastMeta.Message.AllTableFieldsFound" ), stepMeta );
        remarks.add( checkResult );
      }
      if ( prev != null && prev.size() > 0 ) {
        // step mode. step receiving input
        checkResult =
          new CheckResult( CheckResultInterface.TYPE_RESULT_OK, MESSAGES
            .getString( "TeraFastMeta.Message.StepInputDataFound" ), stepMeta );
        remarks.add( checkResult );

        error = false;
        for ( String field : this.streamFieldList.getValue() ) {
          if ( prev.searchValueMeta( field ) == null ) {
            error = true;
            checkResult =
              new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, MESSAGES
                .getString( "TeraFastMeta.Exception.StreamFieldNotFound" ), stepMeta );
            remarks.add( checkResult );
          }
        }
        if ( !error ) {
          checkResult =
            new CheckResult( CheckResultInterface.TYPE_RESULT_OK, MESSAGES
              .getString( "TeraFastMeta.Message.AllStreamFieldsFound" ), stepMeta );
          remarks.add( checkResult );
        }
      }
    } catch ( HopDatabaseException e ) {
      checkResult =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, MESSAGES
          .getString( "TeraFastMeta.Exception.ConnectionFailed" ), stepMeta );
      remarks.add( checkResult );
    } catch ( HopException e ) {
      checkResult = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, e.getMessage(), stepMeta );
      remarks.add( checkResult );
    }
  }

  /**
   * @return the database.
   * @throws HopException if an error occurs.
   */
  public Database connectToDatabase() throws HopException {
    if ( this.getDbMeta() != null ) {
      Database db = new Database( loggingObject, this.getDbMeta() );
      db.connect();
      return db;
    }
    throw new HopException( MESSAGES.getString( "TeraFastMeta.Exception.ConnectionNotDefined" ) );
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.hop.pipeline.step.StepMetaInterface#getStep(StepMeta,
   * StepDataInterface, int, PipelineMeta, Pipeline)
   */
  public StepInterface getStep( final StepMeta stepMeta, final StepDataInterface stepDataInterface, final int cnr,
                                final PipelineMeta pipelineMeta, final Pipeline disp ) {
    return new TeraFast( stepMeta, stepDataInterface, cnr, pipelineMeta, disp );
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.hop.pipeline.step.StepMetaInterface#getStepData()
   */
  @Override
  public StepDataInterface getStepData() {
    return new GenericStepData();
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.hop.pipeline.step.StepMetaInterface#setDefault()
   */
  public void setDefault() {
    this.fastloadPath.setValue( DEFAULT_FASTLOAD_PATH );
    this.dataFile.setValue( DEFAULT_DATA_FILE );
    this.sessions.setValue( DEFAULT_SESSIONS );
    this.errorLimit.setValue( DEFAULT_ERROR_LIMIT );
    this.truncateTable.setValue( DEFAULT_TRUNCATETABLE );
    this.variableSubstitution.setValue( DEFAULT_VARIABLE_SUBSTITUTION );
    this.targetTable.setValue( DEFAULT_TARGET_TABLE );
    this.useControlFile.setValue( true );
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.hop.pipeline.step.BaseStepMeta#getFields(RowMetaInterface, String,
   * RowMetaInterface[], StepMeta,
   * VariableSpace)
   */
  @Override
  public void getFields( final RowMetaInterface inputRowMeta, final String name, final RowMetaInterface[] info,
                         final StepMeta nextStep, final VariableSpace space, IMetaStore metaStore ) throws HopStepException {
    // Default: nothing changes to rowMeta
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.hop.pipeline.step.BaseStepMeta#getRequiredFields(VariableSpace)
   */
  @Override
  public RowMetaInterface getRequiredFields( final VariableSpace space ) throws HopException {
    if ( !this.useControlFile.getValue() ) {
      final Database database = connectToDatabase();
      database.shareVariablesWith( space );

      RowMetaInterface fields =
        database.getTableFieldsMeta(
          StringUtils.EMPTY,
          space.environmentSubstitute( this.targetTable.getValue() ) );
      database.disconnect();
      if ( fields == null ) {
        throw new HopException( MESSAGES.getString( "TeraFastMeta.Exception.TableNotFound" ) );
      }
      return fields;
    }
    return null;
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.hop.pipeline.step.BaseStepMeta#clone()
   */
  @Override
  public Object clone() {
    return super.clone();
  }

  /**
   * @return the fastloadPath
   */
  public StringPluginProperty getFastloadPath() {
    return this.fastloadPath;
  }

  /**
   * @param fastloadPath the fastloadPath to set
   */
  public void setFastloadPath( final StringPluginProperty fastloadPath ) {
    this.fastloadPath = fastloadPath;
  }

  /**
   * @return the controlFile
   */
  public StringPluginProperty getControlFile() {
    return this.controlFile;
  }

  /**
   * @param controlFile the controlFile to set
   */
  public void setControlFile( final StringPluginProperty controlFile ) {
    this.controlFile = controlFile;
  }

  /**
   * @return the dataFile
   */
  public StringPluginProperty getDataFile() {
    return this.dataFile;
  }

  /**
   * @param dataFile the dataFile to set
   */
  public void setDataFile( final StringPluginProperty dataFile ) {
    this.dataFile = dataFile;
  }

  /**
   * @return the logFile
   */
  public StringPluginProperty getLogFile() {
    return this.logFile;
  }

  /**
   * @param logFile the logFile to set
   */
  public void setLogFile( final StringPluginProperty logFile ) {
    this.logFile = logFile;
  }

  /**
   * @return the sessions
   */
  public IntegerPluginProperty getSessions() {
    return this.sessions;
  }

  /**
   * @param sessions the sessions to set
   */
  public void setSessions( final IntegerPluginProperty sessions ) {
    this.sessions = sessions;
  }

  /**
   * @return the errorLimit
   */
  public IntegerPluginProperty getErrorLimit() {
    return this.errorLimit;
  }

  /**
   * @param errorLimit the errorLimit to set
   */
  public void setErrorLimit( final IntegerPluginProperty errorLimit ) {
    this.errorLimit = errorLimit;
  }

  /**
   * @return the useControlFile
   */
  public BooleanPluginProperty getUseControlFile() {
    return this.useControlFile;
  }

  /**
   * @param useControlFile the useControlFile to set
   */
  public void setUseControlFile( final BooleanPluginProperty useControlFile ) {
    this.useControlFile = useControlFile;
  }

  /**
   * @return the targetTable
   */
  public StringPluginProperty getTargetTable() {
    return this.targetTable;
  }

  /**
   * @param targetTable the targetTable to set
   */
  public void setTargetTable( final StringPluginProperty targetTable ) {
    this.targetTable = targetTable;
  }

  /**
   * @return the truncateTable
   */
  public BooleanPluginProperty getTruncateTable() {
    return this.truncateTable;
  }

  /**
   * @param truncateTable the truncateTable to set
   */
  public void setTruncateTable( final BooleanPluginProperty truncateTable ) {
    this.truncateTable = truncateTable;
  }

  /**
   * @return the tableFieldList
   */
  public StringListPluginProperty getTableFieldList() {
    return this.tableFieldList;
  }

  /**
   * @param tableFieldList the tableFieldList to set
   */
  public void setTableFieldList( final StringListPluginProperty tableFieldList ) {
    this.tableFieldList = tableFieldList;
  }

  /**
   * @return the streamFieldList
   */
  public StringListPluginProperty getStreamFieldList() {
    return this.streamFieldList;
  }

  /**
   * @param streamFieldList the streamFieldList to set
   */
  public void setStreamFieldList( final StringListPluginProperty streamFieldList ) {
    this.streamFieldList = streamFieldList;
  }

  /**
   * @return the variableSubstitution
   */
  public BooleanPluginProperty getVariableSubstitution() {
    return this.variableSubstitution;
  }

  /**
   * @param variableSubstitution the variableSubstitution to set
   */
  public void setVariableSubstitution( BooleanPluginProperty variableSubstitution ) {
    this.variableSubstitution = variableSubstitution;
  }

}
