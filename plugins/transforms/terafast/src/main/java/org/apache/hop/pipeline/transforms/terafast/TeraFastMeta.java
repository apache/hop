/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.terafast;

import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.ActionTransformType;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.AbstractTransformMeta;
import org.apache.hop.core.util.BooleanPluginProperty;
import org.apache.hop.core.util.IntegerPluginProperty;
import org.apache.hop.core.util.PluginMessages;
import org.apache.hop.core.util.StringListPluginProperty;
import org.apache.hop.core.util.StringPluginProperty;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "TeraFast",
    image = "terafast.svg",
    description = "i18n::TeraFast.Description",
    name = "i18n::TeraFast.Name",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Bulk",
    keywords = "i18n::TeraFastMeta.keyword",
    documentationUrl = "/pipeline/transforms/terafast.html",
    actionTransformTypes = {ActionTransformType.RDBMS, ActionTransformType.OUTPUT})
public class TeraFastMeta extends AbstractTransformMeta<ITransform, ITransformData> {

  public static final PluginMessages MESSAGES = PluginMessages.getMessages(TeraFastMeta.class);

  /** Default fast load path. */
  public static final String DEFAULT_FASTLOAD_PATH = "/usr/bin/fastload";

  /** Default data file. */
  public static final String DEFAULT_DATA_FILE = "${Internal.Transform.CopyNr}.dat";

  public static final String DEFAULT_TARGET_TABLE = "${TARGET_TABLE}_${RUN_ID}";

  /** Default session. */
  public static final int DEFAULT_SESSIONS = 2;

  public static final boolean DEFAULT_TRUNCATETABLE = true;

  public static final boolean DEFAULT_VARIABLE_SUBSTITUTION = true;

  /** Default error limit. */
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

  /** available options. */
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

  /** */
  public TeraFastMeta() {
    super();
    this.fastloadPath = this.getPropertyFactory().createString(FASTLOADPATH);
    this.controlFile = this.getPropertyFactory().createString(CONTROLFILE);
    this.dataFile = this.getPropertyFactory().createString(DATAFILE);
    this.logFile = this.getPropertyFactory().createString(LOGFILE);
    this.sessions = this.getPropertyFactory().createInteger(SESSIONS);
    this.errorLimit = this.getPropertyFactory().createInteger(ERRORLIMIT);
    this.targetTable = this.getPropertyFactory().createString(TARGETTABLE);
    this.useControlFile = this.getPropertyFactory().createBoolean(USECONTROLFILE);
    this.truncateTable = this.getPropertyFactory().createBoolean(TRUNCATETABLE);
    this.tableFieldList = this.getPropertyFactory().createStringList(TABLE_FIELD_LIST);
    this.streamFieldList = this.getPropertyFactory().createStringList(STREAM_FIELD_LIST);
    this.variableSubstitution = this.getPropertyFactory().createBoolean(VARIABLE_SUBSTITUTION);
  }

  @Override
  public void check(
      final List<ICheckResult> remarks,
      final PipelineMeta pipelineMeta,
      final TransformMeta transformMeta,
      final IRowMeta prev,
      final String[] input,
      final String[] output,
      final IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    CheckResult checkResult;
    try {
      IRowMeta tableFields = getRequiredFields(variables);
      checkResult =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              MESSAGES.getString("TeraFastMeta.Message.ConnectionEstablished"),
              transformMeta);
      remarks.add(checkResult);

      checkResult =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              MESSAGES.getString("TeraFastMeta.Message.TableExists"),
              transformMeta);
      remarks.add(checkResult);

      boolean error = false;
      for (String field : this.tableFieldList.getValue()) {
        if (tableFields.searchValueMeta(field) == null) {
          error = true;
          checkResult =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_ERROR,
                  MESSAGES.getString("TeraFastMeta.Exception.TableFieldNotFound"),
                  transformMeta);
          remarks.add(checkResult);
        }
      }
      if (!error) {
        checkResult =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                MESSAGES.getString("TeraFastMeta.Message.AllTableFieldsFound"),
                transformMeta);
        remarks.add(checkResult);
      }
      if (prev != null && prev.size() > 0) {
        // transform mode. transform receiving input
        checkResult =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                MESSAGES.getString("TeraFastMeta.Message.TransformInputDataFound"),
                transformMeta);
        remarks.add(checkResult);

        error = false;
        for (String field : this.streamFieldList.getValue()) {
          if (prev.searchValueMeta(field) == null) {
            error = true;
            checkResult =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_ERROR,
                    MESSAGES.getString("TeraFastMeta.Exception.StreamFieldNotFound"),
                    transformMeta);
            remarks.add(checkResult);
          }
        }
        if (!error) {
          checkResult =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
                  MESSAGES.getString("TeraFastMeta.Message.AllStreamFieldsFound"),
                  transformMeta);
          remarks.add(checkResult);
        }
      }
    } catch (HopDatabaseException e) {
      checkResult =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              MESSAGES.getString("TeraFastMeta.Exception.ConnectionFailed"),
              transformMeta);
      remarks.add(checkResult);
    } catch (HopException e) {
      checkResult = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, e.getMessage(), transformMeta);
      remarks.add(checkResult);
    }
  }

  /**
   * @return the database.
   * @throws HopException if an error occurs.
   */
  public Database connectToDatabase(IVariables variables) throws HopException {
    if (this.getDbMeta() != null) {
      Database db = new Database(loggingObject, variables, this.getDbMeta());
      db.connect();
      return db;
    }
    throw new HopException(MESSAGES.getString("TeraFastMeta.Exception.ConnectionNotDefined"));
  }

  /**
   * {@inheritDoc}
   *
   * @see ITransformMeta#setDefault()
   */
  @Override
  public void setDefault() {
    this.fastloadPath.setValue(DEFAULT_FASTLOAD_PATH);
    this.dataFile.setValue(DEFAULT_DATA_FILE);
    this.sessions.setValue(DEFAULT_SESSIONS);
    this.errorLimit.setValue(DEFAULT_ERROR_LIMIT);
    this.truncateTable.setValue(DEFAULT_TRUNCATETABLE);
    this.variableSubstitution.setValue(DEFAULT_VARIABLE_SUBSTITUTION);
    this.targetTable.setValue(DEFAULT_TARGET_TABLE);
    this.useControlFile.setValue(true);
  }

  @Override
  public void getFields(
      final IRowMeta inputRowMeta,
      final String name,
      final IRowMeta[] info,
      final TransformMeta nextTransform,
      final IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    // Default: nothing changes to rowMeta
  }

  /**
   * {@inheritDoc}
   *
   * @see BaseTransformMeta#getRequiredFields(IVariables)
   */
  @Override
  public IRowMeta getRequiredFields(final IVariables variables) throws HopException {
    if (!this.useControlFile.getValue()) {
      final Database database = connectToDatabase(variables);
      IRowMeta fields =
          database.getTableFieldsMeta(
              StringUtils.EMPTY, variables.resolve(this.targetTable.getValue()));
      database.disconnect();
      if (fields == null) {
        throw new HopException(MESSAGES.getString("TeraFastMeta.Exception.TableNotFound"));
      }
      return fields;
    }
    return null;
  }

  /**
   * {@inheritDoc}
   *
   * @see BaseTransformMeta#clone()
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
  public void setFastloadPath(final StringPluginProperty fastloadPath) {
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
  public void setControlFile(final StringPluginProperty controlFile) {
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
  public void setDataFile(final StringPluginProperty dataFile) {
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
  public void setLogFile(final StringPluginProperty logFile) {
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
  public void setSessions(final IntegerPluginProperty sessions) {
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
  public void setErrorLimit(final IntegerPluginProperty errorLimit) {
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
  public void setUseControlFile(final BooleanPluginProperty useControlFile) {
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
  public void setTargetTable(final StringPluginProperty targetTable) {
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
  public void setTruncateTable(final BooleanPluginProperty truncateTable) {
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
  public void setTableFieldList(final StringListPluginProperty tableFieldList) {
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
  public void setStreamFieldList(final StringListPluginProperty streamFieldList) {
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
  public void setVariableSubstitution(BooleanPluginProperty variableSubstitution) {
    this.variableSubstitution = variableSubstitution;
  }
}
