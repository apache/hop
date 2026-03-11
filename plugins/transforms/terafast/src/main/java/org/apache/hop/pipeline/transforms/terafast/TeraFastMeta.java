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
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.ActionTransformType;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
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
@Getter
@Setter
public class TeraFastMeta extends BaseTransformMeta<ITransform, ITransformData> {

  private static final Class<?> PKG = TeraFastMeta.class;

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

  /** available options. */
  @HopMetadataProperty(key = "fastload_path")
  private String fastloadPath;

  @HopMetadataProperty(key = "controlfile_path")
  private String controlFile;

  @HopMetadataProperty(key = "datafile_path")
  private String dataFile;

  @HopMetadataProperty(key = "logfile_path")
  private String logFile;

  @HopMetadataProperty(key = "sessions")
  private Integer sessions;

  @HopMetadataProperty(key = "error_limit")
  private Integer errorLimit;

  @HopMetadataProperty(key = "use_control_file")
  private boolean useControlFile;

  @HopMetadataProperty(key = "variable_substitution")
  private boolean variableSubstitution;

  @HopMetadataProperty(key = "truncate_table")
  private boolean truncateTable;

  @HopMetadataProperty(key = "target_table")
  private String targetTable;

  @HopMetadataProperty(key = "table_field_list")
  private List<String> tableFieldList;

  @HopMetadataProperty(key = "stream_field_list")
  private List<String> streamFieldList;

  @HopMetadataProperty(
      key = "connectionName",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
  private String connectionName;

  /** */
  public TeraFastMeta() {
    super();
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
              BaseMessages.getString(PKG, "TeraFastMeta.Message.ConnectionEstablished"),
              transformMeta);
      remarks.add(checkResult);

      checkResult =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "TeraFastMeta.Message.TableExists"),
              transformMeta);
      remarks.add(checkResult);

      boolean error = false;
      for (String field : this.tableFieldList) {
        if (tableFields.searchValueMeta(field) == null) {
          error = true;
          checkResult =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_ERROR,
                  BaseMessages.getString(PKG, "TeraFastMeta.Exception.TableFieldNotFound"),
                  transformMeta);
          remarks.add(checkResult);
        }
      }
      if (!error) {
        checkResult =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "TeraFastMeta.Message.AllTableFieldsFound"),
                transformMeta);
        remarks.add(checkResult);
      }
      if (prev != null && !prev.isEmpty()) {
        // transform mode. transform receiving input
        checkResult =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "TeraFastMeta.Message.TransformInputDataFound"),
                transformMeta);
        remarks.add(checkResult);

        error = false;
        for (String field : this.streamFieldList) {
          if (prev.searchValueMeta(field) == null) {
            error = true;
            checkResult =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_ERROR,
                    BaseMessages.getString(PKG, "TeraFastMeta.Exception.StreamFieldNotFound"),
                    transformMeta);
            remarks.add(checkResult);
          }
        }
        if (!error) {
          checkResult =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(PKG, "TeraFastMeta.Message.AllStreamFieldsFound"),
                  transformMeta);
          remarks.add(checkResult);
        }
      }
    } catch (HopDatabaseException e) {
      checkResult =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "TeraFastMeta.Exception.ConnectionFailed"),
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
    DatabaseMeta databaseMeta =
        getParentTransformMeta().getParentPipelineMeta().findDatabase(connectionName, variables);
    if (databaseMeta != null) {
      Database db = new Database(loggingObject, variables, databaseMeta);
      db.connect();
      return db;
    }
    throw new HopException(
        BaseMessages.getString(PKG, "TeraFastMeta.Exception.ConnectionNotDefined"));
  }

  /**
   * {@inheritDoc}
   *
   * @see ITransformMeta#setDefault()
   */
  @Override
  public void setDefault() {
    this.fastloadPath = DEFAULT_FASTLOAD_PATH;
    this.dataFile = DEFAULT_DATA_FILE;
    this.sessions = DEFAULT_SESSIONS;
    this.errorLimit = DEFAULT_ERROR_LIMIT;
    this.truncateTable = DEFAULT_TRUNCATETABLE;
    this.variableSubstitution = DEFAULT_VARIABLE_SUBSTITUTION;
    this.targetTable = DEFAULT_TARGET_TABLE;
    this.useControlFile = true;
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
    if (!this.useControlFile) {
      final Database database = connectToDatabase(variables);
      IRowMeta fields =
          database.getTableFieldsMeta(StringUtils.EMPTY, variables.resolve(this.targetTable));
      database.disconnect();
      if (fields == null) {
        throw new HopException(BaseMessages.getString(PKG, "TeraFastMeta.Exception.TableNotFound"));
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
}
