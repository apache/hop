/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.workflow.actions.waitforsql;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IEnumHasCode;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;

/** This defines a Wait for SQL data action */
@Action(
    id = "WAIT_FOR_SQL",
    name = "i18n::ActionWaitForSQL.Name",
    description = "i18n::ActionWaitForSQL.Description",
    image = "WaitForSQL.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Utility",
    keywords = "i18n::ActionWaitForSql.keyword",
    documentationUrl = "/workflow/actions/waitforsql.html")
public class ActionWaitForSql extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionWaitForSql.class;

  @HopMetadataProperty(key = "clear_result_rows")
  private boolean clearResultList;

  @HopMetadataProperty(key = "add_rows_result")
  private boolean addRowsResult;

  @HopMetadataProperty(key = "is_usevars")
  private boolean useVars;

  @HopMetadataProperty(key = "is_custom_sql")
  private boolean customSqlEnabled;

  @HopMetadataProperty(key = "custom_sql")
  private String customSql;

  @HopMetadataProperty(key = "connection")
  private String connection;

  @HopMetadataProperty(key = "tablename")
  private String tableName;

  @HopMetadataProperty(key = "schemaname")
  private String schemaName;

  /** Maximum timeout in seconds */
  @HopMetadataProperty(key = "maximum_timeout")
  private String maximumTimeout;

  /** Cycle time in seconds */
  @HopMetadataProperty(key = "check_cycle_time")
  private String checkCycleTime;

  @HopMetadataProperty(key = "success_on_timeout")
  private boolean successOnTimeout;

  @HopMetadataProperty(key = "rows_count_value")
  private String rowsCountValue;

  @HopMetadataProperty(key = "success_condition", storeWithCode = true)
  private SuccessCondition successCondition;

  private static final String SELECT_COUNT = "SELECT count(*) FROM ";

  public static final int SUCCESS_CONDITION_ROWS_COUNT_EQUAL = 0;
  public static final int SUCCESS_CONDITION_ROWS_COUNT_DIFFERENT = 1;
  public static final int SUCCESS_CONDITION_ROWS_COUNT_SMALLER = 2;
  public static final int SUCCESS_CONDITION_ROWS_COUNT_SMALLER_EQUAL = 3;
  public static final int SUCCESS_CONDITION_ROWS_COUNT_GREATER = 4;
  public static final int SUCCESS_CONDITION_ROWS_COUNT_GREATER_EQUAL = 5;

  public enum SuccessCondition implements IEnumHasCodeAndDescription {
    ROWS_COUNT_EQUAL(
        "rows_count_equal",
        BaseMessages.getString(PKG, "ActionWaitForSQL.SuccessWhenRowCountEqual.Label")),
    ROWS_COUNT_DIFFERENT(
        "rows_count_different",
        BaseMessages.getString(PKG, "ActionWaitForSQL.SuccessWhenRowCountDifferent.Label")),
    ROWS_COUNT_SMALLER(
        "rows_count_smaller",
        BaseMessages.getString(PKG, "ActionWaitForSQL.SuccessWhenRowCountSmallerThan.Label")),
    ROWS_COUNT_SMALLER_EQUAL(
        "rows_count_smaller_equal",
        BaseMessages.getString(
            PKG, "ActionWaitForSQL.SuccessWhenRowCountSmallerOrEqualThan.Label")),
    ROWS_COUNT_GREATER(
        "rows_count_greater",
        BaseMessages.getString(PKG, "ActionWaitForSQL.SuccessWhenRowCountGreaterThan.Label")),
    ROWS_COUNT_GREATER_EQUAL(
        "rows_count_greater_equal",
        BaseMessages.getString(PKG, "ActionWaitForSQL.SuccessWhenRowCountGreaterOrEqual.Label"));

    private final String code;
    private final String description;

    SuccessCondition(String code, String description) {
      this.code = code;
      this.description = description;
    }

    public static String[] getDescriptions() {
      return IEnumHasCodeAndDescription.getDescriptions(SuccessCondition.class);
    }

    public static SuccessCondition lookupDescription(String description) {
      return IEnumHasCodeAndDescription.lookupDescription(
          SuccessCondition.class, description, ROWS_COUNT_EQUAL);
    }

    public static SuccessCondition lookupCode(String code) {
      return IEnumHasCode.lookupCode(SuccessCondition.class, code, ROWS_COUNT_EQUAL);
    }

    /**
     * Gets code
     *
     * @return value of code
     */
    @Override
    public String getCode() {
      return code;
    }

    /**
     * Gets description
     *
     * @return value of description
     */
    @Override
    public String getDescription() {
      return description;
    }
  }

  private static final String DEFAULT_MAXIMUM_TIMEOUT = "0"; // infinite timeout
  private static final String DEFAULT_CHECK_CYCLE_TIME = "60"; // 1 minute

  public ActionWaitForSql(String n) {
    super(n, "");
    clearResultList = true;
    rowsCountValue = "0";
    successCondition = SuccessCondition.ROWS_COUNT_GREATER;
    customSqlEnabled = false;
    useVars = false;
    addRowsResult = false;
    customSql = null;
    connection = null;
    schemaName = null;
    tableName = null;
    maximumTimeout = DEFAULT_MAXIMUM_TIMEOUT;
    checkCycleTime = DEFAULT_CHECK_CYCLE_TIME;
    successOnTimeout = false;
  }

  public ActionWaitForSql() {
    this("");
  }

  @Override
  public Object clone() {
    return super.clone();
  }

  public SuccessCondition getSuccessCondition() {
    return successCondition;
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public boolean isUnconditional() {
    return false;
  }

  // Visible for testing purposes
  protected void checkConnection() throws HopDatabaseException {
    // check connection
    // connect and disconnect
    try {
      DatabaseMeta databaseMeta = parentWorkflowMeta.findDatabase(connection, getVariables());
      try (Database database = new Database(this, this, databaseMeta)) {
        database.connect();
      }
    } catch (HopException e) {
      throw new HopDatabaseException(e.getMessage(), e);
    }
  }

  @Override
  public Result execute(Result previousResult, int nr) {
    Result result = previousResult;
    result.setResult(false);
    result.setNrErrors(1);
    String realCustomSql = null;
    String realTablename = resolve(tableName);
    String realSchemaname = resolve(schemaName);

    if (connection == null) {
      logError(BaseMessages.getString(PKG, "ActionWaitForSQL.NoDbConnection"));
      return result;
    }

    if (customSqlEnabled) {
      // clear result list rows
      if (clearResultList) {
        result.getRows().clear();
      }

      realCustomSql = customSql;
      if (useVars) {
        realCustomSql = resolve(realCustomSql);
      }
      if (isDebug()) {
        logDebug(
            BaseMessages.getString(PKG, "ActionWaitForSQL.Log.EnteredCustomSQL", realCustomSql));
      }

      if (Utils.isEmpty(realCustomSql)) {
        logError(BaseMessages.getString(PKG, "ActionWaitForSQL.Error.NoCustomSQL"));
        return result;
      }

    } else {
      if (Utils.isEmpty(realTablename)) {
        logError(BaseMessages.getString(PKG, "ActionWaitForSQL.Error.NoTableName"));
        return result;
      }
    }

    try {
      // check connection
      // connect and disconnect
      checkConnection();

      // starttime (in seconds)
      long timeStart = System.currentTimeMillis() / 1000;

      int nrRowsLimit = Const.toInt(resolve(rowsCountValue), 0);
      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(PKG, "ActionWaitForSQL.Log.nrRowsLimit", "" + nrRowsLimit));
      }

      long iMaximumTimeout =
          Const.toInt(resolve(maximumTimeout), Const.toInt(DEFAULT_MAXIMUM_TIMEOUT, 0));
      long iCycleTime =
          Const.toInt(resolve(checkCycleTime), Const.toInt(DEFAULT_CHECK_CYCLE_TIME, 0));

      //
      // Sanity check on some values, and complain on insanity
      //
      if (iMaximumTimeout < 0) {
        iMaximumTimeout = Const.toInt(DEFAULT_MAXIMUM_TIMEOUT, 0);
        if (isBasic()) {
          logBasic("Maximum timeout invalid, reset to " + iMaximumTimeout);
        }
      }

      if (iCycleTime < 1) {
        // If lower than 1 set to the default
        iCycleTime = Const.toInt(DEFAULT_CHECK_CYCLE_TIME, 1);
        if (isBasic()) {
          logBasic("Check cycle time invalid, reset to " + iCycleTime);
        }
      }

      if (iMaximumTimeout == 0) {
        if (isBasic()) {
          logBasic("Waiting indefinitely for SQL data");
        }
      } else {
        if (isBasic()) {
          logBasic("Waiting " + iMaximumTimeout + " seconds for SQL data");
        }
      }

      boolean continueLoop = true;
      while (continueLoop && !parentWorkflow.isStopped()) {
        if (sqlDataOK(result, nrRowsLimit, realSchemaname, realTablename, realCustomSql)) {
          // SQL data exists, we're happy to exit
          if (isBasic()) {
            logBasic("Detected SQL data within timeout");
          }
          result.setResult(true);
          continueLoop = false;
        } else {
          long now = System.currentTimeMillis() / 1000;

          if ((iMaximumTimeout > 0) && (now > (timeStart + iMaximumTimeout))) {
            continueLoop = false;

            // SQL data doesn't exist after timeout, either true or false
            if (isSuccessOnTimeout()) {
              if (isBasic()) {
                logBasic("Didn't detect SQL data before timeout, success");
              }
              result.setResult(true);
            } else {
              if (isBasic()) {
                logBasic("Didn't detect SQL data before timeout, failure");
              }
              result.setResult(false);
            }
          }
          // sleep algorithm
          long sleepTime = 0;

          if (iMaximumTimeout == 0) {
            sleepTime = iCycleTime;
          } else {
            if ((now + iCycleTime) < (timeStart + iMaximumTimeout)) {
              sleepTime = iCycleTime;
            } else {
              sleepTime = iCycleTime - ((now + iCycleTime) - (timeStart + iMaximumTimeout));
            }
          }
          try {
            if (sleepTime > 0) {
              if (isDetailed()) {
                logDetailed("Sleeping " + sleepTime + " seconds before next check for SQL data");
              }
              Thread.sleep(sleepTime * 1000);
            }
          } catch (InterruptedException e) {
            // something strange happened
            result.setResult(false);
            continueLoop = false;
          }
        }
      }
    } catch (Exception e) {
      logBasic("Exception while waiting for SQL data: " + e.getMessage());
    }

    if (result.isResult()) {
      // Remove error count set at the beginning of the method
      //
      result.setNrErrors(0);
    }

    return result;
  }

  protected boolean sqlDataOK(
      Result result,
      long nrRowsLimit,
      String realSchemaName,
      String realTableName,
      String customSql)
      throws HopException {
    String countStatement = null;
    long rowsCount = 0;
    boolean successOK = false;
    List<Object[]> ar = null;
    IRowMeta rowMeta = null;

    DatabaseMeta databaseMeta = parentWorkflowMeta.findDatabase(connection, getVariables());
    try (Database db = new Database(this, this, databaseMeta)) {
      db.connect();
      if (customSqlEnabled) {
        countStatement = customSql;
      } else {
        if (!Utils.isEmpty(realSchemaName)) {
          countStatement =
              SELECT_COUNT
                  + db.getDatabaseMeta()
                      .getQuotedSchemaTableCombination(this, realSchemaName, realTableName);
        } else {
          countStatement = SELECT_COUNT + db.getDatabaseMeta().quoteField(realTableName);
        }
      }

      if (countStatement != null) {
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(PKG, "ActionWaitForSQL.Log.RunSQLStatement", countStatement));
        }

        if (customSqlEnabled) {
          ar = db.getRows(countStatement, 0);
          if (ar != null) {
            rowsCount = ar.size();
          } else {
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG, "ActionWaitForSQL.Log.customSQLreturnedNothing", countStatement));
            }
          }

        } else {
          RowMetaAndData row = db.getOneRow(countStatement);
          if (row != null) {
            rowsCount = row.getInteger(0);
          }
        }
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(PKG, "ActionWaitForSQL.Log.NrRowsReturned", "" + rowsCount));
        }

        switch (successCondition) {
          case ROWS_COUNT_EQUAL:
            successOK = (rowsCount == nrRowsLimit);
            break;
          case ROWS_COUNT_DIFFERENT:
            successOK = (rowsCount != nrRowsLimit);
            break;
          case ROWS_COUNT_SMALLER:
            successOK = (rowsCount < nrRowsLimit);
            break;
          case ROWS_COUNT_SMALLER_EQUAL:
            successOK = (rowsCount <= nrRowsLimit);
            break;
          case ROWS_COUNT_GREATER:
            successOK = (rowsCount > nrRowsLimit);
            break;
          case ROWS_COUNT_GREATER_EQUAL:
            successOK = (rowsCount >= nrRowsLimit);
            break;
          default:
            break;
        }
      } // end if countStatement!=null

      if (addRowsResult && customSqlEnabled && ar != null) {
        rowMeta = db.getQueryFields(countStatement, false);
      }
    } catch (HopDatabaseException dbe) {
      logError(
          BaseMessages.getString(PKG, "ActionWaitForSQL.Error.RunningEntry", dbe.getMessage()));
    }

    // ad rows to result
    if (successOK && addRowsResult && customSqlEnabled && ar != null) {
      List<RowMetaAndData> rows = new ArrayList<>();
      for (Object[] objects : ar) {
        rows.add(new RowMetaAndData(rowMeta, objects));
      }
      if (rows != null) {
        result.getRows().addAll(rows);
      }
    }
    return successOK;
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, WorkflowMeta workflowMeta) {
    List<ResourceReference> references = super.getResourceDependencies(variables, workflowMeta);

    DatabaseMeta databaseMeta = null;
    try {
      IHopMetadataProvider metadataProvider = workflowMeta.getMetadataProvider();
      databaseMeta =
          metadataProvider.getSerializer(DatabaseMeta.class).load(variables.resolve(connection));
    } catch (HopException e) {
      // Ignore error loading metadata
    }

    if (databaseMeta != null) {
      ResourceReference reference = new ResourceReference(this);
      reference
          .getEntries()
          .add(new ResourceEntry(databaseMeta.getHostname(), ResourceType.SERVER));
      reference
          .getEntries()
          .add(new ResourceEntry(databaseMeta.getDatabaseName(), ResourceType.DATABASENAME));
      references.add(reference);
    }

    return references;
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      WorkflowMeta workflowMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    ActionValidatorUtils.andValidator()
        .validate(
            this,
            "WaitForSQL",
            remarks,
            AndValidator.putValidators(ActionValidatorUtils.notBlankValidator()));
  }

  /**
   * Gets isClearResultList
   *
   * @return value of isClearResultList
   */
  public boolean isClearResultList() {
    return clearResultList;
  }

  /**
   * @param isClearResultList The isClearResultList to set
   */
  public void setClearResultList(boolean isClearResultList) {
    this.clearResultList = isClearResultList;
  }

  /**
   * Gets isAddRowsResult
   *
   * @return value of isAddRowsResult
   */
  public boolean isAddRowsResult() {
    return addRowsResult;
  }

  /**
   * @param isAddRowsResult The isAddRowsResult to set
   */
  public void setAddRowsResult(boolean isAddRowsResult) {
    this.addRowsResult = isAddRowsResult;
  }

  /**
   * Gets isUseVars
   *
   * @return value of isUseVars
   */
  public boolean isUseVars() {
    return useVars;
  }

  /**
   * @param isUseVars The isUseVars to set
   */
  public void setUseVars(boolean isUseVars) {
    this.useVars = isUseVars;
  }

  /**
   * Gets isCustomSql
   *
   * @return value of isCustomSql
   */
  public boolean isCustomSqlEnabled() {
    return customSqlEnabled;
  }

  /**
   * @param isCustomSql The isCustomSql to set
   */
  public void setCustomSqlEnabled(boolean isCustomSql) {
    this.customSqlEnabled = isCustomSql;
  }

  /**
   * Gets customSql
   *
   * @return value of customSql
   */
  public String getCustomSql() {
    return customSql;
  }

  /**
   * @param customSql The customSql to set
   */
  public void setCustomSql(String customSql) {
    this.customSql = customSql;
  }

  /**
   * Gets tableName
   *
   * @return value of tableName
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * @param tableName The tableName to set
   */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /**
   * Gets schemaName
   *
   * @return value of schemaName
   */
  public String getSchemaName() {
    return schemaName;
  }

  /**
   * @param schemaName The schemaName to set
   */
  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  /**
   * Gets maximumTimeout
   *
   * @return value of maximumTimeout
   */
  public String getMaximumTimeout() {
    return maximumTimeout;
  }

  /**
   * @param maximumTimeout The maximumTimeout to set
   */
  public void setMaximumTimeout(String maximumTimeout) {
    this.maximumTimeout = maximumTimeout;
  }

  /**
   * Gets checkCycleTime
   *
   * @return value of checkCycleTime
   */
  public String getCheckCycleTime() {
    return checkCycleTime;
  }

  /**
   * @param checkCycleTime The checkCycleTime to set
   */
  public void setCheckCycleTime(String checkCycleTime) {
    this.checkCycleTime = checkCycleTime;
  }

  /**
   * Gets successOnTimeout
   *
   * @return value of successOnTimeout
   */
  public boolean isSuccessOnTimeout() {
    return successOnTimeout;
  }

  /**
   * @param successOnTimeout The successOnTimeout to set
   */
  public void setSuccessOnTimeout(boolean successOnTimeout) {
    this.successOnTimeout = successOnTimeout;
  }

  /**
   * Gets selectCount
   *
   * @return value of selectCount
   */
  public static String getSelectCount() {
    return SELECT_COUNT;
  }

  /**
   * Gets rowsCountValue
   *
   * @return value of rowsCountValue
   */
  public String getRowsCountValue() {
    return rowsCountValue;
  }

  /**
   * @param rowsCountValue The rowsCountValue to set
   */
  public void setRowsCountValue(String rowsCountValue) {
    this.rowsCountValue = rowsCountValue;
  }

  /**
   * @param successCondition The successCondition to set
   */
  public void setSuccessCondition(SuccessCondition successCondition) {
    this.successCondition = successCondition;
  }

  /**
   * Gets connection
   *
   * @return value of connection
   */
  public String getConnection() {
    return connection;
  }

  /**
   * @param connection The connection to set
   */
  public void setConnection(String connection) {
    this.connection = connection;
  }
}
