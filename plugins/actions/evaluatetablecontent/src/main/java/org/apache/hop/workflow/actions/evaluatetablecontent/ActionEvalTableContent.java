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

package org.apache.hop.workflow.actions.evaluatetablecontent;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;

/** This defines a Table content evaluation action */
@Action(
    id = "EVAL_TABLE_CONTENT",
    name = "i18n::ActionEvalTableContent.Name",
    description = "i18n::ActionEvalTableContent.Description",
    image = "EvalTableContent.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Conditions",
    keywords = "i18n::ActionEvalTableContent.keyword",
    documentationUrl = "/workflow/actions/evaluatetablecontent.html")
@Setter
@Getter
public class ActionEvalTableContent extends ActionBase {
  private static final Class<?> PKG = ActionEvalTableContent.class;

  @HopMetadataProperty(key = "add_rows_result")
  private boolean addRowsResult;

  @HopMetadataProperty(key = "clear_result_rows")
  private boolean clearResultList;

  @HopMetadataProperty(key = "is_usevars")
  private boolean useVars;

  @HopMetadataProperty(key = "is_custom_sql")
  private boolean useCustomSql;

  @HopMetadataProperty(key = "custom_sql")
  private String customSql;

  @HopMetadataProperty(key = "connection")
  private String connection;

  @HopMetadataProperty(key = "tablename")
  private String tableName;

  @HopMetadataProperty(key = "schemaname")
  private String schemaname;

  @HopMetadataProperty(key = "limit")
  private String limit;

  @HopMetadataProperty(key = "success_condition")
  private String successCondition;

  private DatabaseMeta databaseMeta;

  private static final String SELECT_COUNT = "SELECT count(*) FROM ";

  public static final String[] successConditionsDesc =
      new String[] {
        BaseMessages.getString(PKG, "ActionEvalTableContent.SuccessWhenRowCountEqual.Label"),
        BaseMessages.getString(PKG, "ActionEvalTableContent.SuccessWhenRowCountDifferent.Label"),
        BaseMessages.getString(PKG, "ActionEvalTableContent.SuccessWhenRowCountSmallerThan.Label"),
        BaseMessages.getString(
            PKG, "ActionEvalTableContent.SuccessWhenRowCountSmallerOrEqualThan.Label"),
        BaseMessages.getString(PKG, "ActionEvalTableContent.SuccessWhenRowCountGreaterThan.Label"),
        BaseMessages.getString(
            PKG, "ActionEvalTableContent.SuccessWhenRowCountGreaterOrEqual.Label")
      };

  public static final String[] successConditionsCode =
      new String[] {
        "rows_count_equal",
        "rows_count_different",
        "rows_count_smaller",
        "rows_count_smaller_equal",
        "rows_count_greater",
        "rows_count_greater_equal"
      };

  public static final int SUCCESS_CONDITION_ROWS_COUNT_EQUAL = 0;
  public static final int SUCCESS_CONDITION_ROWS_COUNT_DIFFERENT = 1;
  public static final int SUCCESS_CONDITION_ROWS_COUNT_SMALLER = 2;
  public static final int SUCCESS_CONDITION_ROWS_COUNT_SMALLER_EQUAL = 3;
  public static final int SUCCESS_CONDITION_ROWS_COUNT_GREATER = 4;
  public static final int SUCCESS_CONDITION_ROWS_COUNT_GREATER_EQUAL = 5;

  public ActionEvalTableContent(String n) {
    super(n, "");
    limit = "0";
    successCondition = getSuccessConditionCode(SUCCESS_CONDITION_ROWS_COUNT_GREATER);
    useCustomSql = false;
    useVars = false;
    addRowsResult = false;
    clearResultList = true;
    customSql = null;
    schemaname = null;
    tableName = null;
    connection = null;
  }

  public ActionEvalTableContent() {
    this("");
  }

  public static int getSuccessConditionByDesc(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < successConditionsDesc.length; i++) {
      if (successConditionsDesc[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }

    // If this fails, try to match using the code.
    return getSuccessConditionByCode(tt);
  }

  public static String getSuccessConditionCode(int i) {
    if (i < 0 || i >= successConditionsCode.length) {
      return successConditionsCode[0];
    }
    return successConditionsCode[i];
  }

  public static int getSuccessConditionByCode(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < successConditionsCode.length; i++) {
      if (successConditionsCode[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    return 0;
  }

  public static String getSuccessConditionDesc(int i) {
    if (i < 0 || i >= successConditionsDesc.length) {
      return successConditionsDesc[0];
    }
    return successConditionsDesc[i];
  }

  public DatabaseMeta getDatabase() {
    if (databaseMeta != null) {
      return databaseMeta;
    }
    try {
      databaseMeta = DatabaseMeta.loadDatabase(getMetadataProvider(), connection);
      return databaseMeta;
    } catch (HopXmlException e) {
      return null;
    }
  }

  @VisibleForTesting
  public void setDatabaseMeta(DatabaseMeta databaseMeta) {
    this.databaseMeta = databaseMeta;
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public boolean isUnconditional() {
    return false;
  }

  @Override
  public Result execute(Result previousResult, int nr) {
    Result result = previousResult;
    result.setResult(false);

    String countSqlStatement = null;
    long rowsCount = 0;
    long errCount = 0;

    boolean successOK = false;

    int nrRowsLimit = Const.toInt(resolve(limit), 0);
    if (isDetailed()) {
      logDetailed(
          BaseMessages.getString(PKG, "ActionEvalTableContent.Log.nrRowsLimit", "" + nrRowsLimit));
    }

    if (getDatabase() != null) {
      try (Database db = new Database(this, this, getDatabase())) {
        db.connect();

        if (useCustomSql) {
          String realCustomSql = customSql;
          if (useVars) {
            realCustomSql = resolve(realCustomSql);
          }
          if (isDebug()) {
            logDebug(
                BaseMessages.getString(
                    PKG, "ActionEvalTableContent.Log.EnteredCustomSQL", realCustomSql));
          }

          if (!Utils.isEmpty(realCustomSql)) {
            countSqlStatement = realCustomSql;
          } else {
            errCount++;
            logError(BaseMessages.getString(PKG, "ActionEvalTableContent.Error.NoCustomSQL"));
          }

        } else {
          String realTablename = resolve(tableName);
          String realSchemaname = resolve(schemaname);

          if (!Utils.isEmpty(realTablename)) {
            if (!Utils.isEmpty(realSchemaname)) {
              countSqlStatement =
                  SELECT_COUNT
                      + db.getDatabaseMeta()
                          .getQuotedSchemaTableCombination(this, realSchemaname, realTablename);
            } else {
              countSqlStatement = SELECT_COUNT + db.getDatabaseMeta().quoteField(realTablename);
            }
          } else {
            errCount++;
            logError(BaseMessages.getString(PKG, "ActionEvalTableContent.Error.NoTableName"));
          }
        }

        if (countSqlStatement != null) {
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG, "ActionEvalTableContent.Log.RunSQLStatement", countSqlStatement));
          }

          if (useCustomSql) {
            if (clearResultList) {
              result.getRows().clear();
            }

            List<Object[]> ar = db.getRows(countSqlStatement, 0);
            if (ar != null) {
              rowsCount = ar.size();

              // ad rows to result
              IRowMeta rowMeta = db.getQueryFields(countSqlStatement, false);

              List<RowMetaAndData> rows = new ArrayList<>();
              for (int i = 0; i < ar.size(); i++) {
                rows.add(new RowMetaAndData(rowMeta, ar.get(i)));
              }
              if (addRowsResult && useCustomSql && rows != null) {
                result.getRows().addAll(rows);
              }
            } else {
              if (isDebug()) {
                logDebug(
                    BaseMessages.getString(
                        PKG,
                        "ActionEvalTableContent.Log.customSQLreturnedNothing",
                        countSqlStatement));
              }
            }

          } else {
            RowMetaAndData row = db.getOneRow(countSqlStatement);
            if (row != null) {
              rowsCount = row.getInteger(0);
            }
          }
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG, "ActionEvalTableContent.Log.NrRowsReturned", "" + rowsCount));
          }
          switch (getSuccessConditionByDesc(successCondition)) {
            case ActionEvalTableContent.SUCCESS_CONDITION_ROWS_COUNT_EQUAL:
              successOK = (rowsCount == nrRowsLimit);
              break;
            case ActionEvalTableContent.SUCCESS_CONDITION_ROWS_COUNT_DIFFERENT:
              successOK = (rowsCount != nrRowsLimit);
              break;
            case ActionEvalTableContent.SUCCESS_CONDITION_ROWS_COUNT_SMALLER:
              successOK = (rowsCount < nrRowsLimit);
              break;
            case ActionEvalTableContent.SUCCESS_CONDITION_ROWS_COUNT_SMALLER_EQUAL:
              successOK = (rowsCount <= nrRowsLimit);
              break;
            case ActionEvalTableContent.SUCCESS_CONDITION_ROWS_COUNT_GREATER:
              successOK = (rowsCount > nrRowsLimit);
              break;
            case ActionEvalTableContent.SUCCESS_CONDITION_ROWS_COUNT_GREATER_EQUAL:
              successOK = (rowsCount >= nrRowsLimit);
              break;
            default:
              break;
          }
        } // end if countSqlStatement!=null
      } catch (HopException dbe) {
        errCount++;
        logError(
            BaseMessages.getString(
                PKG, "ActionEvalTableContent.Error.RunningEntry", dbe.getMessage()));
      }
    } else {
      errCount++;
      logError(BaseMessages.getString(PKG, "ActionEvalTableContent.NoDbConnection"));
    }

    result.setResult(successOK);
    result.setNrLinesRead(rowsCount);
    result.setNrErrors(errCount);

    return result;
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, WorkflowMeta workflowMeta) {
    List<ResourceReference> references = super.getResourceDependencies(variables, workflowMeta);
    if (getDatabase() != null) {
      ResourceReference reference = new ResourceReference(this);
      reference
          .getEntries()
          .add(new ResourceEntry(getDatabase().getHostname(), ResourceType.SERVER));
      reference
          .getEntries()
          .add(new ResourceEntry(getDatabase().getDatabaseName(), ResourceType.DATABASENAME));
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
}
