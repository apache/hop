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

package org.apache.hop.pipeline.transforms.databasejoin;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.ActionTransformType;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaNone;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.DatabaseImpact;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Getter
@Setter
@Transform(
    id = "DBJoin",
    image = "dbjoin.svg",
    name = "i18n::DatabaseJoin.Name",
    description = "i18n::DatabaseJoin.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Lookup",
    keywords = "i18n::DatabaseJoinMeta.keyword",
    documentationUrl = "/pipeline/transforms/databasejoin.html",
    actionTransformTypes = {
      ActionTransformType.RDBMS,
      ActionTransformType.LOOKUP,
      ActionTransformType.JOIN
    })
public class DatabaseJoinMeta extends BaseTransformMeta<DatabaseJoin, DatabaseJoinData> {

  private static final Class<?> PKG = DatabaseJoinMeta.class;

  @HopMetadataProperty(
      key = "connection",
      injectionKeyDescription = "DatabaseJoinMeta.Injection.Connection",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
  private String connection;

  @HopMetadataProperty(key = "cache", injectionKeyDescription = "DatabaseJoinMeta.Injection.Cache")
  private boolean cached;

  /** Limit the cache size to this! */
  @HopMetadataProperty(
      key = "cache_size",
      injectionKeyDescription = "DatabaseJoinMeta.Injection.CacheSize")
  private int cacheSize;

  /** SQL Statement */
  @HopMetadataProperty(
      key = "sql",
      injectionKeyDescription = "DatabaseJoinMeta.Injection.SQL",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_SQL)
  private String sql;

  /**
   * When set, SQL is loaded from this file (VFS path, supports variables). SQL editor is read-only.
   */
  @HopMetadataProperty(
      key = "sql_from_file",
      injectionKey = "SQL_FROM_FILE",
      injectionKeyDescription = "DatabaseJoinMeta.Injection.SqlFromFile")
  private String sqlFromFile;

  /** Number of rows to return (0=ALL) */
  @HopMetadataProperty(
      key = "rowlimit",
      injectionKeyDescription = "DatabaseJoinMeta.Injection.RowLimit")
  private int rowLimit;

  /**
   * false: don't return rows where nothing is found true: at least return one source row, the rest
   * is NULL
   */
  @HopMetadataProperty(
      key = "outer_join",
      injectionKeyDescription = "DatabaseJoinMeta.Injection.OuterJoin")
  private boolean outerJoin;

  /** Fields to use as parameters (fill in the ? markers) */
  @HopMetadataProperty(
      key = "field",
      groupKey = "parameter",
      injectionGroupDescription = "DatabaseJoinMeta.Injection.Parameters",
      injectionKeyDescription = "DatabaseJoinMeta.Injection.Field")
  private List<ParameterField> parameters = new ArrayList<>();

  /** false: don't replace variable in script true: replace variable in script */
  @HopMetadataProperty(
      key = "replace_vars",
      injectionKeyDescription = "DatabaseJoinMeta.Injection.ReplaceVariables")
  private boolean replaceVariables;

  /**
   * Returns the SQL to execute: either from the inline editor or loaded from the file specified by
   * sqlFromFile (using VFS). Variables are resolved in the file path.
   */
  public String getEffectiveSql(IVariables variables) throws HopException {
    if (!Utils.isEmpty(sqlFromFile)) {
      String path = variables.resolve(sqlFromFile);
      try {
        return HopVfs.getTextFileContent(path, StandardCharsets.UTF_8);
      } catch (HopFileException e) {
        throw new HopException(
            BaseMessages.getString(PKG, "DatabaseJoinMeta.Exception.CouldNotLoadSqlFromFile", path),
            e);
      }
    }
    return sql;
  }

  private String resolveSql(IVariables variables) throws HopException {
    return variables.resolve(Const.NVL(getEffectiveSql(variables), ""));
  }

  @Override
  public void setDefault() {
    rowLimit = 0;
    sql = "";
    outerJoin = false;
    replaceVariables = false;
    parameters = new ArrayList<>();
  }

  /**
   * SQL parameter specification parsed from source SQL. Supports named ?{name} and positional ?
   * placeholders.
   */
  static final class SqlParameterSpec {
    private final String preparedSql;
    private final List<String> parameterReferences;
    private final int positionalParameterCount;

    SqlParameterSpec(
        String preparedSql, List<String> parameterReferences, int positionalParameterCount) {
      this.preparedSql = preparedSql;
      this.parameterReferences = parameterReferences;
      this.positionalParameterCount = positionalParameterCount;
    }

    String getPreparedSql() {
      return preparedSql;
    }

    List<String> getParameterReferences() {
      return parameterReferences;
    }

    int getParameterCount() {
      return parameterReferences.size();
    }

    int getPositionalParameterCount() {
      return positionalParameterCount;
    }

    boolean hasNamedParameters() {
      for (String parameterReference : parameterReferences) {
        if (parameterReference != null) {
          return true;
        }
      }
      return false;
    }
  }

  public IRowMeta createQueryParameterRowMeta(IRowMeta sourceMeta, SqlParameterSpec parameterSpec)
      throws HopTransformException {
    IRowMeta queryParametersMeta = new RowMeta();

    int positionalIndex = 0;
    for (String parameterReference : parameterSpec.getParameterReferences()) {
      String sourceFieldName = parameterReference;
      if (sourceFieldName == null) {
        sourceFieldName = getPositionalParameterFieldName(positionalIndex);
        positionalIndex++;
      }

      if (Utils.isEmpty(sourceFieldName)) {
        throw new HopTransformException(
            "Unable to find input field for positional SQL parameter #"
                + positionalIndex
                + ". Configure parameter fields or use ?{fieldName}.");
      }

      int sourceIndex = sourceMeta == null ? -1 : sourceMeta.indexOfValue(sourceFieldName);
      if (sourceIndex < 0 || sourceIndex >= sourceMeta.size()) {
        throw new HopTransformException(
            "Unable to find input field [" + sourceFieldName + "] referenced in SQL parameter.");
      }

      queryParametersMeta.addValueMeta(sourceMeta.getValueMeta(sourceIndex).clone());
    }

    return queryParametersMeta;
  }

  /**
   * Parse SQL with support for ?{name} named placeholders. Returns prepared SQL (with '?' markers)
   * and a list of parameter references (null for positional parameters).
   */
  static SqlParameterSpec parseSqlParameterSpec(String sourceSql) {
    Database.SqlParameterSpec parsed = Database.parseSqlParameterSpec(sourceSql);
    int positionalParameterCount = 0;
    for (String parameterReference : parsed.getParameterReferences()) {
      if (parameterReference == null) {
        positionalParameterCount++;
      }
    }
    return new SqlParameterSpec(
        parsed.getPreparedSql(), parsed.getParameterReferences(), positionalParameterCount);
  }

  static java.util.Set<String> getMissingNamedParameters(
      IRowMeta sourceMeta, SqlParameterSpec parameterSpec) {
    java.util.Set<String> missingFields = new java.util.LinkedHashSet<>();

    for (String parameterReference : parameterSpec.getParameterReferences()) {
      if (parameterReference != null
          && (sourceMeta == null || sourceMeta.indexOfValue(parameterReference) < 0)) {
        missingFields.add(parameterReference);
      }
    }

    return missingFields;
  }

  /** Get the missing positional parameter fields for the given parameter specification. */
  java.util.Set<String> getMissingPositionalParameterFields(
      IRowMeta sourceMeta, SqlParameterSpec parameterSpec) {
    java.util.Set<String> missingFields = new java.util.LinkedHashSet<>();
    for (int i = 0; i < parameterSpec.getPositionalParameterCount(); i++) {
      String positionalFieldName = getPositionalParameterFieldName(i);
      if (Utils.isEmpty(positionalFieldName)) {
        missingFields.add("#" + (i + 1));
      } else if (sourceMeta == null || sourceMeta.indexOfValue(positionalFieldName) < 0) {
        missingFields.add(positionalFieldName);
      }
    }
    return missingFields;
  }

  /**
   * Heuristic to determine if the SQL is likely a stored procedure call. Used to suppress metadata
   * discovery errors at design time.
   */
  private boolean isLikelyStoredProcedureSql(String sqlText) {
    String normalized = Const.NVL(sqlText, "").trim().toLowerCase();
    return normalized.startsWith("exec ")
        || normalized.startsWith("execute ")
        || normalized.startsWith("{call ");
  }

  /**
   * Get the field name for the positional parameter at the given index. Returns null if no field is
   * defined for that index.
   */
  String getPositionalParameterFieldName(int positionalIndex) {
    if (parameters == null || positionalIndex < 0 || positionalIndex >= parameters.size()) {
      return null;
    }
    return parameters.get(positionalIndex).getName();
  }

  /** Build a row meta describing declared parameters (from the step configuration). */
  public IRowMeta createDeclaredParameterRowMeta() {
    IRowMeta param = new RowMeta();
    if (parameters == null) {
      return param;
    }

    for (ParameterField field : this.parameters) {
      IValueMeta v;
      try {
        int id = ValueMetaFactory.getIdForValueMeta(field.getType());
        v = ValueMetaFactory.createValueMeta(field.getName(), id);
      } catch (HopPluginException e) {
        v = new ValueMetaNone(field.getName());
      }
      param.addValueMeta(v);
    }
    return param;
  }

  /**
   * Create a parameter row meta suitable for metadata discovery: maps named placeholders to
   * declared parameters when possible and falls back to ValueMetaNone placeholders when not
   * available.
   */
  public IRowMeta createMetadataLookupParameterRowMeta(SqlParameterSpec parameterSpec) {
    return createMetadataLookupParameterRowMeta(parameterSpec, null);
  }

  /**
   * Create a parameter row meta suitable for metadata discovery: maps placeholders to incoming
   * stream fields when available and falls back to declared parameter types.
   */
  public IRowMeta createMetadataLookupParameterRowMeta(
      SqlParameterSpec parameterSpec, IRowMeta sourceMeta) {
    IRowMeta declared = createDeclaredParameterRowMeta();
    IRowMeta queryParameters = new RowMeta();

    int positionalIndex = 0;
    for (String parameterReference : parameterSpec.getParameterReferences()) {
      String lookupFieldName = parameterReference;
      String defaultName = lookupFieldName;
      if (lookupFieldName == null) {
        lookupFieldName = getPositionalParameterFieldName(positionalIndex);
        defaultName = "param" + (positionalIndex + 1);
      }

      IValueMeta valueMeta = null;
      if (!Utils.isEmpty(lookupFieldName) && sourceMeta != null) {
        valueMeta = sourceMeta.searchValueMeta(lookupFieldName);
      }
      if (valueMeta == null && !Utils.isEmpty(lookupFieldName)) {
        valueMeta = declared.searchValueMeta(lookupFieldName);
      }
      if (valueMeta == null) {
        valueMeta = new ValueMetaNone(defaultName);
      }

      queryParameters.addValueMeta(valueMeta.clone());
      if (parameterReference == null) {
        positionalIndex++;
      }
    }

    return queryParameters;
  }

  /** Create a parameter row suitable for metadata discovery with null values. */
  public Object[] createMetadataLookupParameterRowData(IRowMeta queryParametersMeta) {
    if (queryParametersMeta == null || queryParametersMeta.isEmpty()) {
      return new Object[0];
    }
    return new Object[queryParametersMeta.size()];
  }

  @Override
  public void getFields(
      IRowMeta row,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    if (connection == null) {
      return;
    }

    DatabaseMeta databaseMeta = null;

    try {
      databaseMeta =
          metadataProvider.getSerializer(DatabaseMeta.class).load(variables.resolve(connection));
    } catch (HopException e) {
      throw new HopTransformException(
          "Unable to get databaseMeta for connection: " + Const.CR + variables.resolve(connection),
          e);
    }

    try (Database db = new Database(loggingObject, variables, databaseMeta)) {
      databases = new Database[] {db}; // Keep track of this one for cancelQuery

      // Which fields are parameters?
      // info[0] comes from the database connection.
      //
      String sqlToUse;
      try {
        sqlToUse = resolveSql(variables);
      } catch (HopException e) {
        throw new HopTransformException(e.getMessage(), e);
      }

      SqlParameterSpec parameterSpec = parseSqlParameterSpec(sqlToUse);

      // Build metadata lookup parameters, preferring incoming field types when available.
      IRowMeta param = createMetadataLookupParameterRowMeta(parameterSpec, row);
      Object[] paramRow = createMetadataLookupParameterRowData(param);

      // First try without connecting to the database... (can be S L O W)
      // See if it's in the cache...
      //
      IRowMeta add = null;
      try {
        add = db.getQueryFields(parameterSpec.getPreparedSql(), true, param, paramRow);
      } catch (HopDatabaseException dbe) {
        if (isLikelyStoredProcedureSql(parameterSpec.getPreparedSql())) {
          logDetailed(
              "Unable to determine stored procedure output fields at design time; deferring metadata to runtime.");
          logDebug("Stored procedure metadata discovery exception", dbe);
          return;
        }
        throw new HopTransformException(
            BaseMessages.getString(PKG, "DatabaseJoinMeta.Exception.UnableToDetermineQueryFields")
                + Const.CR
                + parameterSpec.getPreparedSql(),
            dbe);
      }

      if (add != null) { // Cache hit, just return it this...
        for (int i = 0; i < add.size(); i++) {
          IValueMeta v = add.getValueMeta(i);
          v.setOrigin(name);
        }
        row.addRowMeta(add);
      } else {
        // No cache hit, connect to the database, do it the hard way...
        //
        try {
          db.connect();
        } catch (HopDatabaseException dbe) {
          throw new HopTransformException(
              BaseMessages.getString(PKG, "DatabaseJoinMeta.Exception.ErrorObtainingFields"), dbe);
        }

        try {
          add = db.getQueryFields(parameterSpec.getPreparedSql(), true, param, paramRow);
          for (int i = 0; i < add.size(); i++) {
            IValueMeta v = add.getValueMeta(i);
            v.setOrigin(name);
          }
          row.addRowMeta(add);
        } catch (HopDatabaseException dbe) {
          if (isLikelyStoredProcedureSql(parameterSpec.getPreparedSql())) {
            logDetailed(
                "Unable to determine stored procedure output fields after connecting; deferring metadata to runtime.");
            logDebug("Stored procedure metadata discovery exception after connecting", dbe);
            return;
          }
          throw new HopTransformException(
              BaseMessages.getString(PKG, "DatabaseJoinMeta.Exception.ErrorObtainingFields"), dbe);
        }
      }
    }
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {

    CheckResult cr;
    String errorMessage = "";
    DatabaseMeta databaseMeta = null;

    try {
      databaseMeta =
          metadataProvider.getSerializer(DatabaseMeta.class).load(variables.resolve(connection));
    } catch (HopException e) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG,
                  "DatabaseJoinMeta.CheckResult.DatabaseMetaError",
                  variables.resolve(connection)),
              transformMeta);
      remarks.add(cr);
    }

    if (databaseMeta != null) {
      Database db = new Database(loggingObject, variables, databaseMeta);
      databases = new Database[] {db}; // Keep track of this one for cancelQuery

      try {
        String sqlToUse = resolveSql(variables);
        if (Utils.isEmpty(sqlToUse)) {
          errorMessage = BaseMessages.getString(PKG, "DatabaseJoinMeta.CheckResult.InvalidDBQuery");
          cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
          remarks.add(cr);
        } else {
          db.connect();
          SqlParameterSpec parameterSpec = parseSqlParameterSpec(sqlToUse);

          errorMessage = "";

          IRowMeta param;
          try {
            param = createQueryParameterRowMeta(prev, parameterSpec);
          } catch (HopTransformException e) {
            cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, e.getMessage(), transformMeta);
            remarks.add(cr);
            param = new RowMeta();
          }

          IRowMeta r =
              db.getQueryFields(
                  parameterSpec.getPreparedSql(), true, param, new Object[param.size()]);
          if (r != null) {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_OK,
                    BaseMessages.getString(PKG, "DatabaseJoinMeta.CheckResult.QueryOK"),
                    transformMeta);
            remarks.add(cr);
          } else {
            errorMessage =
                BaseMessages.getString(PKG, "DatabaseJoinMeta.CheckResult.InvalidDBQuery");
            cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
            remarks.add(cr);
          }

          int q = db.countParameters(parameterSpec.getPreparedSql());
          if (q != parameterSpec.getParameterCount()) {
            errorMessage =
                BaseMessages.getString(
                        PKG, "DatabaseJoinMeta.CheckResult.DismatchBetweenParametersAndQuestion")
                    + Const.CR;
            errorMessage +=
                BaseMessages.getString(
                        PKG, "DatabaseJoinMeta.CheckResult.DismatchBetweenParametersAndQuestion2")
                    + q
                    + Const.CR;
            errorMessage +=
                BaseMessages.getString(
                        PKG, "DatabaseJoinMeta.CheckResult.DismatchBetweenParametersAndQuestion3")
                    + parameterSpec.getParameterCount();

            cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
            remarks.add(cr);
          } else {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_OK,
                    BaseMessages.getString(PKG, "DatabaseJoinMeta.CheckResult.NumberOfParamCorrect")
                        + q
                        + ")",
                    transformMeta);
            remarks.add(cr);
          }

          // Look up fields in the input stream <prev>
          if (prev != null && !prev.isEmpty()) {
            java.util.Set<String> missingNamedParameters =
                getMissingNamedParameters(prev, parameterSpec);
            java.util.Set<String> missingPositionalParameterFields =
                getMissingPositionalParameterFields(prev, parameterSpec);

            boolean first = true;
            errorMessage = "";
            boolean errorFound = false;

            for (String parameterName : missingNamedParameters) {
              if (first) {
                first = false;
                errorMessage +=
                    BaseMessages.getString(PKG, "DatabaseJoinMeta.CheckResult.MissingFields")
                        + Const.CR;
              }
              errorFound = true;
              errorMessage += "\t\t" + parameterName + Const.CR;
            }

            for (String positionalReference : missingPositionalParameterFields) {
              if (first) {
                first = false;
                errorMessage +=
                    BaseMessages.getString(PKG, "DatabaseJoinMeta.CheckResult.MissingFields")
                        + Const.CR;
              }
              errorFound = true;
              errorMessage += "\t\t" + positionalReference + Const.CR;
            }

            if (errorFound) {
              cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
            } else {
              cr =
                  new CheckResult(
                      ICheckResult.TYPE_RESULT_OK,
                      BaseMessages.getString(PKG, "DatabaseJoinMeta.CheckResult.AllFieldsFound"),
                      transformMeta);
            }
            remarks.add(cr);
          } else {
            errorMessage =
                BaseMessages.getString(PKG, "DatabaseJoinMeta.CheckResult.CounldNotReadFields")
                    + Const.CR;
            cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
            remarks.add(cr);
          }
        }
      } catch (HopException e) {
        errorMessage =
            BaseMessages.getString(PKG, "DatabaseJoinMeta.CheckResult.ErrorOccurred")
                + e.getMessage();
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } finally {
        db.close();
      }
    } else {
      errorMessage = BaseMessages.getString(PKG, "DatabaseJoinMeta.CheckResult.InvalidConnection");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "DatabaseJoinMeta.CheckResult.ReceivingInfo"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "DatabaseJoinMeta.CheckResult.NoInputReceived"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public IRowMeta getTableFields(IVariables variables) {
    DatabaseMeta databaseMeta =
        getParentTransformMeta().getParentPipelineMeta().findDatabase(connection, variables);

    IRowMeta fields = null;
    if (databaseMeta != null) {
      Database db = new Database(loggingObject, variables, databaseMeta);
      databases = new Database[] {db}; // Keep track of this one for cancelQuery

      SqlParameterSpec parameterSpec = null;
      try {
        parameterSpec = parseSqlParameterSpec(resolveSql(variables));
        db.connect();
        IRowMeta param = createMetadataLookupParameterRowMeta(parameterSpec);
        fields =
            db.getQueryFields(
                parameterSpec.getPreparedSql(),
                true,
                param,
                createMetadataLookupParameterRowData(param));
      } catch (HopException dbe) {
        if (parameterSpec != null && isLikelyStoredProcedureSql(parameterSpec.getPreparedSql())) {
          logDetailed(
              "Unable to determine stored procedure output fields in getTableFields(); deferring metadata to runtime.");
          logDebug("Stored procedure getTableFields metadata discovery exception", dbe);
          return null;
        }
        logError(
            BaseMessages.getString(PKG, "DatabaseJoinMeta.Log.DatabaseErrorOccurred")
                + dbe.getMessage());
      } finally {
        db.close();
      }
    }
    return fields;
  }

  @Override
  public void analyseImpact(
      IVariables variables,
      List<DatabaseImpact> impact,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    // Find the lookupfields...
    //
    IRowMeta out = prev.clone();
    getFields(
        out,
        transformMeta.getName(),
        new IRowMeta[] {
          info,
        },
        null,
        variables,
        metadataProvider);

    try {
      DatabaseMeta databaseMeta =
          metadataProvider.getSerializer(DatabaseMeta.class).load(variables.resolve(connection));

      if (out != null) {
        for (int i = 0; i < out.size(); i++) {
          IValueMeta outvalue = out.getValueMeta(i);
          DatabaseImpact di =
              new DatabaseImpact(
                  DatabaseImpact.TYPE_IMPACT_READ,
                  pipelineMeta.getName(),
                  transformMeta.getName(),
                  databaseMeta.getDatabaseName(),
                  "",
                  outvalue.getName(),
                  outvalue.getName(),
                  transformMeta.getName(),
                  resolveSql(variables),
                  BaseMessages.getString(PKG, "DatabaseJoinMeta.DatabaseImpact.Title"));
          impact.add(di);
        }
      }
    } catch (HopException e) {
      throw new HopTransformException(
          "Unable to get databaseMeta for connection: " + Const.CR + variables.resolve(connection),
          e);
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }
}
