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

  public IRowMeta getParameterRow(IRowMeta fields) {
    IRowMeta param = new RowMeta();

    if (fields != null) {
      for (ParameterField field : this.parameters) {
        IValueMeta valueMeta = fields.searchValueMeta(field.getName());
        if (valueMeta != null) {
          param.addValueMeta(valueMeta);
        }
      }
    }

    return param;
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
      IRowMeta param = getParameterRow(row);

      String sqlToUse;
      try {
        sqlToUse = resolveSql(variables);
      } catch (HopException e) {
        throw new HopTransformException(e.getMessage(), e);
      }

      // First try without connecting to the database... (can be S L O W)
      // See if it's in the cache...
      //
      IRowMeta add = null;
      try {
        add = db.getQueryFields(sqlToUse, true, param, new Object[param.size()]);
      } catch (HopDatabaseException dbe) {
        throw new HopTransformException(
            BaseMessages.getString(PKG, "DatabaseJoinMeta.Exception.UnableToDetermineQueryFields")
                + Const.CR
                + sqlToUse,
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
          add = db.getQueryFields(sqlToUse, true, param, new Object[param.size()]);
          for (int i = 0; i < add.size(); i++) {
            IValueMeta v = add.getValueMeta(i);
            v.setOrigin(name);
          }
          row.addRowMeta(add);
        } catch (HopDatabaseException dbe) {
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
        db.connect();
        String sqlToUse = resolveSql(variables);
        if (!Utils.isEmpty(sqlToUse)) {
          IRowMeta param = getParameterRow(prev);

          errorMessage = "";

          IRowMeta r = db.getQueryFields(sqlToUse, true, param, new Object[param.size()]);
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

          int q = db.countParameters(sqlToUse);
          if (q != parameters.size()) {
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
                    + parameters.size();

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
        }

        // Look up fields in the input stream <prev>
        if (prev != null && !prev.isEmpty()) {
          boolean first = true;
          errorMessage = "";
          boolean errorFound = false;

          for (ParameterField field : this.parameters) {
            IValueMeta v = prev.searchValueMeta(field.getName());
            if (v == null) {
              if (first) {
                first = false;
                errorMessage +=
                    BaseMessages.getString(PKG, "DatabaseJoinMeta.CheckResult.MissingFields")
                        + Const.CR;
              }
              errorFound = true;
              errorMessage += "\t\t" + field.getName() + Const.CR;
            }
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
    // Build a dummy parameter row...
    //

    DatabaseMeta databaseMeta =
        getParentTransformMeta().getParentPipelineMeta().findDatabase(connection, variables);

    IRowMeta param = new RowMeta();
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

    IRowMeta fields = null;
    if (databaseMeta != null) {
      Database db = new Database(loggingObject, variables, databaseMeta);
      databases = new Database[] {db}; // Keep track of this one for cancelQuery

      try {
        db.connect();
        fields = db.getQueryFields(resolveSql(variables), true, param, new Object[param.size()]);
      } catch (HopException dbe) {
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
