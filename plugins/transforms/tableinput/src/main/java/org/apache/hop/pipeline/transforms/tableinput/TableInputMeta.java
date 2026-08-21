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

package org.apache.hop.pipeline.transforms.tableinput;

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
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
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
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.apache.hop.pipeline.transform.stream.IStream.StreamType;
import org.apache.hop.pipeline.transform.stream.Stream;
import org.apache.hop.pipeline.transform.stream.StreamIcon;

@Transform(
    id = "TableInput",
    image = "tableinput.svg",
    name = "i18n::TableInput.Name",
    description = "i18n::TableInput.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    documentationUrl = "/pipeline/transforms/tableinput.html",
    keywords = "i18n::TableInputMeta.keyword",
    actionTransformTypes = {ActionTransformType.INPUT, ActionTransformType.RDBMS})
@Getter
@Setter
public class TableInputMeta extends BaseTransformMeta<TableInput, TableInputData> {

  private static final Class<?> PKG = TableInputMeta.class;

  @HopMetadataProperty(
      key = "sql",
      injectionKey = "SQL",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_SQL_SELECT)
  private String sql;

  @HopMetadataProperty(key = "limit", injectionKey = "LIMIT")
  private String rowLimit;

  /** Should I execute once per row? */
  @HopMetadataProperty(key = "execute_each_row", injectionKey = "EXECUTE_FOR_EACH_ROW")
  private boolean executeEachInputRow;

  @HopMetadataProperty(key = "variables_active", injectionKey = "REPLACE_VARIABLES")
  private boolean variableReplacementActive;

  @HopMetadataProperty(
      key = "connection",
      injectionKey = "CONNECTIONNAME",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
  private String connection;

  @HopMetadataProperty private String lookup;

  /**
   * When set, SQL is loaded from this file (VFS path, supports variables). SQL editor is read-only.
   */
  @HopMetadataProperty(key = "sql_from_file", injectionKey = "SQL_FROM_FILE")
  private String sqlFromFile;

  /**
   * When true, {@code {fieldName}} in SQL is bound as a prepared-statement parameter. Defaults to
   * false for existing metadata (HopMetadataProperty default). New transforms enable this in the
   * constructor.
   */
  @HopMetadataProperty(
      key = "use_named_parameters",
      injectionKey = "USE_NAMED_PARAMETERS",
      injectionKeyDescription = "TableInputMeta.Injection.UseNamedParameters")
  private boolean useNamedParameters;

  /** When true, output fields come from {@link #fields} instead of the database query metadata. */
  @HopMetadataProperty(
      key = "specify_fields",
      injectionKey = "SPECIFY_FIELDS",
      injectionKeyDescription = "TableInputMeta.Injection.SpecifyFields")
  private boolean specifyFields;

  /**
   * When true (and {@link #specifyFields} is true), the query result metadata is compared with
   * {@link #fields} and the transform fails on missing columns or type mismatches.
   */
  @HopMetadataProperty(
      key = "validate_specified_fields",
      injectionKey = "VALIDATE_SPECIFIED_FIELDS",
      injectionKeyDescription = "TableInputMeta.Injection.ValidateSpecifiedFields")
  private boolean validateSpecifiedFields;

  @HopMetadataProperty(
      key = "field",
      groupKey = "fields",
      injectionGroupKey = "OUTPUT_FIELDS",
      injectionGroupDescription = "TableInputMeta.Injection.OutputFields",
      injectionKeyDescription = "TableInputMeta.Injection.OutputField")
  private List<TableInputField> fields;

  public TableInputMeta() {
    super();
    this.fields = new ArrayList<>();
    this.useNamedParameters = true;
  }

  @Override
  public void setDefault() {
    sql = "SELECT <values> FROM <table name> WHERE <conditions>";
    rowLimit = "0";
  }

  @Override
  public void getFields(
      IRowMeta row,
      String origin,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    IRowMeta incomingParameters = row.clone();
    row.clear();

    if (specifyFields) {
      addSpecifiedFields(row, origin, variables);
      return;
    }

    boolean param = false;

    DatabaseMeta databaseMeta = null;

    try {
      databaseMeta =
          metadataProvider.getSerializer(DatabaseMeta.class).load(variables.resolve(connection));
    } catch (HopException e) {
      throw new HopTransformException(
          "Unable to get databaseMeta for connection: " + Const.CR + variables.resolve(connection),
          e);
    }

    Database db = new Database(loggingObject, variables, databaseMeta);
    super.databases = new Database[] {db}; // keep track of it for canceling purposes...

    // First try without connecting to the database... (can be S L O W)
    String effectiveSql;
    try {
      effectiveSql = getEffectiveSql(variables);
    } catch (HopException e) {
      throw new HopTransformException(e.getMessage(), e);
    }
    String sNewSql = effectiveSql;
    if (isVariableReplacementActive()) {
      sNewSql = db.resolve(effectiveSql);
      if (variables != null) {
        sNewSql = variables.resolve(sNewSql);
      }
    }

    String jdbcSql = sNewSql;
    if (useNamedParameters) {
      try {
        jdbcSql = TableInputSql.parse(sNewSql).getJdbcSql();
      } catch (HopException e) {
        throw new HopTransformException(e.getMessage(), e);
      }
    }

    IRowMeta add = null;
    try {
      add = db.getQueryFields(jdbcSql, param);
    } catch (HopDatabaseException dbe) {
      throw new HopTransformException(
          "Unable to get queryfields for SQL: " + Const.CR + jdbcSql, dbe);
    }

    if (add != null) {
      for (int i = 0; i < add.size(); i++) {
        IValueMeta v = add.getValueMeta(i);
        v.setOrigin(origin);
      }
      row.addRowMeta(add);
    } else {
      try {
        db.connect();

        IRowMeta paramRowMeta = null;
        Object[] paramData = null;

        IRowMeta incoming = parameterRowMeta(info, incomingParameters);
        if (incoming != null && !incoming.isEmpty()) {
          param = true;
          paramRowMeta = incoming;
          paramData = RowDataUtil.allocateRowData(paramRowMeta.size());
        }

        add = db.getQueryFields(jdbcSql, param, paramRowMeta, paramData);

        if (add == null) {
          return;
        }
        for (int i = 0; i < add.size(); i++) {
          IValueMeta v = add.getValueMeta(i);
          v.setOrigin(origin);
        }
        row.addRowMeta(add);
      } catch (HopException ke) {
        throw new HopTransformException(
            "Unable to get queryfields for SQL: " + Const.CR + jdbcSql, ke);
      } finally {
        db.disconnect();
      }
    }
  }

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
            BaseMessages.getString(PKG, "TableInputMeta.Exception.CouldNotLoadSqlFromFile", path),
            e);
      }
    }
    return sql;
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

    String effectiveSql = null;
    try {
      effectiveSql = getEffectiveSql(variables);
    } catch (HopException e) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              "Could not get SQL: " + e.getMessage(),
              transformMeta);
      remarks.add(cr);
    }

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
                  "TableInputMeta.CheckResult.DatabaseMetaError",
                  variables.resolve(connection)),
              transformMeta);
      remarks.add(cr);
    }

    if (databaseMeta != null) {
      cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, "Connection exists", transformMeta);
      remarks.add(cr);

      Database db = new Database(loggingObject, variables, databaseMeta);
      super.databases = new Database[] {db}; // keep track of it for canceling purposes...

      try {
        db.connect();
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK, "Connection to database OK", transformMeta);
        remarks.add(cr);

        if (effectiveSql != null) {
          if (!Utils.isEmpty(effectiveSql)) {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_OK, "SQL statement is entered", transformMeta);
            remarks.add(cr);
          } else {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_ERROR, "SQL statement is missing.", transformMeta);
            remarks.add(cr);
          }
        }

        if (specifyFields) {
          if (Utils.isEmpty(fields) || fields.stream().allMatch(f -> Utils.isEmpty(f.getName()))) {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_ERROR,
                    BaseMessages.getString(PKG, "TableInput.Exception.SpecifyFieldsEmpty"),
                    transformMeta);
            remarks.add(cr);
          } else {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_OK,
                    BaseMessages.getString(PKG, "TableInputMeta.CheckResult.SpecifiedFieldsOk"),
                    transformMeta);
            remarks.add(cr);
          }
        }
      } catch (HopException e) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                "An error occurred: " + e.getMessage(),
                transformMeta);
        remarks.add(cr);
      } finally {
        db.close();
      }
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              "Please select or create a connection to use",
              transformMeta);
      remarks.add(cr);
    }

    IStream infoStream = getTransformIOMeta().getInfoStreams().get(0);
    IRowMeta parameterFields = parameterRowMeta(new IRowMeta[] {info}, prev);
    boolean hasIncoming = (input != null && input.length > 0) || (prev != null && !prev.isEmpty());

    if (!Utils.isEmpty(infoStream.getTransformName())) {
      boolean found = false;
      if (input != null) {
        for (String s : input) {
          if (infoStream.getTransformName().equalsIgnoreCase(s)) {
            found = true;
          }
        }
      }
      if (found) {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                "Previous transform to read info from ["
                    + infoStream.getTransformName()
                    + "] is found.",
                transformMeta));
      } else if (!hasIncoming) {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                "Previous transform to read info from ["
                    + infoStream.getTransformName()
                    + "] is not found.",
                transformMeta));
      }
    }

    String sqlForParams = (effectiveSql != null) ? effectiveSql : "";
    if (isVariableReplacementActive() && variables != null) {
      sqlForParams = variables.resolve(sqlForParams);
    }

    if (parameterFields != null && !parameterFields.isEmpty()) {
      checkSqlParameters(remarks, transformMeta, parameterFields, sqlForParams);
    } else if (effectiveSql != null && useNamedParameters) {
      try {
        TableInputSql.Parsed parsedSql = TableInputSql.parse(sqlForParams);
        if (parsedSql.hasNamedParameters()) {
          remarks.add(
              new CheckResult(
                  ICheckResult.TYPE_RESULT_ERROR,
                  BaseMessages.getString(
                      PKG, "TableInputMeta.CheckResult.NamedParametersNeedIncoming"),
                  transformMeta));
        }
      } catch (HopException e) {
        remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_ERROR, e.getMessage(), transformMeta));
      }
    }

    if (hasIncoming) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "TableInputMeta.CheckResult.ReadsIncomingHops"),
              transformMeta));
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "TableInputMeta.CheckResult.NoInputExpected"),
              transformMeta));
    }
  }

  /**
   * Parameter-row layout from incoming hops. The optional info stream (when {@code lookup} is set)
   * is preferred so named informational hops keep working; otherwise previous hops are used.
   */
  IRowMeta parameterRowMeta(IRowMeta[] info, IRowMeta prev) {
    if (info != null && info.length > 0 && info[0] != null && !info[0].isEmpty()) {
      return info[0];
    }
    if (prev != null && !prev.isEmpty()) {
      return prev;
    }
    return null;
  }

  /**
   * @param transforms optionally search the info transform in a list of transforms
   */
  @Override
  public void searchInfoAndTargetTransforms(List<TransformMeta> transforms) {
    List<IStream> infoStreams = getTransformIOMeta().getInfoStreams();
    for (IStream stream : infoStreams) {
      stream.setTransformMeta(TransformMeta.findTransform(transforms, stream.getSubject()));
    }
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

    try {
      DatabaseMeta databaseMeta =
          metadataProvider.getSerializer(DatabaseMeta.class).load(variables.resolve(connection));
      String effectiveSql = getEffectiveSql(variables);

      // Find the lookup fields.
      IRowMeta out = new RowMeta();

      getFields(
          out, transformMeta.getName(), new IRowMeta[] {info}, null, variables, metadataProvider);

      if (out != null) {
        for (int i = 0; i < out.size(); i++) {
          IValueMeta outvalue = out.getValueMeta(i);
          DatabaseImpact ii =
              new DatabaseImpact(
                  DatabaseImpact.TYPE_IMPACT_READ,
                  pipelineMeta.getName(),
                  transformMeta.getName(),
                  databaseMeta.getDatabaseName(),
                  "",
                  outvalue.getName(),
                  outvalue.getName(),
                  transformMeta.getName(),
                  effectiveSql,
                  "read from one or more database tables via SQL statement");
          impact.add(ii);
        }
      }
    } catch (HopException e) {
      throw new HopTransformException(
          "Unable to get databaseMeta for connection: " + Const.CR + variables.resolve(connection),
          e);
    }
  }

  /**
   * Returns the Input/Output metadata for this transform. The generator transform only produces
   * output, does not accept input!
   */
  @Override
  public ITransformIOMeta getTransformIOMeta() {
    ITransformIOMeta ioMeta = super.getTransformIOMeta(false);
    if (ioMeta == null) {

      ioMeta = new TransformIOMeta(true, true, false, false, false, false);

      IStream stream =
          new Stream(
              StreamType.INFO,
              null,
              BaseMessages.getString(PKG, "TableInputMeta.InfoStream.Description"),
              StreamIcon.INFO,
              lookup);
      ioMeta.addStream(stream);
      setTransformIOMeta(ioMeta);
    }

    return ioMeta;
  }

  /**
   * Build output row metadata from the specified field list. Used when {@link #specifyFields} is
   * enabled.
   */
  public IRowMeta createSpecifiedRowMeta(String origin, IVariables variables)
      throws HopTransformException {
    IRowMeta specified = new RowMeta();
    addSpecifiedFields(specified, origin, variables);
    return specified;
  }

  /**
   * Map specified output fields onto JDBC result columns by name.
   *
   * @param jdbcMeta metadata returned by the query
   * @param specifiedMeta metadata from {@link #createSpecifiedRowMeta}
   * @param validateTypes when true, Hop types must match; when false, values are converted later
   * @return jdbc index for each specified field
   */
  public int[] createSpecifiedMapping(
      IRowMeta jdbcMeta, IRowMeta specifiedMeta, boolean validateTypes) throws HopException {
    if (jdbcMeta == null) {
      throw new HopException(
          BaseMessages.getString(PKG, "TableInput.Exception.SpecifiedFieldMissing", ""));
    }
    int[] mapping = new int[specifiedMeta.size()];
    for (int i = 0; i < specifiedMeta.size(); i++) {
      IValueMeta specified = specifiedMeta.getValueMeta(i);
      int index = jdbcMeta.indexOfValue(specified.getName());
      if (index < 0) {
        throw new HopException(
            BaseMessages.getString(
                PKG, "TableInput.Exception.SpecifiedFieldMissing", specified.getName()));
      }
      mapping[i] = index;
      if (validateTypes) {
        IValueMeta jdbc = jdbcMeta.getValueMeta(index);
        if (jdbc.getType() != specified.getType()) {
          throw new HopException(
              BaseMessages.getString(
                  PKG,
                  "TableInput.Exception.SpecifiedFieldTypeMismatch",
                  specified.getName(),
                  specified.getTypeDesc(),
                  jdbc.getTypeDesc()));
        }
      }
    }
    return mapping;
  }

  private void addSpecifiedFields(IRowMeta row, String origin, IVariables variables)
      throws HopTransformException {
    if (Utils.isEmpty(fields)) {
      throw new HopTransformException(
          BaseMessages.getString(PKG, "TableInput.Exception.SpecifyFieldsEmpty"));
    }
    boolean added = false;
    try {
      for (TableInputField field : fields) {
        if (field == null || Utils.isEmpty(field.getName())) {
          continue;
        }
        IValueMeta valueMeta = field.toValueMeta(origin, variables);
        row.addValueMeta(valueMeta);
        added = true;
      }
    } catch (HopPluginException e) {
      throw new HopTransformException(e.getMessage(), e);
    }
    if (!added) {
      throw new HopTransformException(
          BaseMessages.getString(PKG, "TableInput.Exception.SpecifyFieldsEmpty"));
    }
  }

  private void checkSqlParameters(
      List<ICheckResult> remarks, TransformMeta transformMeta, IRowMeta info, String sqlForParams) {
    if (useNamedParameters) {
      try {
        TableInputSql.Parsed parsedSql = TableInputSql.parse(sqlForParams);
        if (parsedSql.hasNamedParameters()) {
          List<String> missing = new ArrayList<>();
          for (String name : parsedSql.getNamedParameters()) {
            if (info.indexOfValue(name) < 0 && !missing.contains(name)) {
              missing.add(name);
            }
          }
          if (missing.isEmpty()) {
            remarks.add(
                new CheckResult(
                    ICheckResult.TYPE_RESULT_OK,
                    BaseMessages.getString(
                        PKG,
                        "TableInputMeta.CheckResult.NamedParametersOk",
                        Integer.toString(parsedSql.getNamedParameters().size())),
                    transformMeta));
          } else {
            remarks.add(
                new CheckResult(
                    ICheckResult.TYPE_RESULT_ERROR,
                    BaseMessages.getString(
                        PKG,
                        "TableInputMeta.CheckResult.NamedParametersMissing",
                        String.join(", ", missing)),
                    transformMeta));
          }
          return;
        }
        checkPositionalParameters(
            remarks, transformMeta, info, parsedSql.getPositionalParameterCount());
      } catch (HopException e) {
        remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_ERROR, e.getMessage(), transformMeta));
      }
      return;
    }

    int count = 0;
    for (int i = 0; i < sqlForParams.length(); i++) {
      char c = sqlForParams.charAt(i);
      if (c == '\'') {
        do {
          i++;
          if (i >= sqlForParams.length()) {
            break;
          }
          c = sqlForParams.charAt(i);
        } while (c != '\'');
      }
      if (c == '?') {
        count++;
      }
    }
    checkPositionalParameters(remarks, transformMeta, info, count);
  }

  private void checkPositionalParameters(
      List<ICheckResult> remarks, TransformMeta transformMeta, IRowMeta info, int count) {
    if (count == info.size()) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              "This transform is expecting and receiving "
                  + info.size()
                  + " fields of input from the previous transform.",
              transformMeta));
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              "This transform is receiving "
                  + info.size()
                  + " but not the expected "
                  + count
                  + " fields of input from the previous transform.",
              transformMeta));
    }
  }
}
