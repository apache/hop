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

package org.apache.hop.pipeline.transforms.salesforceinput;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.salesforce.SalesforceConnectionUtils;
import org.apache.hop.pipeline.transforms.salesforce.SalesforceTransformMeta;

@Transform(
    id = "SalesforceInput",
    name = "i18n::SalesforceInput.TypeLongDesc.SalesforceInput",
    description = "i18n::SalesforceInput.TypeTooltipDesc.SalesforceInput",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    image = "SFI.svg",
    keywords = "i18n::SalesforceInputMeta.keyword",
    documentationUrl = "/pipeline/transforms/salesforceinput.html")
@Getter
@Setter
public class SalesforceInputMeta
    extends SalesforceTransformMeta<SalesforceInput, SalesforceInputData> {
  public static final String CONST_FIELDS = "fields";
  public static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

  private static final Class<?> PKG = SalesforceInputMeta.class;

  /** Flag indicating that we should include the generated SQL in the output */
  @HopMetadataProperty(key = "include_sql", injectionKey = "INCLUDE_SQL_IN_OUTPUT")
  private boolean includeSQL;

  /** The name of the field in the output containing the generated SQL */
  @HopMetadataProperty(key = "sql_field", injectionKey = "SQL_FIELDNAME")
  private String sqlField;

  /** Flag indicating that we should include the server Timestamp in the output */
  @HopMetadataProperty(key = "include_Timestamp", injectionKey = "INCLUDE_TIMESTAMP_IN_OUTPUT")
  private boolean includeTimestamp;

  /** The name of the field in the output containing the server Timestamp */
  @HopMetadataProperty(key = "timstamp_field", injectionKey = "TIMESTAMP_FIELDNAME")
  private String timestampField;

  /** Flag indicating that we should include the filename in the output */
  @HopMetadataProperty(key = "include_targeturl", injectionKey = "INCLUDE_URL_IN_OUTPUT")
  private boolean includeTargetURL;

  /** The name of the field in the output containing the filename */
  @HopMetadataProperty(key = "targeturl_field", injectionKey = "URL_FIELDNAME")
  private String targetURLField;

  /** Flag indicating that we should include the module in the output */
  @HopMetadataProperty(key = "include_module", injectionKey = "INCLUDE_MODULE_IN_OUTPUT")
  private boolean includeModule;

  /** The name of the field in the output containing the module */
  @HopMetadataProperty(key = "module_field", injectionKey = "MODULE_FIELDNAME")
  private String moduleField;

  /** Flag indicating that a deletion date field should be included in the output */
  @HopMetadataProperty(
      key = "include_deletion_date",
      injectionKey = "INCLUDE_DELETION_DATE_IN_OUTPUT")
  private boolean includeDeletionDate;

  /** The name of the field in the output containing the deletion Date */
  @HopMetadataProperty(key = "deletion_date_field", injectionKey = "DELETION_DATE_FIELDNAME")
  private String deletionDateField;

  /** Flag indicating that a row number field should be included in the output */
  @HopMetadataProperty(key = "include_rownum", injectionKey = "INCLUDE_ROWNUM_IN_OUTPUT")
  private boolean includeRowNumber;

  /** The name of the field in the output containing the row number */
  @HopMetadataProperty(key = "rownum_field", injectionKey = "ROWNUM_FIELDNAME")
  private String rowNumberField;

  /** The condition */
  @HopMetadataProperty(key = "condition", injectionKey = "QUERY_CONDITION")
  private String condition;

  /** The maximum number or lines to read */
  @HopMetadataProperty(key = "limit", injectionKey = "LIMIT")
  private String rowLimit;

  /** The fields to return... */
  @HopMetadataProperty(
      key = "field",
      groupKey = "fields",
      injectionKey = "FIELD",
      injectionGroupKey = "FIELDS")
  private List<SalesforceInputField> fields;

  /** option: specify query */
  @HopMetadataProperty(key = "specifyQuery", injectionKey = "USE_SPECIFIED_QUERY")
  private boolean specifyQuery;

  // ** query entered by user **/
  @HopMetadataProperty(key = "query", injectionKey = "SPECIFY_QUERY")
  private String query;

  private int nrFields;

  @HopMetadataProperty(key = "read_to", injectionKey = "END_DATE")
  private String readTo;

  @HopMetadataProperty(key = "read_from", injectionKey = "START_DATE")
  private String readFrom;

  private int recordsFilterCode;

  /** records filter */
  @HopMetadataProperty(key = "records_filter")
  private String recordsFilter;

  /** Use Salesforce Field API Names instead of Labels for output fields */
  @HopMetadataProperty(key = "use_field_api_names", injectionKey = "USE_FIELD_API_NAMES")
  private boolean useFieldApiNames;

  /** Query all records including deleted ones */
  @HopMetadataProperty(key = "queryAll", injectionKey = "QUERY_ALL")
  private boolean queryAll;

  public SalesforceInputMeta() {
    super(); // allocate BaseTransformMeta
  }

  @Injection(name = "RETRIEVE")
  public void setRecordsFilterDesc(String recordsFilterDesc) {
    this.recordsFilterCode = SalesforceConnectionUtils.getRecordsFilterByDesc(recordsFilterDesc);
  }

  /**
   * @return Returns the includeTargetURL.
   */
  public boolean includeTargetURL() {
    return includeTargetURL;
  }

  /**
   * @return Returns the includeTimestamp.
   */
  public boolean includeTimestamp() {
    return includeTimestamp;
  }

  /**
   * @return Returns the includeModule.
   */
  public boolean includeModule() {
    return includeModule;
  }

  /**
   * @return Returns the includeRowNumber.
   */
  public boolean includeRowNumber() {
    return includeRowNumber;
  }

  /**
   * @return Returns the includeDeletionDate.
   */
  public boolean includeDeletionDate() {
    return includeDeletionDate;
  }

  @Override
  public Object clone() {
    SalesforceInputMeta retval = (SalesforceInputMeta) super.clone();
    retval.fields = new ArrayList<SalesforceInputField>();

    int nrFields = fields.size();

    //    retval.allocate(nrFields);

    for (int i = 0; i < nrFields; i++) {
      if (fields.get(i) != null) {
        //        retval.inputFields.get(i) = (SalesforceInputField) inputFields.get(i).clone();
        retval.fields.add((SalesforceInputField) fields.get(i).clone());
      }
    }

    return retval;
  }

  @Override
  public void setDefault() {
    super.setDefault();
    setFields(new ArrayList<>());
    setIncludeDeletionDate(false);
    setQueryAll(false);
    setReadFrom("");
    setReadTo("");
    nrFields = 0;
    setSpecifyQuery(false);
    setQuery("");
    setCondition("");
    setIncludeTargetURL(false);
    setTargetURLField("");
    setIncludeModule(false);
    setModuleField("");
    setIncludeRowNumber(false);
    setRowNumberField("");
    setDeletionDateField("");
    setIncludeSQL(false);
    setSqlField("");
    setIncludeTimestamp(false);
    setTimestampField("");

    setRowLimit("0");
  }

  @Override
  public void getFields(
      IRowMeta r,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    int i;
    for (i = 0; i < fields.size(); i++) {
      SalesforceInputField field = fields.get(i);

      int type = field.getTypeCode();
      if (type == IValueMeta.TYPE_NONE) {
        type = IValueMeta.TYPE_STRING;
      }
      try {
        IValueMeta v = ValueMetaFactory.createValueMeta(variables.resolve(field.getName()), type);
        v.setLength(field.getLength());
        v.setPrecision(field.getPrecision());
        v.setOrigin(name);
        v.setConversionMask(field.getFormat());
        v.setDecimalSymbol(field.getDecimalSymbol());
        v.setGroupingSymbol(field.getGroupSymbol());
        v.setCurrencySymbol(field.getCurrencySymbol());
        r.addValueMeta(v);
      } catch (Exception e) {
        throw new HopTransformException(e);
      }
    }

    if (includeTargetURL && !Utils.isEmpty(targetURLField)) {
      IValueMeta v = new ValueMetaString(variables.resolve(targetURLField));
      v.setLength(250);
      v.setPrecision(-1);
      v.setOrigin(name);
      r.addValueMeta(v);
    }
    if (includeModule && !Utils.isEmpty(moduleField)) {
      IValueMeta v = new ValueMetaString(variables.resolve(moduleField));
      v.setLength(250);
      v.setPrecision(-1);
      v.setOrigin(name);
      r.addValueMeta(v);
    }

    if (includeSQL && !Utils.isEmpty(sqlField)) {
      IValueMeta v = new ValueMetaString(variables.resolve(sqlField));
      v.setLength(250);
      v.setPrecision(-1);
      v.setOrigin(name);
      r.addValueMeta(v);
    }
    if (includeTimestamp && !Utils.isEmpty(timestampField)) {
      IValueMeta v = new ValueMetaDate(variables.resolve(timestampField));
      v.setOrigin(name);
      r.addValueMeta(v);
    }

    if (includeRowNumber && !Utils.isEmpty(rowNumberField)) {
      IValueMeta v = new ValueMetaInteger(variables.resolve(rowNumberField));
      v.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH, 0);
      v.setOrigin(name);
      r.addValueMeta(v);
    }
    if (includeDeletionDate && !Utils.isEmpty(deletionDateField)) {
      IValueMeta v = new ValueMetaDate(variables.resolve(deletionDateField));
      v.setOrigin(name);
      r.addValueMeta(v);
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
    super.check(
        remarks,
        pipelineMeta,
        transformMeta,
        prev,
        input,
        output,
        info,
        variables,
        metadataProvider);
    CheckResult cr;

    // See if we get input...
    if (input != null && input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SalesforceInputMeta.CheckResult.NoInputExpected"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "SalesforceInputMeta.CheckResult.NoInput"),
              transformMeta);
    }
    remarks.add(cr);

    // check return fields
    if (getFields().size() == 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SalesforceInputMeta.CheckResult.NoFields"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "SalesforceInputMeta.CheckResult.FieldsOk"),
              transformMeta);
    }
    remarks.add(cr);

    // check additional fields
    if (includeTargetURL() && Utils.isEmpty(getTargetURLField())) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SalesforceInputMeta.CheckResult.NoTargetURLField"),
              transformMeta);
      remarks.add(cr);
    }
    if (isIncludeSQL() && Utils.isEmpty(getSqlField())) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SalesforceInputMeta.CheckResult.NoSQLField"),
              transformMeta);
      remarks.add(cr);
    }
    if (includeModule() && Utils.isEmpty(moduleField)) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SalesforceInputMeta.CheckResult.NoModuleField"),
              transformMeta);
      remarks.add(cr);
    }
    if (includeTimestamp() && Utils.isEmpty(getTimestampField())) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SalesforceInputMeta.CheckResult.NoTimestampField"),
              transformMeta);
      remarks.add(cr);
    }
    if (includeRowNumber() && Utils.isEmpty(getRowNumberField())) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SalesforceInputMeta.CheckResult.NoRowNumberField"),
              transformMeta);
      remarks.add(cr);
    }
    if (includeDeletionDate() && Utils.isEmpty(getDeletionDateField())) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SalesforceInputMeta.CheckResult.NoDeletionDateField"),
              transformMeta);
      remarks.add(cr);
    }
  }

  public SalesforceInputDialog getDialog(
      org.eclipse.swt.widgets.Shell shell,
      IVariables variables,
      org.apache.hop.pipeline.transform.ITransformMeta transformMeta,
      PipelineMeta pipelineMeta,
      String transformName) {
    return new SalesforceInputDialog(
        shell, variables, (SalesforceInputMeta) transformMeta, pipelineMeta);
  }
}
