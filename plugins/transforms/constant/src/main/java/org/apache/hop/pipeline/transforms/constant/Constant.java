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

package org.apache.hop.pipeline.transforms.constant;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Generates a number of (empty or the same) rows */
public class Constant extends BaseTransform<ConstantMeta, ConstantData> {
  private static final Class<?> PKG = ConstantMeta.class;

  public Constant(
      TransformMeta transformMeta,
      ConstantMeta meta,
      ConstantData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  public static final RowMetaAndData buildRow(
      ConstantMeta meta, ConstantData data, List<ICheckResult> remarks) {
    IRowMeta rowMeta = new RowMeta();
    // Collected in lockstep with rowMeta: a field that gets skipped below must not leave a hole
    // in the data, or every constant after it ends up in the previous field's column.
    List<Object> rowData = new ArrayList<>();

    int fieldNr = 0;
    for (ConstantField field : meta.getFields()) {
      fieldNr++;
      int valtype = ValueMetaFactory.getIdForValueMeta(field.getFieldType());
      // Skip unnamed fields exactly like ConstantMeta.getFields() does. That method builds the
      // transform's output row meta, so keeping a blank-named field here would make the constants
      // row one value wider than the meta describing it.
      if (StringUtils.isEmpty(field.getFieldName())) {
        // A field that was filled in but never named can't become a column. Say so rather than
        // dropping it quietly - it is nearly always a forgotten name, not a deliberate blank.
        if (hasContent(field)) {
          String message =
              BaseMessages.getString(PKG, "Constant.CheckResult.NoFieldNameWarning", fieldNr);
          remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_WARNING, message, null));
        }
      } else {
        IValueMeta value = null;
        try {
          value = ValueMetaFactory.createValueMeta(field.getFieldName(), valtype);
        } catch (Exception exception) {
          remarks.add(
              new CheckResult(ICheckResult.TYPE_RESULT_ERROR, exception.getMessage(), null));
          continue;
        }
        value.setLength(field.getFieldLength());
        value.setPrecision(field.getFieldPrecision());

        Object fieldValue = null;
        if (field.isEmptyString()) {
          // Just set empty string
          fieldValue = StringUtil.EMPTY_STRING;
        } else if (value.getType() == IValueMeta.TYPE_NONE) {
          // No value type was selected for this field, so there's nothing to convert to.
          String message =
              BaseMessages.getString(
                  PKG, "Constant.CheckResult.SpecifyTypeError", value.getName(), field.getValue());
          remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_ERROR, message, null));
        } else {

          String stringValue = field.getValue();

          // If the value is empty: consider it to be NULL.
          if (!Utils.isEmpty(stringValue)) {
            switch (value.getType()) {
              case IValueMeta.TYPE_NUMBER:
                try {
                  if (field.getFieldFormat() != null
                      || field.getDecimal() != null
                      || field.getGroup() != null
                      || field.getCurrency() != null) {
                    if (!StringUtils.isEmpty(field.getFieldFormat())) {
                      data.df.applyPattern(field.getFieldFormat());
                    }
                    if (!StringUtils.isEmpty(field.getDecimal())) {
                      data.dfs.setDecimalSeparator(field.getDecimal().charAt(0));
                    }
                    if (!StringUtils.isEmpty(field.getGroup())) {
                      data.dfs.setGroupingSeparator(field.getGroup().charAt(0));
                    }
                    if (!StringUtils.isEmpty(field.getCurrency())) {
                      data.dfs.setCurrencySymbol(field.getCurrency());
                    }

                    data.df.setDecimalFormatSymbols(data.dfs);
                  }

                  fieldValue = data.nf.parse(stringValue).doubleValue();
                } catch (Exception e) {
                  String message =
                      BaseMessages.getString(
                          PKG,
                          "Constant.BuildRow.Error.Parsing.Number",
                          value.getName(),
                          stringValue,
                          e.toString());
                  remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_ERROR, message, null));
                }
                break;

              case IValueMeta.TYPE_STRING:
                fieldValue = stringValue;
                break;

              case IValueMeta.TYPE_DATE:
                try {
                  if (field.getFieldFormat() != null) {
                    data.daf.applyPattern(field.getFieldFormat());
                    data.daf.setDateFormatSymbols(data.dafs);
                  }

                  fieldValue = data.daf.parse(stringValue);
                } catch (Exception e) {
                  String message =
                      BaseMessages.getString(
                          PKG,
                          "Constant.BuildRow.Error.Parsing.Date",
                          value.getName(),
                          stringValue,
                          e.toString());
                  remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_ERROR, message, null));
                }
                break;

              case IValueMeta.TYPE_INTEGER:
                try {
                  fieldValue = Long.valueOf(stringValue);
                } catch (Exception e) {
                  String message =
                      BaseMessages.getString(
                          PKG,
                          "Constant.BuildRow.Error.Parsing.Integer",
                          value.getName(),
                          stringValue,
                          e.toString());
                  remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_ERROR, message, null));
                }
                break;

              case IValueMeta.TYPE_BIGNUMBER:
                try {
                  fieldValue = new BigDecimal(stringValue);
                } catch (Exception e) {
                  String message =
                      BaseMessages.getString(
                          PKG,
                          "Constant.BuildRow.Error.Parsing.BigNumber",
                          value.getName(),
                          stringValue,
                          e.toString());
                  remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_ERROR, message, null));
                }
                break;

              case IValueMeta.TYPE_BOOLEAN:
                fieldValue =
                    "Y".equalsIgnoreCase(stringValue) || "TRUE".equalsIgnoreCase(stringValue);
                break;

              case IValueMeta.TYPE_BINARY:
                fieldValue = stringValue.getBytes();
                break;

              case IValueMeta.TYPE_TIMESTAMP:
                try {
                  fieldValue = Timestamp.valueOf(stringValue);
                } catch (Exception e) {
                  String message =
                      BaseMessages.getString(
                          PKG,
                          "Constant.BuildRow.Error.Parsing.Timestamp",
                          value.getName(),
                          stringValue,
                          e.toString());
                  remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_ERROR, message, null));
                }
                break;

              case IValueMeta.TYPE_INET:
                try {
                  fieldValue = InetAddress.getByName(stringValue);
                } catch (Exception e) {
                  String message =
                      BaseMessages.getString(
                          PKG,
                          "Constant.BuildRow.Error.Parsing.InternetAddress",
                          value.getName(),
                          stringValue,
                          e.toString());
                  remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_ERROR, message, null));
                }
                break;

              default:
                // Any other value type: let the value meta plugin itself do the conversion.
                // This way types like JSON and UUID work without a dedicated case here, and any
                // type that simply can't be built from text reports why instead of claiming that
                // no type was selected.
                try {
                  IValueMeta stringMeta = new ValueMetaString(field.getFieldName());
                  stringMeta.setConversionMask(field.getFieldFormat());

                  fieldValue = value.convertData(stringMeta, stringValue);
                } catch (Exception e) {
                  String message =
                      BaseMessages.getString(
                          PKG,
                          "Constant.BuildRow.Error.Parsing.Type",
                          value.getTypeDesc(),
                          value.getName(),
                          stringValue,
                          e.toString());
                  remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_ERROR, message, null));
                }
            }
          }
        }
        // Now add value to the row!
        // This is in fact a copy from the fields row, but now with data.
        rowMeta.addValueMeta(value);
        rowData.add(fieldValue);
      } // end if
    } // end for

    return new RowMetaAndData(rowMeta, rowData.toArray());
  }

  /**
   * Whether anything was filled in for this field. Used to tell a forgotten field name apart from a
   * leftover blank row, which carries nothing and is not worth reporting.
   */
  private static boolean hasContent(ConstantField field) {
    return field.isEmptyString()
        || StringUtils.isNotEmpty(field.getValue())
        || StringUtils.isNotEmpty(field.getFieldType())
        || StringUtils.isNotEmpty(field.getFieldFormat())
        || StringUtils.isNotEmpty(field.getCurrency())
        || StringUtils.isNotEmpty(field.getDecimal())
        || StringUtils.isNotEmpty(field.getGroup());
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] r = null;
    r = getRow();

    if (r == null) { // no more rows to be expected from the previous transform(s)
      setOutputDone();
      return false;
    }

    if (data.firstRow) {
      // The output meta is the original input meta + the
      // additional constant fields.

      data.firstRow = false;
      data.outputMeta = getInputRowMeta().clone();
      meta.getFields(data.outputMeta, getTransformName(), null, null, this, metadataProvider);
    }

    // Add the constant data to the end of the row.
    r = RowDataUtil.addRowData(r, getInputRowMeta().size(), data.getConstants().getData());

    putRow(data.outputMeta, r);

    if (isRowLevel()) {
      logRowlevel(
          BaseMessages.getString(
              PKG,
              "Constant.Log.Wrote.Row",
              Long.toString(getLinesWritten()),
              data.outputMeta.getString(r)));
    }

    if (checkFeedback(getLinesWritten()) && isBasic()) {
      logBasic(
          BaseMessages.getString(PKG, "Constant.Log.LineNr", Long.toString(getLinesWritten())));
    }

    return true;
  }

  @Override
  public boolean init() {

    data.firstRow = true;

    if (super.init()) {
      // Create a row (constants) with all the values in it...
      List<ICheckResult> remarks = new ArrayList<>(); // stores the errors and warnings...
      data.constants = buildRow(meta, data, remarks);

      // Only a genuinely unbuildable constant stops the transform. Warnings - a field that was
      // filled in but never named - are logged and the transform runs without that field.
      boolean initialized = true;
      for (ICheckResult cr : remarks) {
        if (cr.getType() == ICheckResult.TYPE_RESULT_ERROR) {
          logError(cr.getText());
          initialized = false;
        } else {
          logMinimal(cr.getText());
        }
      }
      return initialized;
    }
    return false;
  }
}
