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

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/** Generates a number of (empty or the same) rows */
public class Constant extends BaseTransform<ConstantMeta, ConstantData>
    implements ITransform<ConstantMeta, ConstantData> {
  private static final Class<?> PKG = ConstantMeta.class; // For Translator

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
    Object[] rowData = new Object[meta.getFields().size()];

    for (int i = 0; i < meta.getFields().size(); i++) {
      ConstantField field = meta.getFields().get(i);
      int valtype = ValueMetaFactory.getIdForValueMeta(field.getFieldType());
      if (field.getFieldName() != null) {
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

        if (field.isEmptyString()) {
          // Just set empty string
          rowData[i] = StringUtil.EMPTY_STRING;
        } else {

          String stringValue = field.getValue();

          // If the value is empty: consider it to be NULL.
          if (stringValue == null || stringValue.length() == 0) {
            rowData[i] = null;

            if (value.getType() == IValueMeta.TYPE_NONE) {
              String message =
                  BaseMessages.getString(
                      PKG, "Constant.CheckResult.SpecifyTypeError", value.getName(), stringValue);
              remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_ERROR, message, null));
            }
          } else {
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

                  rowData[i] = new Double(data.nf.parse(stringValue).doubleValue());
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
                rowData[i] = stringValue;
                break;

              case IValueMeta.TYPE_DATE:
                try {
                  if (field.getFieldFormat() != null) {
                    data.daf.applyPattern(field.getFieldFormat());
                    data.daf.setDateFormatSymbols(data.dafs);
                  }

                  rowData[i] = data.daf.parse(stringValue);
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
                  rowData[i] = new Long(Long.parseLong(stringValue));
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
                  rowData[i] = new BigDecimal(stringValue);
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
                rowData[i] =
                    Boolean.valueOf(
                        "Y".equalsIgnoreCase(stringValue) || "TRUE".equalsIgnoreCase(stringValue));
                break;

              case IValueMeta.TYPE_BINARY:
                rowData[i] = stringValue.getBytes();
                break;

              case IValueMeta.TYPE_TIMESTAMP:
                try {
                  rowData[i] = Timestamp.valueOf(stringValue);
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

              default:
                String message =
                    BaseMessages.getString(
                        PKG, "Constant.CheckResult.SpecifyTypeError", value.getName(), stringValue);
                remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_ERROR, message, null));
            }
          }
        }
        // Now add value to the row!
        // This is in fact a copy from the fields row, but now with data.
        rowMeta.addValueMeta(value);
      } // end if
    } // end for

    return new RowMetaAndData(rowMeta, rowData);
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

    if (log.isRowLevel()) {
      logRowlevel(
          BaseMessages.getString(
              PKG,
              "Constant.Log.Wrote.Row",
              Long.toString(getLinesWritten()),
              getInputRowMeta().getString(r)));
    }

    if (checkFeedback(getLinesWritten()) && log.isBasic()) {
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
      List<ICheckResult> remarks = new ArrayList<>(); // stores the errors...
      data.constants = buildRow(meta, data, remarks);
      if (remarks.isEmpty()) {
        return true;
      } else {
        for (int i = 0; i < remarks.size(); i++) {
          ICheckResult cr = remarks.get(i);
          logError(cr.getText());
        }
      }
    }
    return false;
  }
}
