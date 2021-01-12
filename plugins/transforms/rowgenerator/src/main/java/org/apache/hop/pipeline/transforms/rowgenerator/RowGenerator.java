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

package org.apache.hop.pipeline.transforms.rowgenerator;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Generates a number of (empty or the same) rows
 *
 * @author Matt
 * @since 4-apr-2003
 */
public class RowGenerator extends BaseTransform<RowGeneratorMeta, RowGeneratorData>
    implements ITransform<RowGeneratorMeta, RowGeneratorData> {

  private static final Class<?> PKG = RowGeneratorMeta.class; // For Translator

  public RowGenerator(
      TransformMeta transformMeta,
      RowGeneratorMeta meta,
      RowGeneratorData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  public static final RowMetaAndData buildRow(
      RowGeneratorMeta meta, List<ICheckResult> remarks, String origin) throws HopPluginException {
    IRowMeta rowMeta = new RowMeta();
    Object[] rowData = RowDataUtil.allocateRowData(meta.getFieldName().length + 2);
    int index = 0;

    if (meta.isNeverEnding()) {
      if (!Utils.isEmpty(meta.getRowTimeField())) {
        rowMeta.addValueMeta(new ValueMetaDate(meta.getRowTimeField()));
        rowData[index++] = null;
      }

      if (!Utils.isEmpty(meta.getLastTimeField())) {
        rowMeta.addValueMeta(new ValueMetaDate(meta.getLastTimeField()));
        rowData[index++] = null;
      }
    }

    for (int i = 0; i < meta.getFieldName().length; i++) {
      int valtype = ValueMetaFactory.getIdForValueMeta(meta.getFieldType()[i]);
      if (meta.getFieldName()[i] != null) {
        IValueMeta valueMeta =
            ValueMetaFactory.createValueMeta(meta.getFieldName()[i], valtype); // build a
        // value!
        valueMeta.setLength(meta.getFieldLength()[i]);
        valueMeta.setPrecision(meta.getFieldPrecision()[i]);
        valueMeta.setConversionMask(meta.getFieldFormat()[i]);
        valueMeta.setCurrencySymbol(meta.getCurrency()[i]);
        valueMeta.setGroupingSymbol(meta.getGroup()[i]);
        valueMeta.setDecimalSymbol(meta.getDecimal()[i]);
        valueMeta.setOrigin(origin);

        IValueMeta stringMeta = ValueMetaFactory.cloneValueMeta(valueMeta, IValueMeta.TYPE_STRING);

        if (meta.isSetEmptyString() != null && meta.isSetEmptyString()[i]) {
          // Set empty string
          rowData[index] = StringUtil.EMPTY_STRING;
        } else {
          String stringValue = meta.getValue()[i];

          // If the value is empty: consider it to be NULL.
          if (Utils.isEmpty(stringValue)) {
            rowData[index] = null;

            if (valueMeta.getType() == IValueMeta.TYPE_NONE) {
              String message =
                  BaseMessages.getString(
                      PKG,
                      "RowGenerator.CheckResult.SpecifyTypeError",
                      valueMeta.getName(),
                      stringValue);
              remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_ERROR, message, null));
            }
          } else {
            // Convert the data from String to the specified type ...
            //
            try {
              rowData[index] = valueMeta.convertData(stringMeta, stringValue);
            } catch (HopValueException e) {
              switch (valueMeta.getType()) {
                case IValueMeta.TYPE_NUMBER:
                  String message =
                      BaseMessages.getString(
                          PKG,
                          "RowGenerator.BuildRow.Error.Parsing.Number",
                          valueMeta.getName(),
                          stringValue,
                          e.toString());
                  remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_ERROR, message, null));
                  break;

                case IValueMeta.TYPE_DATE:
                  message =
                      BaseMessages.getString(
                          PKG,
                          "RowGenerator.BuildRow.Error.Parsing.Date",
                          valueMeta.getName(),
                          stringValue,
                          e.toString());
                  remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_ERROR, message, null));
                  break;

                case IValueMeta.TYPE_INTEGER:
                  message =
                      BaseMessages.getString(
                          PKG,
                          "RowGenerator.BuildRow.Error.Parsing.Integer",
                          valueMeta.getName(),
                          stringValue,
                          e.toString());
                  remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_ERROR, message, null));
                  break;

                case IValueMeta.TYPE_BIGNUMBER:
                  message =
                      BaseMessages.getString(
                          PKG,
                          "RowGenerator.BuildRow.Error.Parsing.BigNumber",
                          valueMeta.getName(),
                          stringValue,
                          e.toString());
                  remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_ERROR, message, null));
                  break;

                case IValueMeta.TYPE_TIMESTAMP:
                  message =
                      BaseMessages.getString(
                          PKG,
                          "RowGenerator.BuildRow.Error.Parsing.Timestamp",
                          valueMeta.getName(),
                          stringValue,
                          e.toString());
                  remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_ERROR, message, null));
                  break;

                default:
                  // Boolean and binary don't throw errors normally, so it's probably an unspecified
                  // error problem...
                  message =
                      BaseMessages.getString(
                          PKG,
                          "RowGenerator.CheckResult.SpecifyTypeError",
                          valueMeta.getName(),
                          stringValue);
                  remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_ERROR, message, null));
                  break;
              }
            }
          }
        }

        // Now add value to the row!
        // This is in fact a copy from the fields row, but now with data.
        rowMeta.addValueMeta(valueMeta);
        index++;
      }
    }

    return new RowMetaAndData(rowMeta, rowData);
  }

  @Override
  public synchronized boolean processRow() throws HopException {

    Object[] r = null;
    boolean retval = true;

    if (first) {
      first = false;
      getRow();
    } else {
      if (meta.isNeverEnding() && data.delay > 0) {
        try {
          Thread.sleep(data.delay);
        } catch (InterruptedException e) {
          throw new HopException(e);
        }
      }
    }

    if ((meta.isNeverEnding() || data.rowsWritten < data.rowLimit) && !isStopped()) {
      r = data.outputRowMeta.cloneRow(data.outputRowData);
    } else {
      setOutputDone(); // signal end to receiver(s)
      return false;
    }

    if (meta.isNeverEnding()) {
      data.prevDate = data.rowDate;
      data.rowDate = new Date();

      int index = 0;
      if (!Utils.isEmpty(meta.getRowTimeField())) {
        r[index++] = data.rowDate;
      }
      if (!Utils.isEmpty(meta.getLastTimeField())) {
        r[index++] = data.prevDate;
      }
    }

    putRow(data.outputRowMeta, r);
    data.rowsWritten++;

    if (log.isRowLevel()) {
      logRowlevel(
          BaseMessages.getString(
              PKG,
              "RowGenerator.Log.Wrote.Row",
              Long.toString(data.rowsWritten),
              data.outputRowMeta.getString(r)));
    }

    if (checkFeedback(data.rowsWritten)) {
      if (log.isBasic()) {
        logBasic(
            BaseMessages.getString(
                PKG, "RowGenerator.Log.LineNr", Long.toString(data.rowsWritten)));
      }
    }

    return retval;
  }

  @Override
  public boolean init() {
    try {

      if (super.init()) {
        // Determine the number of rows to generate...
        data.rowLimit = Const.toLong( resolve(meta.getRowLimit()), -1L);
        data.rowsWritten = 0L;
        data.delay = Const.toLong( resolve(meta.getIntervalInMs()), -1L);

        if (data.rowLimit < 0L) { // Unable to parse
          logError(BaseMessages.getString(PKG, "RowGenerator.Wrong.RowLimit.Number"));
          return false; // fail
        }

        // Create a row (constants) with all the values in it...
        List<ICheckResult> remarks = new ArrayList<>(); // stores the errors...
        RowMetaAndData outputRow = buildRow(meta, remarks, getTransformName());
        if (!remarks.isEmpty()) {
          for (int i = 0; i < remarks.size(); i++) {
            CheckResult cr = (CheckResult) remarks.get(i);
            logError(cr.getText());
          }
          return false;
        }

        data.outputRowData = outputRow.getData();
        data.outputRowMeta = outputRow.getRowMeta();
        return true;
      }
      return false;
    } catch (Exception e) {
      setErrors(1L);
      logError("Error initializing transform", e);
      return false;
    }
  }

  @Override
  public boolean canProcessOneRow() {
    return true;
  }
}
