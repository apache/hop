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

package org.apache.hop.pipeline.transforms.formula;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.formula.util.FormulaFieldsExtractor;
import org.apache.hop.pipeline.transforms.formula.util.FormulaParser;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.CellValue;
import org.apache.poi.ss.usermodel.DateUtil;

public class Formula extends BaseTransform<FormulaMeta, FormulaData> {

  private FormulaPoi[] poi;
  private List<String>[] formulaFieldLists;
  private final HashMap<String, String> replaceMap = new HashMap<>();

  @Override
  public boolean init() {
    return true;
  }

  @Override
  public void dispose() {
    if (poi != null) {
      for (final var it : poi) {
        try {
          it.destroy();
        } catch (IOException e) {
          logError("Unable to close temporary workbook", e);
        }
      }
    }
    super.dispose();
  }

  @Override
  public void batchComplete() throws HopException {
    super.batchComplete();
    for (final var it : poi) {
      it.reset();
    }
  }

  @Override
  public boolean processRow() throws HopException {

    Object[] r = getRow();
    if (r == null) {
      setOutputDone();
      return false;
    }

    if (first) {
      first = false;

      try {
        data.outputRowMeta = getInputRowMeta().clone();
        meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);
      } catch (HopTransformException e) {
        throw new RuntimeException(e);
      }

      data.returnType = new int[meta.getFormulas().size()];
      for (int i = 0; i < meta.getFormulas().size(); i++) {
        data.returnType[i] = -1;
      }

      // Calculate replace indexes...
      //
      data.replaceIndex = new int[meta.getFormulas().size()];
      for (int j = 0; j < meta.getFormulas().size(); j++) {
        FormulaMetaFunction fn = meta.getFormulas().get(j);
        if (!Utils.isEmpty(fn.getReplaceField())) {
          data.replaceIndex[j] = data.outputRowMeta.indexOfValue(fn.getReplaceField());

          // keep track of the formula fields and the fields they replace for formula parsing later
          // on.
          replaceMap.put(fn.getFieldName(), fn.getReplaceField());
          if (data.replaceIndex[j] < 0) {
            throw new HopException(
                "Unknown field specified to replace with a formula result: ["
                    + fn.getReplaceField()
                    + "]");
          }
        } else {
          data.replaceIndex[j] = -1;
        }
      }

      // create one backing row per formula
      poi =
          IntStream.range(0, meta.getFormulas().size())
              .mapToObj(it -> new FormulaPoi(this::logDebug))
              .toArray(FormulaPoi[]::new);
      // compute only once for all rows the default field list
      formulaFieldLists =
          meta.getFormulas().stream()
              .map(FormulaMetaFunction::getFormula)
              .map(FormulaFieldsExtractor::getFormulaFieldList)
              .toArray(List[]::new);
    }

    int tempIndex = getInputRowMeta().size();

    if (isRowLevel()) {
      logRowlevel("Read row #" + getLinesRead() + " : " + Arrays.toString(r));
    }

    Object[] outputRowData = RowDataUtil.resizeArray(r, data.outputRowMeta.size());
    Object outputValue = null;

    for (int i = 0; i < meta.getFormulas().size(); i++) {

      FormulaMetaFunction formula = meta.getFormulas().get(i);
      FormulaParser parser =
          new FormulaParser(
              formula,
              data.outputRowMeta,
              outputRowData,
              poi[i],
              variables,
              replaceMap,
              formulaFieldLists[i]);
      try {
        CellValue cellValue = parser.getFormulaValue();
        CellType cellType = cellValue.getCellType();

        int outputValueType = formula.getValueType();
        switch (cellType) {
          case BLANK:
            // should never happen.
            break;
          case NUMERIC:
            outputValue = cellValue.getNumberValue();
            switch (outputValueType) {
              case IValueMeta.TYPE_NUMBER:
                data.returnType[i] = FormulaData.RETURN_TYPE_NUMBER;
                formula.setNeedDataConversion(outputValueType != IValueMeta.TYPE_NUMBER);
                break;
              case IValueMeta.TYPE_INTEGER:
                data.returnType[i] = FormulaData.RETURN_TYPE_INTEGER;
                formula.setNeedDataConversion(outputValueType != IValueMeta.TYPE_NUMBER);
                break;
              case IValueMeta.TYPE_BIGNUMBER:
                data.returnType[i] = FormulaData.RETURN_TYPE_BIGDECIMAL;
                formula.setNeedDataConversion(outputValueType != IValueMeta.TYPE_NUMBER);
                break;
              case IValueMeta.TYPE_DATE:
                outputValue = DateUtil.getJavaDate(cellValue.getNumberValue());
                data.returnType[i] = FormulaData.RETURN_TYPE_DATE;
                formula.setNeedDataConversion(outputValueType != IValueMeta.TYPE_NUMBER);
                break;
              case IValueMeta.TYPE_TIMESTAMP:
                outputValue =
                    Timestamp.from(DateUtil.getJavaDate(cellValue.getNumberValue()).toInstant());
                data.returnType[i] = FormulaData.RETURN_TYPE_TIMESTAMP;
                formula.setNeedDataConversion(outputValueType != IValueMeta.TYPE_NUMBER);
                break;
              default:
                break;
            }
            // get cell value
            break;
          case BOOLEAN:
            outputValue = cellValue.getBooleanValue();
            data.returnType[i] = FormulaData.RETURN_TYPE_BOOLEAN;
            formula.setNeedDataConversion(outputValueType != IValueMeta.TYPE_BOOLEAN);
            break;
          case STRING:
            outputValue = cellValue.getStringValue();
            data.returnType[i] = FormulaData.RETURN_TYPE_STRING;
            formula.setNeedDataConversion(outputValueType != IValueMeta.TYPE_STRING);
            break;
          default:
            break;
        }

        int realIndex = (data.replaceIndex[i] < 0) ? tempIndex++ : data.replaceIndex[i];

        outputRowData[realIndex] =
            getReturnValue(outputValue, data.returnType[i], realIndex, formula);
      } catch (Exception e) {
        throw new HopException(
            "Formula '" + formula.getFormula() + "' could not not be parsed ", e);
      }
    }

    putRow(data.outputRowMeta, outputRowData);

    if (isRowLevel()) {
      logRowlevel("Wrote row #" + getLinesWritten() + " : " + Arrays.toString(r));
    }
    if (checkFeedback(getLinesRead())) {
      logBasic("Linenr " + getLinesRead());
    }

    return true;
  }

  /**
   * This is the base transform that forms that basis for all transforms. You can derive from this
   * class to implement your own transforms.
   *
   * @param transformMeta The TransformMeta object to run.
   * @param meta
   * @param data the data object to store temporary data, database connections, caches, result sets,
   *     hashtables etc.
   * @param copyNr The copynumber for this transform.
   * @param pipelineMeta The PipelineMeta of which the transform transformMeta is part of.
   * @param pipeline The (running) pipeline to obtain information shared among the transforms.
   */
  public Formula(
      TransformMeta transformMeta,
      FormulaMeta meta,
      FormulaData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  protected Object getReturnValue(
      Object formulaResult, int returnType, int realIndex, FormulaMetaFunction fn)
      throws HopException {
    if (formulaResult == null) {
      return null;
    }
    Object value = null;
    switch (returnType) {
      case FormulaData.RETURN_TYPE_STRING:
        if (fn.isNeedDataConversion()) {
          value = convertDataToTargetValueMeta(realIndex, formulaResult);
        } else {
          value = formulaResult.toString();
        }
        break;
      case FormulaData.RETURN_TYPE_NUMBER:
        if (fn.isNeedDataConversion()) {
          value = convertDataToTargetValueMeta(realIndex, formulaResult);
        } else {
          value = ((Number) formulaResult).doubleValue();
        }
        break;
      case FormulaData.RETURN_TYPE_INTEGER:
        if (fn.isNeedDataConversion()) {
          value = convertDataToTargetValueMeta(realIndex, formulaResult);
        } else {
          value = formulaResult;
        }
        break;
      case FormulaData.RETURN_TYPE_LONG:
        if (fn.isNeedDataConversion()) {
          value = convertDataToTargetValueMeta(realIndex, formulaResult);
        } else {
          value = formulaResult;
        }
        break;
      case FormulaData.RETURN_TYPE_DATE:
        if (fn.isNeedDataConversion()) {
          value = convertDataToTargetValueMeta(realIndex, formulaResult);
        } else {
          value = formulaResult;
        }
        break;
      case FormulaData.RETURN_TYPE_BIGDECIMAL:
        if (fn.isNeedDataConversion()) {
          value = convertDataToTargetValueMeta(realIndex, formulaResult);
        } else {
          value = formulaResult;
        }
        break;
      case FormulaData.RETURN_TYPE_BYTE_ARRAY:
        value = formulaResult;
        break;
      case FormulaData.RETURN_TYPE_BOOLEAN:
        value = formulaResult;
        break;
      case FormulaData.RETURN_TYPE_TIMESTAMP:
        if (fn.isNeedDataConversion()) {
          value = convertDataToTargetValueMeta(realIndex, formulaResult);
        } else {
          value = formulaResult;
        }
        break;
    } // if none case is caught - null is returned.
    return value;
  }

  private Object convertDataToTargetValueMeta(int i, Object formulaResult) throws HopException {
    if (formulaResult == null) {
      return formulaResult;
    }
    IValueMeta target = data.outputRowMeta.getValueMeta(i);
    IValueMeta actual = ValueMetaFactory.guessValueMetaInterface(formulaResult);
    return target.convertData(actual, formulaResult);
  }
}
