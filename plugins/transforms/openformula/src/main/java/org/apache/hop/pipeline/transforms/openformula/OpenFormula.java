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

package org.apache.hop.pipeline.transforms.openformula;

import java.math.BigDecimal;
import java.util.Date;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.openformula.Formula;
import org.apache.hop.openformula.error.FormulaErrorValue;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Evaluate OpenFormula expressions.
 *
 * <p>Preserves PDI Formula step semantics for Apache Hop.
 */
public class OpenFormula extends BaseTransform<OpenFormulaMeta, OpenFormulaData> {
  private static final Class<?> PKG = OpenFormula.class;

  public OpenFormula(
      TransformMeta transformMeta,
      OpenFormulaMeta meta,
      OpenFormulaData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean init() {
    if (!super.init()) {
      return false;
    }
    data.returnType = new int[meta.getFormulas().size()];
    data.needDataConversion = new boolean[meta.getFormulas().size()];
    for (int i = 0; i < meta.getFormulas().size(); i++) {
      data.returnType[i] = -1;
    }
    return true;
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

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

      data.context = new RowFormulaContext(data.outputRowMeta);

      data.replaceIndex = new int[meta.getFormulas().size()];
      for (int i = 0; i < meta.getFormulas().size(); i++) {
        OpenFormulaMetaFunction fn = meta.getFormulas().get(i);
        if (!Utils.isEmpty(fn.getReplaceField())) {
          data.replaceIndex[i] = getInputRowMeta().indexOfValue(fn.getReplaceField());
          if (data.replaceIndex[i] < 0) {
            throw new HopException(
                BaseMessages.getString(
                    PKG, "OpenFormulaMeta.Exception.UnknownReplaceField", fn.getReplaceField()));
          }
        } else {
          data.replaceIndex[i] = -1;
        }
      }
    }

    if (isRowLevel()) {
      logRowlevel("Read row #" + getLinesRead() + " : " + getInputRowMeta().getString(r));
    }

    Object[] outputRowData;
    try {
      outputRowData = calcFields(getInputRowMeta(), r);
    } catch (Exception e) {
      return handleFormulaError(r, e);
    }

    putRow(data.outputRowMeta, outputRowData);

    if (isRowLevel()) {
      logRowlevel(
          "Wrote row #" + getLinesWritten() + " : " + data.outputRowMeta.getString(outputRowData));
    }
    if (checkFeedback(getLinesRead()) && isBasic()) {
      logBasic("Linenr " + getLinesRead());
    }
    return true;
  }

  Object[] calcFields(IRowMeta rowMeta, Object[] r) throws HopException {
    try {
      Object[] outputRowData = RowDataUtil.createResizedCopy(r, data.outputRowMeta.size());
      int tempIndex = rowMeta.size();

      data.context.setRowData(outputRowData);

      if (data.formulas == null) {
        data.formulas = new Formula[meta.getFormulas().size()];
        for (int i = 0; i < meta.getFormulas().size(); i++) {
          OpenFormulaMetaFunction fn = meta.getFormulas().get(i);
          if (!Utils.isEmpty(fn.getFieldName())) {
            data.formulas[i] = data.createFormula(fn.getFormula());
          } else {
            throw new HopException(
                BaseMessages.getString(
                    PKG, "OpenFormula.Exception.MissingFieldName", Const.NVL(fn.getFormula(), "")));
          }
        }
      }

      for (int i = 0; i < meta.getFormulas().size(); i++) {
        OpenFormulaMetaFunction fn = meta.getFormulas().get(i);
        if (Utils.isEmpty(fn.getFieldName())) {
          continue;
        }
        if (data.formulas[i] == null) {
          data.formulas[i] = data.createFormula(fn.getFormula());
        }

        Object formulaResult = data.formulas[i].evaluate();
        if (formulaResult instanceof FormulaErrorValue errorValue) {
          throw new HopException(
              BaseMessages.getString(
                  PKG,
                  "OpenFormula.Exception.FormulaError",
                  fn.getFormula(),
                  fn.getFieldName(),
                  errorValue.toString()));
        }

        // IF can return a field on one row and a literal on the next, so the Java class is not
        // stable. Discover it on every row and convert to the output field type. The flag stays
        // on this copy: every copy shares the metadata.
        if (formulaResult != null) {
          discoverReturnType(fn, formulaResult, i);
        }

        int realIndex = (data.replaceIndex[i] < 0) ? tempIndex++ : data.replaceIndex[i];
        outputRowData[realIndex] =
            getReturnValue(
                formulaResult, data.returnType[i], realIndex, data.needDataConversion[i]);
      }

      return outputRowData;
    } catch (HopException e) {
      throw e;
    } catch (Throwable e) {
      throw new HopValueException(e);
    }
  }

  private void discoverReturnType(OpenFormulaMetaFunction fn, Object formulaResult, int i)
      throws HopValueException {
    if (formulaResult instanceof String) {
      data.returnType[i] = OpenFormulaData.RETURN_TYPE_STRING;
      data.needDataConversion[i] = fn.getValueType() != IValueMeta.TYPE_STRING;
    } else if (formulaResult instanceof Integer) {
      data.returnType[i] = OpenFormulaData.RETURN_TYPE_INTEGER;
      data.needDataConversion[i] = fn.getValueType() != IValueMeta.TYPE_INTEGER;
    } else if (formulaResult instanceof Long) {
      data.returnType[i] = OpenFormulaData.RETURN_TYPE_LONG;
      data.needDataConversion[i] = fn.getValueType() != IValueMeta.TYPE_INTEGER;
    } else if (formulaResult instanceof Date) {
      data.returnType[i] = OpenFormulaData.RETURN_TYPE_DATE;
      data.needDataConversion[i] = fn.getValueType() != IValueMeta.TYPE_DATE;
    } else if (formulaResult instanceof BigDecimal) {
      data.returnType[i] = OpenFormulaData.RETURN_TYPE_BIGDECIMAL;
      data.needDataConversion[i] = fn.getValueType() != IValueMeta.TYPE_BIGNUMBER;
    } else if (formulaResult instanceof Number) {
      data.returnType[i] = OpenFormulaData.RETURN_TYPE_NUMBER;
      data.needDataConversion[i] = fn.getValueType() != IValueMeta.TYPE_NUMBER;
    } else if (formulaResult instanceof byte[]) {
      data.returnType[i] = OpenFormulaData.RETURN_TYPE_BYTE_ARRAY;
      if (fn.getValueType() != IValueMeta.TYPE_BINARY) {
        throw new HopValueException(
            BaseMessages.getString(
                PKG, "OpenFormula.Exception.ExpectBinary", fn.getFieldName(), fn.getFormula()));
      }
    } else if (formulaResult instanceof Boolean) {
      data.returnType[i] = OpenFormulaData.RETURN_TYPE_BOOLEAN;
      if (fn.getValueType() != IValueMeta.TYPE_BOOLEAN) {
        throw new HopValueException(
            BaseMessages.getString(
                PKG, "OpenFormula.Exception.ExpectBoolean", fn.getFieldName(), fn.getFormula()));
      }
    } else {
      data.returnType[i] = OpenFormulaData.RETURN_TYPE_STRING;
      data.needDataConversion[i] = fn.getValueType() != IValueMeta.TYPE_STRING;
    }
  }

  protected Object getReturnValue(
      Object formulaResult, int returnType, int realIndex, boolean needDataConversion)
      throws HopException {
    if (formulaResult == null) {
      return null;
    }
    return switch (returnType) {
      case OpenFormulaData.RETURN_TYPE_STRING ->
          needDataConversion
              ? convertDataToTargetValueMeta(realIndex, formulaResult)
              : formulaResult.toString();
      case OpenFormulaData.RETURN_TYPE_NUMBER ->
          needDataConversion
              ? convertDataToTargetValueMeta(realIndex, formulaResult)
              : ((Number) formulaResult).doubleValue();
      case OpenFormulaData.RETURN_TYPE_INTEGER ->
          needDataConversion
              ? convertDataToTargetValueMeta(realIndex, formulaResult)
              : Long.valueOf(((Integer) formulaResult).intValue());
      case OpenFormulaData.RETURN_TYPE_LONG,
              OpenFormulaData.RETURN_TYPE_DATE,
              OpenFormulaData.RETURN_TYPE_BIGDECIMAL,
              OpenFormulaData.RETURN_TYPE_TIMESTAMP ->
          needDataConversion
              ? convertDataToTargetValueMeta(realIndex, formulaResult)
              : formulaResult;
      case OpenFormulaData.RETURN_TYPE_BYTE_ARRAY, OpenFormulaData.RETURN_TYPE_BOOLEAN ->
          formulaResult;
      default -> null;
    };
  }

  private Object convertDataToTargetValueMeta(int i, Object formulaResult) throws HopException {
    if (formulaResult == null) {
      return null;
    }
    IValueMeta target = data.outputRowMeta.getValueMeta(i);
    IValueMeta actual = ValueMetaFactory.guessValueMetaInterface(formulaResult);
    return target.convertData(actual, formulaResult);
  }

  private boolean handleFormulaError(Object[] row, Exception e) throws HopTransformException {
    String message =
        BaseMessages.getString(PKG, "OpenFormula.Exception.CouldNotBeEvaluated", e.getMessage());
    if (getTransformMeta().isDoingErrorHandling()) {
      putError(getInputRowMeta(), row, 1, message, null, "OpenFormula001");
      return true;
    }
    logError(message, e);
    setErrors(1);
    stopAll();
    setOutputDone();
    return false;
  }
}
