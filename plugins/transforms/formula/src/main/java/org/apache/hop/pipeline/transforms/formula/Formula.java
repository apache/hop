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

import static org.apache.hop.pipeline.transforms.formula.util.FormulaFieldsExtractor.getFormulaFieldList;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.formula.fast.FastFormulaCompiler;
import org.apache.hop.pipeline.transforms.formula.fast.FastFormulaCompiler.CompiledFormula;
import org.apache.hop.pipeline.transforms.formula.util.FormulaParser;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.CellValue;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.FormulaError;

public class Formula extends BaseTransform<FormulaMeta, FormulaData> {
  private static final Class<?> PKG = Formula.class; // for i18n purposes

  private FormulaPoi[] poi;
  private List<String>[] formulaFieldLists;
  private CompiledFormula[] fastCompiled;
  private List<String>[] fastFieldLists;
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
        throw new HopRuntimeException(e);
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
              .map(f -> getFormulaFieldList(resolve(f)))
              .toArray(List[]::new);

      // compile each formula for the fast path when it is within the supported subset: the
      // resolved formula goes through the same variable resolution and field replacement as the
      // regular POI path, so both evaluate exactly the same expression.
      //
      int formulaCount = meta.getFormulas().size();
      fastCompiled = new CompiledFormula[formulaCount];
      fastFieldLists = new List[formulaCount];
      for (int i = 0; i < formulaCount; i++) {
        FormulaMetaFunction fn = meta.getFormulas().get(i);
        String resolved = resolve(fn.getFormula());
        String effective = applyReplaceMap(resolved, replaceMap);
        List<String> effectiveFields = getFormulaFieldList(effective);
        fastFieldLists[i] = effectiveFields;
        fastCompiled[i] =
            FastFormulaCompiler.compile(
                effective, effectiveFields, data.outputRowMeta, fn.isSetNa());
      }
    }

    int tempIndex = getInputRowMeta().size();

    if (isRowLevel()) {
      logRowlevel("Read row #" + getLinesRead() + " : " + getInputRowMeta().getString(r));
    }

    Object[] outputRowData = RowDataUtil.resizeArray(r, data.outputRowMeta.size());
    for (int i = 0; i < meta.getFormulas().size(); i++) {
      Object outputValue = null;
      FormulaMetaFunction formula = meta.getFormulas().get(i);
      int outputValueType = formula.getValueType();
      CompiledFormula compiled = fastCompiled[i];
      try {
        if (compiled != null && compiled.fastPath()) {
          // Fast path: no POI workbook or worksheet, just run the compiled plain-Java tree.
          Object[] args = buildFastArguments(fastFieldLists[i], outputRowData, formula.isSetNa());
          Object formulaResult = compiled.function().apply(args);
          outputValue = mapFastResult(formulaResult, outputValueType, i, formula);
        } else {
          FormulaParser parser =
              new FormulaParser(
                  formula,
                  data.outputRowMeta,
                  outputRowData,
                  poi[i],
                  variables,
                  replaceMap,
                  formulaFieldLists[i]);
          CellValue cellValue = parser.getFormulaValue();
          CellType cellType = cellValue.getCellType();

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
            case ERROR:
              outputValue = getErrorValue(cellValue, formula);
              break;
            default:
              break;
          }
        }

        int realIndex = (data.replaceIndex[i] < 0) ? tempIndex++ : data.replaceIndex[i];

        outputRowData[realIndex] =
            getReturnValue(outputValue, data.returnType[i], realIndex, formula);
      } catch (Exception e) {
        // The row can not be calculated: divert it to the error stream if the user asked for it,
        // stop the pipeline otherwise.
        return handleFormulaError(r, formula, e);
      }
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

  /**
   * This is the base transform that forms that basis for all transforms. You can derive from this
   * class to implement your own transforms.
   *
   * @param transformMeta The TransformMeta object to run.
   * @param meta Formula Meta of the transform
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

  /**
   * Excel reports a failed calculation as an error value rather than by throwing. Only {@code #N/A}
   * means "no value available" - the transform produces it on purpose through the "Set Null to
   * #N/A" option - so it maps back to null. Every other error code ({@code #DIV/0!}, {@code
   * #VALUE!}, {@code #NUM!}, ...) is a genuine calculation failure and has to be reported instead
   * of silently turning the field blank.
   *
   * @param cellValue the evaluated cell holding the error
   * @param formula the formula that produced it
   * @return null for {@code #N/A}
   * @throws HopValueException for any other error value
   */
  private Object getErrorValue(CellValue cellValue, FormulaMetaFunction formula)
      throws HopValueException {
    byte errorCode = cellValue.getErrorValue();
    if (FormulaError.isValidCode(errorCode) && FormulaError.forInt(errorCode) == FormulaError.NA) {
      return null;
    }
    String errorText =
        FormulaError.isValidCode(errorCode)
            ? FormulaError.forInt(errorCode).getString()
            : Byte.toString(errorCode);
    throw new HopValueException(
        BaseMessages.getString(
            PKG, "Formula.Exception.FormulaError", formula.getFieldName(), errorText));
  }

  /**
   * A formula could not be calculated for the current row. Send the row to the error stream when
   * error handling is configured, stop the pipeline otherwise.
   *
   * @param row the input row that could not be calculated
   * @param formula the formula that failed
   * @param e the cause of the failure
   * @return true when the row was diverted and processing can continue, false to end this transform
   * @throws HopTransformException when the row could not be written to the error stream
   */
  private boolean handleFormulaError(Object[] row, FormulaMetaFunction formula, Exception e)
      throws HopTransformException {
    String message =
        BaseMessages.getString(
            PKG, "Formula.Exception.CouldNotBeEvaluated", formula.getFormula(), e.getMessage());

    if (getTransformMeta().isDoingErrorHandling()) {
      putError(getInputRowMeta(), row, 1, message, formula.getFieldName(), "Formula001");
      return true;
    }

    logError(message, e);
    setErrors(1);
    stopAll();
    setOutputDone();
    return false;
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
      case FormulaData.RETURN_TYPE_INTEGER,
          FormulaData.RETURN_TYPE_LONG,
          FormulaData.RETURN_TYPE_DATE,
          FormulaData.RETURN_TYPE_BIGDECIMAL,
          FormulaData.RETURN_TYPE_TIMESTAMP:
        if (fn.isNeedDataConversion()) {
          value = convertDataToTargetValueMeta(realIndex, formulaResult);
        } else {
          value = formulaResult;
        }
        break;
      case FormulaData.RETURN_TYPE_BYTE_ARRAY, FormulaData.RETURN_TYPE_BOOLEAN:
        value = formulaResult;
        break;
      default:
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

  /**
   * Applies the "replace field" mapping to a formula, mirroring what {@link FormulaParser} does so
   * the fast path evaluates the exact same expression. Formula fields are substituted by their
   * replacement column, e.g. {@code [aliasAmount]} becomes {@code [realAmount]}.
   *
   * @param formula the variable-resolved formula
   * @param replacements the field to replacement mapping
   * @return the formula with replacements applied
   */
  private static String applyReplaceMap(String formula, HashMap<String, String> replacements) {
    String effective = formula;
    for (String field : getFormulaFieldList(formula)) {
      String replacement = replacements.get(field);
      if (replacement != null) {
        effective = effective.replace("[" + field + "]", "[" + replacement + "]");
      }
    }
    return effective;
  }

  /**
   * Builds the argument array handed to a fast-path function in the order of its field list. A null
   * field bound with the "#N/A" option is passed as the {@link FastFormulaCompiler#NA} marker so
   * the functions can tell a blank cell from an error cell, exactly like the POI path.
   */
  private Object[] buildFastArguments(List<String> fieldList, Object[] sourceRow, boolean setNa) {
    Object[] args = new Object[fieldList.size()];
    for (int i = 0; i < fieldList.size(); i++) {
      int fieldIndex = data.outputRowMeta.indexOfValue(fieldList.get(i));
      Object value = fieldIndex < 0 ? null : sourceRow[fieldIndex];
      args[i] = (value == null && setNa) ? FastFormulaCompiler.NA : value;
    }
    return args;
  }

  /**
   * Maps a plain-Java fast-path result to the output value and {@code returnType} the way the POI
   * {@code CellType} switch would, so both paths produce identical output. Numbers become numeric
   * cells, booleans become boolean cells and strings become string cells; a null result (a blank or
   * {@code #N/A} cell) stays null.
   *
   * <p>Replicating the switch rather than refactoring the POI branch keeps the existing behavior
   * untouched; a null {@code returnType} of {@code -1} makes {@link #getReturnValue} return null.
   */
  private Object mapFastResult(
      Object formulaResult, int outputValueType, int i, FormulaMetaFunction formula) {
    if (formulaResult == null) {
      data.returnType[i] = -1;
      return null;
    }
    if (formulaResult instanceof Number number) {
      double cellNumberValue = number.doubleValue();
      switch (outputValueType) {
        case IValueMeta.TYPE_NUMBER:
          data.returnType[i] = FormulaData.RETURN_TYPE_NUMBER;
          formula.setNeedDataConversion(false);
          return cellNumberValue;
        case IValueMeta.TYPE_INTEGER:
          data.returnType[i] = FormulaData.RETURN_TYPE_INTEGER;
          formula.setNeedDataConversion(true);
          return cellNumberValue;
        case IValueMeta.TYPE_BIGNUMBER:
          data.returnType[i] = FormulaData.RETURN_TYPE_BIGDECIMAL;
          formula.setNeedDataConversion(true);
          return cellNumberValue;
        case IValueMeta.TYPE_DATE:
          data.returnType[i] = FormulaData.RETURN_TYPE_DATE;
          formula.setNeedDataConversion(true);
          return DateUtil.getJavaDate(cellNumberValue);
        case IValueMeta.TYPE_TIMESTAMP:
          data.returnType[i] = FormulaData.RETURN_TYPE_TIMESTAMP;
          formula.setNeedDataConversion(true);
          return Timestamp.from(DateUtil.getJavaDate(cellNumberValue).toInstant());
        default:
          data.returnType[i] = -1;
          return cellNumberValue;
      }
    }
    if (formulaResult instanceof Boolean booleanValue) {
      data.returnType[i] = FormulaData.RETURN_TYPE_BOOLEAN;
      formula.setNeedDataConversion(outputValueType != IValueMeta.TYPE_BOOLEAN);
      return booleanValue;
    }
    if (formulaResult instanceof String stringValue) {
      data.returnType[i] = FormulaData.RETURN_TYPE_STRING;
      formula.setNeedDataConversion(outputValueType != IValueMeta.TYPE_STRING);
      return stringValue;
    }
    data.returnType[i] = -1;
    return null;
  }
}
