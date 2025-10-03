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

package org.apache.hop.pipeline.transforms.calculator;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileNotFoundException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Calculate new field values using pre-defined functions. */
@SuppressWarnings("java:S1104")
public class Calculator extends BaseTransform<CalculatorMeta, CalculatorData> {

  private static final Class<?> PKG = CalculatorMeta.class;
  public static final String CONST_FOR_CALCULATION = " for calculation #";

  public class FieldIndexes {
    public int indexName;
    public int indexA;
    public int indexB;
    public int indexC;
  }

  public Calculator(
      TransformMeta transformMeta,
      CalculatorMeta meta,
      CalculatorData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {

    Object[] r = getRow(); // get row, set busy!
    if (r == null) { // no more input to be expected...
      setOutputDone();
      data.clearValuesMetaMapping();
      return false;
    }

    if (first) {
      first = false;
      data.setOutputRowMeta(getInputRowMeta().clone());
      meta.getFields(
          data.getOutputRowMeta(), getTransformName(), null, null, this, metadataProvider);

      // get all metadata, including source rows and temporary fields.
      data.setCalcRowMeta(meta.getAllFields(getInputRowMeta()));

      data.setFieldIndexes(new FieldIndexes[meta.getFunctions().size()]);
      List<Integer> tempIndexes = new ArrayList<>();

      // Calculate the indexes of the values and arguments in the target data or temporary data
      // We do this in advance to save time later on.
      //
      for (int i = 0; i < meta.getFunctions().size(); i++) {
        CalculatorMetaFunction function = meta.getFunctions().get(i);
        data.getFieldIndexes()[i] = new FieldIndexes();

        if (!Utils.isEmpty(function.getFieldName())) {
          data.getFieldIndexes()[i].indexName =
              data.getCalcRowMeta().indexOfValue(function.getFieldName());
          if (data.getFieldIndexes()[i].indexName < 0) {
            // Nope: throw an exception
            throw new HopTransformException(
                BaseMessages.getString(
                    PKG,
                    "Calculator.Error.UnableFindField",
                    function.getFieldName(),
                    "" + (i + 1)));
          }
        } else {
          throw new HopTransformException(
              BaseMessages.getString(PKG, "Calculator.Error.NoNameField", "" + (i + 1)));
        }

        if (!Utils.isEmpty(function.getFieldA())) {
          if (function.getCalcType() != CalculationType.CONSTANT) {
            data.getFieldIndexes()[i].indexA =
                data.getCalcRowMeta().indexOfValue(function.getFieldA());
            if (data.getFieldIndexes()[i].indexA < 0) {
              // Nope: throw an exception
              throw new HopTransformException(
                  "Unable to find the first argument field '"
                      + function.getFieldName()
                      + CONST_FOR_CALCULATION
                      + (i + 1));
            }
          } else {
            data.getFieldIndexes()[i].indexA = -1;
          }
        } else {
          throw new HopTransformException(
              "There is no first argument specified for calculated field #" + (i + 1));
        }

        if (!Utils.isEmpty(function.getFieldB())) {
          data.getFieldIndexes()[i].indexB =
              data.getCalcRowMeta().indexOfValue(function.getFieldB());
          if (data.getFieldIndexes()[i].indexB < 0) {
            // Nope: throw an exception
            throw new HopTransformException(
                "Unable to find the second argument field '"
                    + function.getFieldName()
                    + CONST_FOR_CALCULATION
                    + (i + 1));
          }
        }
        data.getFieldIndexes()[i].indexC = -1;
        if (!Utils.isEmpty(function.getFieldC())) {
          data.getFieldIndexes()[i].indexC =
              data.getCalcRowMeta().indexOfValue(function.getFieldC());
          if (data.getFieldIndexes()[i].indexC < 0) {
            // Nope: throw an exception
            throw new HopTransformException(
                "Unable to find the third argument field '"
                    + function.getFieldName()
                    + CONST_FOR_CALCULATION
                    + (i + 1));
          }
        }

        if (function.isRemovedFromResult()) {
          tempIndexes.add(getInputRowMeta().size() + i);
        }
      }

      // Convert temp indexes to int[]
      data.setTempIndexes(new int[tempIndexes.size()]);
      for (int i = 0; i < data.getTempIndexes().length; i++) {
        data.getTempIndexes()[i] = tempIndexes.get(i);
      }
    }

    if (isRowLevel()) {
      logRowlevel(
          BaseMessages.getString(PKG, "Calculator.Log.ReadRow")
              + getLinesRead()
              + " : "
              + getInputRowMeta().getString(r));
    }

    try {
      Object[] row = calcFields(getInputRowMeta(), r);
      putRow(data.getOutputRowMeta(), row); // copy row to possible alternate rowset(s).

      if (isRowLevel()) {
        logRowlevel("Wrote row #" + getLinesWritten() + " : " + getInputRowMeta().getString(r));
      }
      if (checkFeedback(getLinesRead()) && isBasic()) {
        logBasic(BaseMessages.getString(PKG, "Calculator.Log.Linenr", "" + getLinesRead()));
      }
    } catch (HopFileNotFoundException e) {
      if (meta.isFailIfNoFile()) {
        logError(BaseMessages.getString(PKG, "Calculator.Log.NoFile") + " : " + e.getFilepath());
        setErrors(getErrors() + 1);
        return false;
      }
    } catch (HopException e) {
      logError(
          BaseMessages.getString(
              PKG, "Calculator.ErrorInTransformRunning" + " : " + e.getMessage()));
      throw new HopTransformException(
          BaseMessages.getString(PKG, "Calculator.ErrorInTransformRunning"), e);
    }
    return true;
  }

  /**
   * @param inputRowMeta the input row metadata
   * @param r the input row (data)
   * @return A row including the calculations, excluding the temporary values
   * @throws HopValueException in case there is a calculation error.
   */
  private Object[] calcFields(IRowMeta inputRowMeta, Object[] r)
      throws HopValueException, HopFileNotFoundException {
    // First copy the input data to the new result...
    Object[] calcData = RowDataUtil.resizeArray(r, data.getCalcRowMeta().size());

    for (int i = 0, index = inputRowMeta.size() + i; i < meta.getFunctions().size(); i++, index++) {
      CalculatorMetaFunction fn = meta.getFunctions().get(i);
      if (!Utils.isEmpty(fn.getFieldName())) {
        IValueMeta targetMeta = data.getCalcRowMeta().getValueMeta(index);

        IValueMeta metaA = null;
        Object dataA = null;

        if (data.getFieldIndexes()[i].indexA >= 0) {
          metaA = data.getCalcRowMeta().getValueMeta(data.getFieldIndexes()[i].indexA);
          dataA = calcData[data.getFieldIndexes()[i].indexA];
        }

        IValueMeta metaB = null;
        Object dataB = null;

        if (data.getFieldIndexes()[i].indexB >= 0) {
          metaB = data.getCalcRowMeta().getValueMeta(data.getFieldIndexes()[i].indexB);
          dataB = calcData[data.getFieldIndexes()[i].indexB];
        }

        IValueMeta metaC = null;
        Object dataC = null;

        if (data.getFieldIndexes()[i].indexC >= 0) {
          metaC = data.getCalcRowMeta().getValueMeta(data.getFieldIndexes()[i].indexC);
          dataC = calcData[data.getFieldIndexes()[i].indexC];
        }

        CalculationType calcType = fn.getCalcType();

        CalculationInput calcIn =
            new CalculationInput(
                metaA,
                metaB,
                metaC,
                dataA,
                dataB,
                dataC,
                fn,
                targetMeta,
                this,
                meta.isFailIfNoFile());

        CalculationOutput calcOut = calcType.calculation.calculate(calcIn);
        calcData[index] = calcOut.value;
        int resultType = calcOut.resultType;

        // If we don't have a target data type, throw an error.
        // Otherwise the result is non-deterministic.
        //
        if (targetMeta.getType() == IValueMeta.TYPE_NONE) {
          throw new HopValueException(
              BaseMessages.getString(PKG, "Calculator.Log.NoType")
                  + (i + 1)
                  + " : "
                  + fn.getFieldName()
                  + " = "
                  + fn.getCalcType().getCode()
                  + " / "
                  + fn.getCalcType().getDescription());
        }

        // Convert the data to the correct target data type.
        //
        if (calcData[index] != null && targetMeta.getType() != resultType) {
          IValueMeta resultMeta;
          try {
            // clone() is not necessary as one data instance belongs to one transform instance and
            // no race condition occurs
            resultMeta = data.getValueMetaFor(resultType, "result");
          } catch (Exception exception) {
            throw new HopValueException("Error creating value");
          }
          resultMeta.setConversionMask(fn.getConversionMask());
          resultMeta.setGroupingSymbol(fn.getGroupingSymbol());
          resultMeta.setDecimalSymbol(fn.getDecimalSymbol());
          resultMeta.setCurrencySymbol(fn.getCurrencySymbol());
          try {
            calcData[index] = targetMeta.convertData(resultMeta, calcData[index]);
          } catch (Exception ex) {
            throw new HopValueException(
                "resultType: " + resultType + "; targetMeta: " + targetMeta.getType(), ex);
          }
        }
      }
    }

    // OK, now we should refrain from adding the temporary fields to the result.
    // So we remove them.
    //
    return RowDataUtil.removeItems(calcData, data.getTempIndexes());
  }
}
