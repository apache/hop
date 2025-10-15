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

package org.apache.hop.pipeline.transforms.javafilter;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.apache.hop.pipeline.transforms.util.JaninoCheckerUtil;
import org.codehaus.janino.ExpressionEvaluator;

/** Calculate new field values using pre-defined functions. */
public class JavaFilter extends BaseTransform<JavaFilterMeta, JavaFilterData> {

  private static final Class<?> PKG = JavaFilterMeta.class;

  public JavaFilter(
      TransformMeta transformMeta,
      JavaFilterMeta meta,
      JavaFilterData data,
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
      return false;
    }

    if (first) {
      first = false;

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

      // ICache the position of the RowSet for the output.
      //
      if (data.chosesTargetTransforms) {
        List<IStream> targetStreams = meta.getTransformIOMeta().getTargetStreams();

        if (!Utils.isEmpty(targetStreams.get(0).getTransformName())) {
          TransformMeta to = targetStreams.get(0).getTransformMeta();
          PipelineHopMeta hop = getPipelineMeta().findPipelineHop(getTransformMeta(), to);
          if (hop != null && hop.isEnabled()) {
            data.trueRowSet =
                findOutputRowSet(
                    getTransformName(), getCopy(), targetStreams.get(0).getTransformName(), 0);
            if (data.trueRowSet == null) {
              throw new HopException(
                  BaseMessages.getString(
                      PKG,
                      "JavaFilter.Log.TargetTransformInvalid",
                      targetStreams.get(0).getTransformName()));
            }
          }
        } else {
          data.trueRowSet = null;
        }

        if (!Utils.isEmpty(targetStreams.get(1).getTransformName())) {
          TransformMeta to = targetStreams.get(1).getTransformMeta();
          PipelineHopMeta hop = getPipelineMeta().findPipelineHop(getTransformMeta(), to);
          if (hop != null && hop.isEnabled()) {
            data.falseRowSet =
                findOutputRowSet(
                    getTransformName(), getCopy(), targetStreams.get(1).getTransformName(), 0);
            if (data.falseRowSet == null) {
              throw new HopException(
                  BaseMessages.getString(
                      PKG,
                      "JavaFilter.Log.TargetTransformInvalid",
                      targetStreams.get(1).getTransformName()));
            }
          }
        } else {
          data.falseRowSet = null;
        }
      }
    }

    if (isRowLevel()) {
      logRowlevel("Read row #" + getLinesRead() + " : " + getInputRowMeta().getString(r));
    }

    boolean keep = calcFields(r);

    if (!data.chosesTargetTransforms) {
      if (keep) {
        putRow(data.outputRowMeta, r); // copy row to output rowset(s)
      }
    } else {
      if (keep) {
        if (data.trueRowSet != null) {
          if (isRowLevel()) {
            logRowlevel(
                "Sending row to true  :"
                    + data.trueTransformName
                    + " : "
                    + getInputRowMeta().getString(r));
          }
          putRowTo(data.outputRowMeta, r, data.trueRowSet);
        }
      } else {
        if (data.falseRowSet != null) {
          if (isRowLevel()) {
            logRowlevel(
                "Sending row to false :"
                    + data.falseTransformName
                    + " : "
                    + getInputRowMeta().getString(r));
          }
          putRowTo(data.outputRowMeta, r, data.falseRowSet);
        }
      }
    }

    if (checkFeedback(getLinesRead()) && isBasic()) {
      logBasic(BaseMessages.getString(PKG, "JavaFilter.Log.LineNumber") + getLinesRead());
    }

    return true;
  }

  private boolean calcFields(Object[] r) throws HopValueException {
    try {
      // Initialize evaluators etc. Only do it once.
      //
      if (data.expressionEvaluator == null) {
        String realCondition = resolve(meta.getCondition());
        data.argumentIndexes = new ArrayList<>();

        List<String> parameterNames = new ArrayList<>();
        List<Class<?>> parameterTypes = new ArrayList<>();

        for (int i = 0; i < data.outputRowMeta.size(); i++) {

          IValueMeta valueMeta = data.outputRowMeta.getValueMeta(i);

          // See if the value is being used in a formula...
          //
          if (realCondition.contains(valueMeta.getName())) {
            // If so, add it to the indexes...
            data.argumentIndexes.add(i);

            Class<?> parameterType =
                switch (valueMeta.getType()) {
                  case IValueMeta.TYPE_STRING -> String.class;
                  case IValueMeta.TYPE_NUMBER -> Double.class;
                  case IValueMeta.TYPE_INTEGER -> Long.class;
                  case IValueMeta.TYPE_DATE -> Date.class;
                  case IValueMeta.TYPE_BIGNUMBER -> BigDecimal.class;
                  case IValueMeta.TYPE_BOOLEAN -> Boolean.class;
                  case IValueMeta.TYPE_BINARY -> byte[].class;
                  default -> String.class;
                };
            parameterTypes.add(parameterType);
            parameterNames.add(valueMeta.getName());
          }
        }

        // Create the expression evaluator: is relatively slow so we do it only for the first row...
        //
        data.expressionEvaluator = new ExpressionEvaluator();
        data.expressionEvaluator.setParameters(
            parameterNames.toArray(new String[0]), parameterTypes.toArray(new Class<?>[0]));
        data.expressionEvaluator.setReturnType(Object.class);
        data.expressionEvaluator.setThrownExceptions(new Class<?>[] {Exception.class});

        // Validate Formula
        JaninoCheckerUtil janinoCheckerUtil = new JaninoCheckerUtil();
        List<String> codeCheck = janinoCheckerUtil.checkCode(realCondition);
        if (!codeCheck.isEmpty()) {
          throw new HopException("Script contains code that is not allowed : " + codeCheck);
        }

        data.expressionEvaluator.cook(realCondition);

        // Also create the argument data structure once...
        //
        data.argumentData = new Object[data.argumentIndexes.size()];
      }

      // This method can only accept the specified number of values...
      //
      for (int x = 0; x < data.argumentIndexes.size(); x++) {
        int index = data.argumentIndexes.get(x);
        IValueMeta outputValueMeta = data.outputRowMeta.getValueMeta(index);
        data.argumentData[x] = outputValueMeta.convertToNormalStorageType(r[index]);
      }

      Object formulaResult = data.expressionEvaluator.evaluate(data.argumentData);

      if (formulaResult instanceof Boolean bool) {
        return bool;
      } else {
        throw new HopException(
            "The result of the filter expression must be a boolean and we got back : "
                + formulaResult.getClass().getName());
      }
    } catch (Exception e) {
      throw new HopValueException(e);
    }
  }

  @Override
  public boolean init() {

    if (super.init()) {
      List<IStream> targetStreams = meta.getTransformIOMeta().getTargetStreams();
      data.trueTransformName = targetStreams.get(0).getTransformName();
      data.falseTransformName = targetStreams.get(1).getTransformName();
      data.chosesTargetTransforms =
          targetStreams.get(0).getTransformMeta() != null
              || targetStreams.get(1).getTransformMeta() != null;

      return true;
    }
    return false;
  }
}
