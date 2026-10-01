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

import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.stream.IStream;

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
      // Compiling is relatively slow so we do it only for the first row...
      //
      if (data.condition == null) {
        data.condition =
            JavaFilterCondition.compile(data.outputRowMeta, resolve(meta.getCondition()));
      }

      return data.condition.evaluate(data.outputRowMeta, r);
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
