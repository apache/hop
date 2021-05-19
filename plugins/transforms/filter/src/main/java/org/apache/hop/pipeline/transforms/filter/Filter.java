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

package org.apache.hop.pipeline.transforms.filter;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.sql.SqlCondition;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Filters input rows base on conditions. */
public class Filter extends BaseTransform<FilterMeta, FilterData>
    implements ITransform<FilterMeta, FilterData> {

  private static final Class<?> PKG = FilterMeta.class; // For Translator

  public Filter(
      TransformMeta transformMeta,
      FilterMeta meta,
      FilterData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  private synchronized boolean keepRow(IRowMeta rowMeta, Object[] row) throws HopException {
    try {
      return data.condition.evaluate(rowMeta, row);
    } catch (Exception e) {
      String message =
          BaseMessages.getString(PKG, "Filter.Exception.UnexpectedErrorFoundInEvaluationFuction");
      logError(message);
      logError(
          BaseMessages.getString(PKG, "Filter.Log.ErrorOccurredForRow") + rowMeta.getString(row));
      logError(Const.getStackTracker(e));
      throw new HopException(message, e);
    }
  }

  public boolean processRow() throws HopException {

    boolean keep;

    Object[] r = getRow(); // Get next usable row from input rowset(s)!
    if (r == null) { // no more input to be expected...

      setOutputDone();
      return false;
    }

    if (first) {
      first = false;

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(getInputRowMeta(), getTransformName(), null, null, this, metadataProvider);

      String condition = resolve(meta.getCondition());
      data.sqlCondition = new SqlCondition("", condition, getInputRowMeta());
      data.condition = data.sqlCondition.getCondition();

      // Cache the position of the IRowSet for the output.
      //
      if (data.chosesTargetTransforms) {
        if (StringUtils.isNotEmpty(data.trueTransformName)) {
          data.trueRowSet =
              findOutputRowSet(getTransformName(), getCopy(), data.trueTransformName, 0);
          if (data.trueRowSet == null) {
            throw new HopException(
                BaseMessages.getString(
                    PKG, "Filter.Log.TargetTransformInvalid", data.trueTransformName));
          }
        } else {
          data.trueRowSet = null;
        }

        if (StringUtils.isNotEmpty(data.falseTransformName)) {
          data.falseRowSet =
              findOutputRowSet(getTransformName(), getCopy(), data.falseTransformName, 0);
          if (data.falseRowSet == null) {
            throw new HopException(
                BaseMessages.getString(
                    PKG, "Filter.Log.TargetTransformInvalid", data.falseTransformName));
          }
        } else {
          data.falseRowSet = null;
        }
      }
    }

    keep = keepRow(getInputRowMeta(), r); // Keep this row?
    if (!data.chosesTargetTransforms) {
      if (keep) {
        putRow(data.outputRowMeta, r); // copy row to output rowset(s);
      }
    } else {
      if (keep) {
        if (data.trueRowSet != null) {
          if (log.isRowLevel()) {
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
          if (log.isRowLevel()) {
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

    if (checkFeedback(getLinesRead())) {
      if (log.isBasic()) {
        logBasic(BaseMessages.getString(PKG, "Filter.Log.LineNumber") + getLinesRead());
      }
    }

    return true;
  }

  @Override
  public boolean init() {

    if (super.init()) {

      data.trueTransformName = resolve(meta.getTrueTransformName());
      data.falseTransformName = resolve(meta.getFalseTransformName());
      data.chosesTargetTransforms =
          StringUtils.isNotEmpty(data.trueTransformName)
              || StringUtils.isNotEmpty(data.falseTransformName);
      return true;
    }
    return false;
  }
}
