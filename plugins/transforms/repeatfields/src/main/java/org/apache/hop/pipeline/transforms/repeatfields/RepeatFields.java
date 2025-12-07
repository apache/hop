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
 *
 */

package org.apache.hop.pipeline.transforms.repeatfields;

import com.google.common.primitives.Ints;
import java.util.ArrayList;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

public class RepeatFields extends BaseTransform<RepeatFieldsMeta, RepeatFieldsData> {
  public RepeatFields(
      TransformMeta transformMeta,
      RepeatFieldsMeta meta,
      RepeatFieldsData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
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

      // The output row metadata
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

      data.groupIndexes = new ArrayList<>();
      for (String groupField : meta.getGroupFields()) {
        int index = getInputRowMeta().indexOfValue(groupField);
        if (index < 0) {
          throw new HopException("Unable to find group field: " + groupField);
        }
        data.groupIndexes.add(index);
      }
      data.sourceIndexes = new ArrayList<>();
      data.indicatorIndexes = new ArrayList<>();
      for (Repeat repeat : meta.getRepeats()) {
        String sourceFieldName = resolve(repeat.getSourceField());
        int index = getInputRowMeta().indexOfValue(sourceFieldName);
        if (index < 0) {
          throw new HopException("Unable to find source field to repeat: " + sourceFieldName);
        }
        data.sourceIndexes.add(index);

        int indicatorIndex = -1;
        String indicatorFieldName = resolve(repeat.getIndicatorFieldName());
        if (StringUtils.isNotEmpty(indicatorFieldName)) {
          indicatorIndex = getInputRowMeta().indexOfValue(indicatorFieldName);
          if (indicatorIndex < 0) {
            throw new HopException("Unable to find indicator field: " + indicatorFieldName);
          }
        }
        data.indicatorIndexes.add(indicatorIndex);
      }
    }

    // Compare the current row with the previous one.  If there's a change, start a new group
    //
    if (isNewGroup(r, data.previousRow)) {
      startNewGroup();
    }

    Object[] outputRow = RowDataUtil.resizeArray(r, data.outputRowMeta.size());
    int targetIndex = getInputRowMeta().size();
    for (int i = 0; i < meta.getRepeats().size(); i++) {
      Repeat repeat = meta.getRepeats().get(i);
      int sourceIndex = data.sourceIndexes.get(i);
      IValueMeta sourceValueMeta = getInputRowMeta().getValueMeta(sourceIndex);
      Object sourceValue = r[sourceIndex];
      Object targetValue = sourceValue;
      // What do we need to do?
      switch (repeat.getType()) {
        case Previous -> targetValue = getPreviousValue(sourceValue, targetIndex);
        case PreviousWhenNull -> {
          // If the source value is null, take the previous value.
          if (sourceValueMeta.isNull(sourceValue)) {
            targetValue = getPreviousValue(null, targetIndex);
          }
        }
        case CurrentWhenIndicated ->
            targetValue = getCurrentValueWhenIndicated(i, repeat, r, sourceValue, targetIndex);
      }
      outputRow[targetIndex] = targetValue;
      targetIndex++;
    }

    // Make a copy of the current row to prevent next transforms from modifying it.
    //
    data.previousRow = data.outputRowMeta.cloneRow(outputRow);

    // Send the output to the next transforms.
    //
    putRow(data.outputRowMeta, outputRow);

    return true;
  }

  private Object getCurrentValueWhenIndicated(
      int i, Repeat repeat, Object[] r, Object sourceValue, int targetIndex) throws HopException {
    Object targetValue;
    int indicatorIndex = data.indicatorIndexes.get(i);
    if (indicatorIndex < 0) {
      throw new HopException("Unable to find indicator field: " + repeat.getIndicatorFieldName());
    }
    String indicator = getInputRowMeta().getString(r, indicatorIndex);
    if (StringUtils.isEmpty(indicator)) {
      throw new HopException(
          "No indicator value was found in field " + repeat.getIndicatorFieldName());
    }
    if (indicator.equals(repeat.getIndicatorValue())) {
      targetValue = sourceValue;
    } else {
      if (data.previousRow == null) {
        // The first row in a data set is often the last version of an SCD2 dimension.
        // We'll simply keep this value.
        targetValue = sourceValue;
      } else {
        // We take the target value from the previous row
        targetValue = data.previousRow[targetIndex];
      }
    }
    return targetValue;
  }

  /**
   * Take the value from the previous row target field (if there is one). The default value (if
   * there's no previous row) is the source value of the first row.
   *
   * @param sourceValue The source value (default when no previous row)
   * @param targetIndex The target field index to read from
   * @return The value for the previous row (or the given current source value)
   */
  private Object getPreviousValue(Object sourceValue, int targetIndex) {
    Object value = sourceValue;
    if (data.previousRow != null) {
      value = data.previousRow[targetIndex];
    }
    return value;
  }

  private void startNewGroup() {
    // There is no previous row
    data.previousRow = null;
  }

  private boolean isNewGroup(Object[] currentRow, Object[] previousRow) throws HopValueException {
    if (previousRow == null) {
      return true;
    }
    int compare =
        getInputRowMeta().compare(currentRow, previousRow, Ints.toArray(data.groupIndexes));
    return compare != 0;
  }
}
