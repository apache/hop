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

package org.apache.hop.pipeline.transforms.coalesce;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * The Coalesce Transformation selects the first non null value from a group of input fields and
 * passes it down the stream or returns null if all the fields are null.
 *
 * @author Nicolas ADMENT
 * @since 18-mai-2016
 */
public class CoalesceTransform extends BaseTransform<CoalesceMeta, CoalesceData>
    implements ITransform<CoalesceMeta, CoalesceData> {

  private static final Class<?> PKG = CoalesceMeta.class;

  public CoalesceTransform(
      TransformMeta transformMeta,
      CoalesceMeta meta,
      CoalesceData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {

    // get incoming row, getRow() potentially blocks waiting for more rows,
    // returns null if no more rows expected
    Object[] row = getRow();

    // if no more rows are expected, indicate transform is finished and
    // processRow() should not be called again
    if (row == null) {
      setOutputDone();
      return false;
    }

    // the "first" flag is inherited from the base transform implementation
    // it is used to guard some processing tasks, like figuring out field
    // indexes
    // in the row structure that only need to be done once
    if (first) {
      if (log.isDebug()) {
        logDebug(BaseMessages.getString(PKG, "CoalesceTransform.Log.StartedProcessing"));
      }

      first = false;
      // clone the input row structure and place it in our data object
      data.outputRowMeta = getInputRowMeta().clone();    
      
      // use meta.getFields() to change it, so it reflects the output row
      // structure
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

      // Check output name
      for (CoalesceField coalesce : meta.getFields()) {
        String name = this.resolve(coalesce.getName());
        if ( Utils.isEmpty(name) ) {
          throw new HopException(BaseMessages.getString(PKG,"CoalesceTransform.Log.MissingFieldName"));
        }        
      }
            
      checkInputFieldsExist(meta);
    }

    IRowMeta inputRowMeta = getInputRowMeta();

    // Create a new output row
    Object[] outputRowValues = new Object[data.outputRowMeta.size()];

    // Checks if fields from the input stream are present in the output and
    // if so passes down the values
    int outputIndex = 0;
    for (int inputIndex = 0; inputIndex < inputRowMeta.size(); inputIndex++) {
      IValueMeta vm = inputRowMeta.getValueMeta(inputIndex);

      if (data.outputRowMeta.indexOfValue(vm.getName()) == -1) continue;

      outputRowValues[outputIndex++] = row[inputIndex];
    }

    // Calculates the coalesce value for each extra output field and also
    // converts its value to reflect the Value Type option,
    // or in case it was None to reflect on the default data type logic.
    for (CoalesceField coalesce : meta.getFields()) {

      int inputIndex = getFirstNonNullValueIndex(inputRowMeta, row, coalesce.getInputFieldNames(), meta.isTreatEmptyStringsAsNulls());

      // Resolve variable name
      String name = this.resolve(coalesce.getName());
      outputIndex = data.outputRowMeta.indexOfValue(name);
      
      IValueMeta vm = data.outputRowMeta.getValueMeta(outputIndex);
      try {
        Object result = null;
        if (inputIndex >= 0) {
          result = vm.convertData(inputRowMeta.getValueMeta(inputIndex), row[inputIndex]);
        }
        outputRowValues[outputIndex++] = result;
      } catch (HopValueException e) {
        logError(
            BaseMessages.getString(
                PKG,
                "CoalesceTransform.Log.DataIncompatibleError",
                row[inputIndex].toString(),
                inputRowMeta.getValueMeta(inputIndex).toString(),
                vm.toString()));
        throw e;
      }
    }

    // put the row to the output row stream
    putRow(data.outputRowMeta, outputRowValues);

    if (log.isRowLevel()) {
      logRowlevel(
          BaseMessages.getString(
              PKG, "CoalesceTransform.Log.WroteRowToNextTransform", outputRowValues));
    }

    // log progress if it is time to to so
    if (checkFeedback(getLinesRead())) {
      logBasic("Line nr " + getLinesRead());
    }

    // indicate that processRow() should be called again
    return true;
  }

  private void checkInputFieldsExist(final CoalesceMeta meta) throws HopException {
    IRowMeta prev = getInputRowMeta();

    for (CoalesceField coalesce : meta.getFields()) {
      List<String> missingFields = new ArrayList<>();

      for (String field : coalesce.getInputFieldNames()) {

        if (!Utils.isEmpty(field)) {
          IValueMeta vmi = prev.searchValueMeta(field);
          if (vmi == null) {
            missingFields.add(field);
          }
        }
      }
      if (!missingFields.isEmpty()) {
        String errorText =
            BaseMessages.getString(
                PKG,
                "CoalesceTransform.Log.MissingInputFields",
                StringUtils.join(missingFields, ','));
        throw new HopException(errorText);
      }
    }
  }

  /** The actual coalesce logic, returns the index of the first non null value */
  private int getFirstNonNullValueIndex(      
      final IRowMeta inputRowMeta,
      Object[] row,
      List<String> fields, boolean isTreatEmptyStringsAsNulls) {

    for (String fieldName : fields) {

      int index = inputRowMeta.indexOfValue(fieldName);
      if (index >= 0) {
        if (!isTreatEmptyStringsAsNulls && row[index] != null) {
          return index;
        } else if (isTreatEmptyStringsAsNulls
            && row[index] != null
            && !Utils.isEmpty(row[index].toString())) return index;
      }
    }

    // signifies a null value
    return -1;
  }
}
