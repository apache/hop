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

package org.apache.hop.pipeline.transforms.schemamapping;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Sample rows. Filter rows based on line number */
public class SchemaMapping extends BaseTransform<SchemaMappingMeta, SchemaMappingData> {

  private static final Class<?> PKG = SchemaMapping.class; // For Translator
  public SchemaMapping(
      TransformMeta transformMeta,
      SchemaMappingMeta meta,
      SchemaMappingData data,
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
      data.inputRowMeta = getInputRowMeta();
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, variables, metadataProvider );

      data.fieldnrs = new int[meta.getMappingFieldset().size()];
      for (int i = 0; i < data.fieldnrs.length; i++) {
        SchemaMappingField f = meta.getMappingFieldset().get(i);
        data.fieldnrs[i] = data.inputRowMeta.indexOfValue(f.getFieldStream());
        if (data.fieldnrs[i] < 0) {
          logError(
                  BaseMessages.getString(
                          PKG, "SchemaMapping.Log.CouldNotFindField", f.getFieldStream()));
          setErrors(1);
          stopAll();
          return false;
        }
      }

    } // end if first

    // Create a new output row
    Object[] outputData = new Object[data.fieldnrs.length];
    int outputIndex = 0;

    // Get the field values
    //
    for (int idx : data.fieldnrs) {
      // Normally this can't happen, except when streams are mixed with different
      // number of fields.
      //
      if (idx < data.inputRowMeta.size()) {
        IValueMeta valueMeta = data.inputRowMeta.getValueMeta(idx);

        // TODO: Clone might be a 'bit' expensive as it is only needed in case you want to copy a
        // single field to 2 or
        // more target fields.
        // And even then it is only required for the last n-1 target fields.
        // Perhaps we can consider the requirements for cloning at init(), store it in a boolean[]
        // and just consider
        // this at runtime
        //
        outputData[outputIndex++] = valueMeta.cloneValueData(r[idx]);
      } else {
        if (log.isDetailed()) {
          logDetailed(
                  BaseMessages.getString(PKG, "SchemaMapping.Log.MixingStreamWithDifferentFields"));
        }
      }
    }

    putRow(data.outputRowMeta, outputData);
    if (log.isRowLevel()) {
      logRowlevel(
              BaseMessages.getString(PKG, "SelectValues.Log.WroteRowToNextTransform")
                      + data.outputRowMeta.getString(outputData));
    }

    // Allowed to continue to read in data
    return true;
  }

  @Override
  public boolean init() {

    if (super.init()) {
      // Add init code here.
      return true;
    }
    return false;
  }
}
