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

package org.apache.hop.pipeline.transforms.drools;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.Arrays;


public class RulesExecutor extends BaseTransform<RulesExecutorMeta, RulesExecutorData> {
  // private static Class<?> PKG = Rules.class; // for i18n purposes

  public RulesExecutor(
      TransformMeta transformMeta,
      RulesExecutorMeta meta,
      RulesExecutorData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  public boolean init() {

    if (super.init()) {
      return true;
    }
    return false;
  }

  public boolean runtimeInit() throws HopTransformException {
    data.setOutputRowMeta(getInputRowMeta().clone());
    meta.getFields(data.getOutputRowMeta(), getTransformName(), null, null, this, null);

    data.setRuleFilePath(meta.getRuleFile());
    data.setRuleString(meta.getRuleDefinition());

    data.initializeRules();
    data.initializeColumns(getInputRowMeta());

    return true;
  }

  public void dispose() {
    super.dispose();
  }

  public boolean processRow() throws HopException {

    Object[] r = getRow(); // get row, set busy!
    if (r == null) { // no more input to be expected...

      data.shutdown();
      setOutputDone();
      return false;
    }

    if (first) {
      if (!runtimeInit()) {
        return false;
      }

      first = false;
    }

    // Load the column objects
    data.loadRow(r);

    data.execute();

    Object[] outputRow;
    int beginOutputRowFill = 0;

    String[] expectedResults = meta.getExpectedResultList();

    if (meta.isKeepInputFields()) {
      int inputRowSize = getInputRowMeta().size();
      outputRow = Arrays.copyOf(r, inputRowSize + expectedResults.length);
      beginOutputRowFill = inputRowSize;
    } else {
      outputRow = new Object[expectedResults.length];
    }

    Rules.Column result = null;
    for (int i = 0; i < expectedResults.length; i++) {
      result = (Rules.Column) data.fetchResult(expectedResults[i]);
      outputRow[i + beginOutputRowFill] = result == null ? null : result.getPayload();
    }

    putRow(data.getOutputRowMeta(), outputRow);

    return true;
  }
}
