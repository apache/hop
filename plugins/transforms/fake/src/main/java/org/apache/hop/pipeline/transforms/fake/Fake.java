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

package org.apache.hop.pipeline.transforms.fake;

import java.util.ArrayList;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

public class Fake extends BaseTransform<FakeMeta, FakeData> {

  public Fake(
      TransformMeta transformMeta,
      FakeMeta meta,
      FakeData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean init() {
    data.faker = FakerCatalog.createFaker(resolve(meta.getLocale()));
    data.boundGenerators = new ArrayList<>();

    // Resolve every configured field once: look up the provider method, pick the right overload
    // for its arguments and convert the (variable-resolved) argument values to typed objects.
    //
    for (FakeField field : meta.getFields()) {
      if (field.isValid()) {
        try {
          data.boundGenerators.add(FakerCatalog.bind(data.faker, field, this));
        } catch (HopException e) {
          logError("Error preparing fake data generator for field '" + field.getName() + "'", e);
          return false;
        }
      }
    }

    return super.init();
  }

  @Override
  public boolean processRow() throws HopException {

    Object[] row = getRow();

    if (row == null) {
      setOutputDone();
      return false;
    }

    if (first) {
      // The output meta is the original input meta + the additional fake data fields.
      first = false;

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);
    }

    Object[] outputRowData = RowDataUtil.resizeArray(row, data.outputRowMeta.size());
    int rowIndex = getInputRowMeta().size();
    for (FakerCatalog.BoundGenerator generator : data.boundGenerators) {
      outputRowData[rowIndex++] = generator.produce();
    }

    putRow(data.outputRowMeta, outputRowData);

    return true;
  }
}
