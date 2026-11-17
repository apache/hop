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

package org.apache.hop.pipeline.transforms.uniquerows;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformErrorMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** The fields a pipeline declares for the error hop of Unique rows are the ones it writes. */
class UniqueRowsErrorHopFieldsTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  /** A duplicate goes to the error hop with the count of its set after the input fields. */
  @Test
  void theErrorHopDeclaresTheCountAfterTheInputFields() throws Exception {
    UniqueRowsMeta uniqueRowsMeta = new UniqueRowsMeta();
    uniqueRowsMeta.setCountRows(true);
    uniqueRowsMeta.setCountField("n");
    // Duplicates go to the error hop
    uniqueRowsMeta.setRejectDuplicateRow(true);

    TransformMeta input = new TransformMeta("input", new CodeInputMeta());
    TransformMeta unique = new TransformMeta("unique", uniqueRowsMeta);
    TransformMeta duplicates = new TransformMeta("duplicates", new CodeInputMeta());
    TransformErrorMeta errorMeta = new TransformErrorMeta(unique, duplicates);
    errorMeta.setEnabled(true);
    unique.setTransformErrorMeta(errorMeta);

    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(input);
    pipelineMeta.addTransform(unique);
    pipelineMeta.addTransform(duplicates);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(input, unique));
    pipelineMeta.addPipelineHop(new PipelineHopMeta(unique, duplicates));

    IRowMeta errorRowFields =
        pipelineMeta.getTransformFields(new Variables(), unique, duplicates, null);

    assertEquals(List.of("code", "n"), List.of(errorRowFields.getFieldNames()));
  }

  /** A transform that gives one field, as an input would. */
  public static class CodeInputMeta extends BaseTransformMeta<ITransform, ITransformData> {
    @Override
    public void getFields(
        IRowMeta row,
        String name,
        IRowMeta[] info,
        TransformMeta nextTransform,
        IVariables variables,
        IHopMetadataProvider metadataProvider) {
      row.addValueMeta(new ValueMetaString("code"));
    }
  }
}
