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

package org.apache.hop.lineage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.lineage.hub.LineageConfiguration;
import org.apache.hop.lineage.model.LineageFieldSchema;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaDataCombi;
import org.junit.jupiter.api.Test;

class LineageTransformSchemaEmitterTest {

  @Test
  void fieldsFromRowMeta_mapsNamesAndTypes() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("a"));
    rowMeta.addValueMeta(new ValueMetaInteger("count"));

    List<LineageFieldSchema> fields = LineageTransformSchemaEmitter.fieldsFromRowMeta(rowMeta);

    assertEquals(2, fields.size());
    assertEquals("a", fields.get(0).getName());
    assertEquals(
        ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_STRING), fields.get(0).getHopTypeName());
    assertEquals("count", fields.get(1).getName());
    assertEquals(
        ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_INTEGER), fields.get(1).getHopTypeName());
  }

  @Test
  void fieldsFromRowMeta_nullOrEmpty() {
    assertEquals(0, LineageTransformSchemaEmitter.fieldsFromRowMeta(null).size());
    assertEquals(0, LineageTransformSchemaEmitter.fieldsFromRowMeta(new RowMeta()).size());
  }

  /**
   * Issue #9036: the last transform of a pipeline has no output row sets, so the emitter fell back
   * to the pipeline graph and asked the transforms for their fields. A Table Input on SQL Server
   * runs its SQL to answer that, so its inserts ran twice. With lineage off nothing is emitted, so
   * nothing may be asked either.
   */
  @Test
  @SuppressWarnings("unchecked")
  void disabledLineageDoesNotAskTransformsForTheirFields() throws Exception {
    String previous = System.clearProperty(LineageVariables.HOP_LINEAGE_ENABLED);
    LineageConfiguration.invalidate();
    try {
      PipelineMeta pipelineMeta = mock(PipelineMeta.class);
      IPipelineEngine<PipelineMeta> pipeline = mock(IPipelineEngine.class);
      when(pipeline.getPipelineMeta()).thenReturn(pipelineMeta);
      ITransform transform = mock(ITransform.class);
      when(transform.getPipeline()).thenReturn(pipeline);
      when(transform.getLogChannel()).thenReturn(mock(ILogChannel.class));

      TransformMetaDataCombi combi = new TransformMetaDataCombi();
      combi.transformName = "Output";
      combi.transform = transform;
      combi.transformMeta = new TransformMeta();

      LineageTransformSchemaEmitter.emitTransformBoundarySchemas(combi);

      verify(pipelineMeta, never()).getTransformFields(any(), any(TransformMeta.class));
      verify(pipelineMeta, never()).getPrevTransformFields(any(), any(TransformMeta.class));
    } finally {
      if (previous != null) {
        System.setProperty(LineageVariables.HOP_LINEAGE_ENABLED, previous);
      }
      LineageConfiguration.invalidate();
    }
  }
}
