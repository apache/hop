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

package org.apache.hop.beam.pipeline.handler;

import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.fn.AddSequenceFn;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.pipeline.IBeamPipelineTransformHandler;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.JsonRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.addsequence.AddSequenceMeta;

/**
 * Issue #2379: a Beam handler for the Add Sequence transform.
 *
 * <p>A bounded counter is one {@code GroupByKey} and one stateful {@code DoFn}, so every worker
 * shares a single sequence. Database mode and unbounded input cannot use that {@code GroupByKey}.
 * They go through {@link BeamGenericTransformHandler}, which is what ran this transform before.
 */
public class BeamAddSequenceTransformHandler extends BeamBaseTransformHandler
    implements IBeamPipelineTransformHandler {

  @Override
  public boolean isInput() {
    return false;
  }

  @Override
  public boolean isOutput() {
    return false;
  }

  @Override
  public void handleTransform(
      ILogChannel log,
      IVariables variables,
      String runConfigurationName,
      IBeamPipelineEngineRunConfiguration runConfiguration,
      String dataSamplersJson,
      IHopMetadataProvider metadataProvider,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      Map<String, PCollection<HopRow>> transformCollectionMap,
      Pipeline pipeline,
      IRowMeta rowMeta,
      List<TransformMeta> previousTransforms,
      PCollection<HopRow> input,
      String parentLogChannelId)
      throws HopException {

    if (input == null) {
      throw new HopException(
          "The Add Sequence transform '"
              + transformMeta.getName()
              + "' needs an input: it numbers the rows which arrive from a previous transform.");
    }

    // Don't simply cast but serialize/de-serialize the metadata to prevent classloader exceptions
    //
    AddSequenceMeta meta = new AddSequenceMeta();
    loadTransformMetadata(meta, transformMeta, metadataProvider, pipelineMeta);

    // GroupByKey cannot run on an unbounded global window, a database sequence is the Hop
    // transform's own connection, and a configuration transform is an info stream that only the
    // generic handler passes on. All three keep the generic handler.
    //
    if (meta.isDatabaseUsed()
        || meta.isConfigurationFromTransform()
        || input.isBounded() == PCollection.IsBounded.UNBOUNDED) {
      new BeamGenericTransformHandler()
          .handleTransform(
              log,
              variables,
              runConfigurationName,
              runConfiguration,
              dataSamplersJson,
              metadataProvider,
              pipelineMeta,
              transformMeta,
              transformCollectionMap,
              pipeline,
              rowMeta,
              previousTransforms,
              input,
              parentLogChannelId);
      return;
    }

    long startAt = Const.toLong(variables.resolve(meta.getStartAt()), 1L);
    long incrementBy = Const.toLong(variables.resolve(meta.getIncrementBy()), 1L);
    if (incrementBy <= 0) {
      throw new HopException(
          "The Add Sequence transform '"
              + transformMeta.getName()
              + "' needs a positive increment, not '"
              + meta.getIncrementBy()
              + "'");
    }
    long maxValue = Const.toLong(variables.resolve(meta.getMaxValue()), -1L);

    String valueName = variables.resolve(meta.getValueName());
    if (valueName == null || valueName.isEmpty()) {
      throw new HopException(
          "The Add Sequence transform '" + transformMeta.getName() + "' has no value name");
    }

    // The row layout: the incoming fields plus the sequence field.  The converter has already
    // called AddSequenceMeta.getFields(), so rowMeta already carries the sequence field; adding
    // another one here would emit a duplicate column and shift the value out of the last slot.
    //
    IRowMeta outputRowMeta = rowMeta == null ? new RowMeta() : rowMeta.clone();
    if (outputRowMeta.searchValueMeta(valueName) == null) {
      IValueMeta valueMeta = ValueMetaFactory.createValueMeta(valueName, IValueMeta.TYPE_INTEGER);
      valueMeta.setOrigin(transformMeta.getName());
      outputRowMeta.addValueMeta(valueMeta);
    }

    // Beam state is per key, so funnel every row onto one key: that is what makes a single
    // counter produce a single, gap-free, increasing sequence.
    //
    PCollection<HopRow> grouped =
        input
            .apply(WithKeys.of((Void) null))
            .setCoder(KvCoder.of(VoidCoder.of(), input.getCoder()))
            .apply(GroupByKey.create())
            .apply(Values.create())
            .apply(Flatten.iterables());

    // A stateful DoFn needs a KvCoder input, and Flatten leaves a PCollection<Iterable<HopRow>>.
    // Key every row again on the same null key so the whole dataset is one key, which is what
    // gives a single counter cell.
    PCollection<KV<Void, HopRow>> keyedRows =
        grouped
            .apply(WithKeys.of((Void) null))
            .setCoder(KvCoder.of(VoidCoder.of(), grouped.getCoder()));

    PCollection<HopRow> afterSequence =
        keyedRows.apply(
            ParDo.of(
                new AddSequenceFn(
                    transformMeta.getName(),
                    JsonRowMeta.toJson(outputRowMeta),
                    valueName,
                    startAt,
                    incrementBy,
                    maxValue)));

    transformCollectionMap.put(transformMeta.getName(), afterSequence);
    log.logBasic(
        "Handled transform (ADD SEQUENCE) : "
            + transformMeta.getName()
            + ", gets data from "
            + previousTransforms.size()
            + " previous transform(s)");
  }
}
