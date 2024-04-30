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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.joinlibrary.Join;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.fn.HopKeyValueFn;
import org.apache.hop.beam.core.fn.MergeJoinAssemblerFn;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.pipeline.IBeamPipelineTransformHandler;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.JsonRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.mergejoin.MergeJoinMeta;

public class BeamMergeJoinTransformHandler extends BeamBaseTransformHandler
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

    // Don't simply case but serialize/de-serialize the metadata to prevent classloader exceptions
    //
    MergeJoinMeta meta = new MergeJoinMeta();
    loadTransformMetadata(meta, transformMeta, metadataProvider, pipelineMeta);

    String joinType = meta.getJoinType();
    String[] leftKeys = meta.getKeyFields1().toArray(new String[0]);
    String[] rightKeys = meta.getKeyFields2().toArray(new String[0]);

    TransformMeta leftInfoTransform =
        meta.getTransformIOMeta().getInfoStreams().get(0).getTransformMeta();
    if (leftInfoTransform == null) {
      throw new HopException(
          "The left source transform isn't defined in the Merge Join transform called '"
              + transformMeta.getName()
              + "'");
    }
    PCollection<HopRow> leftPCollection = transformCollectionMap.get(leftInfoTransform.getName());
    if (leftPCollection == null) {
      throw new HopException(
          "The left source collection in the pipeline couldn't be found (probably a programming error)");
    }
    IRowMeta leftRowMeta = pipelineMeta.getTransformFields(variables, leftInfoTransform);

    TransformMeta rightInfoTransform =
        meta.getTransformIOMeta().getInfoStreams().get(1).getTransformMeta();
    if (rightInfoTransform == null) {
      throw new HopException(
          "The right source transform isn't defined in the Merge Join transform called '"
              + transformMeta.getName()
              + "'");
    }
    PCollection<HopRow> rightPCollection = transformCollectionMap.get(rightInfoTransform.getName());
    if (rightPCollection == null) {
      throw new HopException(
          "The right source collection in the pipeline couldn't be found (probably a programming error)");
    }
    IRowMeta rightRowMeta = pipelineMeta.getTransformFields(variables, rightInfoTransform);

    // Create key-value pairs (KV) for the left collections
    //
    List<String> leftK = new ArrayList<>(Arrays.asList(leftKeys));
    IRowMeta leftKRowMeta = new RowMeta();
    List<String> leftV = new ArrayList<>();
    IRowMeta leftVRowMeta = new RowMeta();
    for (String leftKey : leftKeys) {
      leftKRowMeta.addValueMeta(leftRowMeta.searchValueMeta(leftKey).clone());
    }
    for (IValueMeta valueMeta : leftRowMeta.getValueMetaList()) {
      String valueName = valueMeta.getName();
      leftV.add(valueName);
      leftVRowMeta.addValueMeta(valueMeta.clone());
    }

    HopKeyValueFn leftKVFn =
        new HopKeyValueFn(
            JsonRowMeta.toJson(leftRowMeta),
            leftK.toArray(new String[0]),
            leftV.toArray(new String[0]),
            transformMeta.getName());
    PCollection<KV<HopRow, HopRow>> leftKVPCollection = leftPCollection.apply(ParDo.of(leftKVFn));

    // Create key-value pairs (KV) for the right collections
    //
    List<String> rightK = new ArrayList<>(Arrays.asList(rightKeys));
    IRowMeta rightKRowMeta = new RowMeta();
    List<String> rightV = new ArrayList<>();
    IRowMeta rightVRowMeta = new RowMeta();
    for (String rightKey : rightKeys) {
      rightKRowMeta.addValueMeta(rightRowMeta.searchValueMeta(rightKey).clone());
    }
    for (IValueMeta valueMeta : rightRowMeta.getValueMetaList()) {
      String valueName = valueMeta.getName();
      rightV.add(valueName);
      rightVRowMeta.addValueMeta(valueMeta.clone());
    }

    HopKeyValueFn rightKVFn =
        new HopKeyValueFn(
            JsonRowMeta.toJson(rightRowMeta),
            rightK.toArray(new String[0]),
            rightV.toArray(new String[0]),
            transformMeta.getName());
    PCollection<KV<HopRow, HopRow>> rightKVPCollection =
        rightPCollection.apply(ParDo.of(rightKVFn));

    PCollection<KV<HopRow, KV<HopRow, HopRow>>> kvpCollection;

    // For efficiency of detecting "all null value rows" we send an empty row as null value.
    //
    Object[] leftNull = new Object[0];
    Object[] rightNull = new Object[0];

    int assemblerJoinType;
    if (MergeJoinMeta.joinTypes[0].equals(joinType)) {
      // Inner Join
      //
      kvpCollection = Join.innerJoin(leftKVPCollection, rightKVPCollection);
      assemblerJoinType = MergeJoinAssemblerFn.JOIN_TYPE_INNER;
    } else if (MergeJoinMeta.joinTypes[1].equals(joinType)) {
      // Left outer join
      //
      kvpCollection =
          Join.leftOuterJoin(leftKVPCollection, rightKVPCollection, new HopRow(rightNull));
      assemblerJoinType = MergeJoinAssemblerFn.JOIN_TYPE_LEFT_OUTER;
    } else if (MergeJoinMeta.joinTypes[2].equals(joinType)) {
      // Right outer join
      //
      kvpCollection =
          Join.rightOuterJoin(leftKVPCollection, rightKVPCollection, new HopRow(leftNull));
      assemblerJoinType = MergeJoinAssemblerFn.JOIN_TYPE_RIGHT_OUTER;
    } else if (MergeJoinMeta.joinTypes[3].equals(joinType)) {
      // Full outer join
      //
      kvpCollection =
          Join.fullOuterJoin(
              leftKVPCollection, rightKVPCollection, new HopRow(leftNull), new HopRow(rightNull));
      assemblerJoinType = MergeJoinAssemblerFn.JOIN_TYPE_FULL_OUTER;
    } else {
      throw new HopException("Join type '" + joinType + "' is not recognized or supported");
    }

    // The output of the Hop transform is all the left side fields combined with all the right hand
    // side fields.
    // We'll output the same row layout in the Beam engines to avoid confusion for the user.
    //
    final IRowMeta outputRowMeta = leftRowMeta.clone();
    outputRowMeta.addRowMeta(rightRowMeta.clone());

    // The actual output of the Join PCollections is KV(keys, KV(left values, right values))
    // So we need to do some work to assemble the fields back in the right place.
    //
    //
    DoFn<KV<HopRow, KV<HopRow, HopRow>>, HopRow> assemblerFn =
        new MergeJoinAssemblerFn(
            assemblerJoinType,
            JsonRowMeta.toJson(leftRowMeta),
            JsonRowMeta.toJson(rightRowMeta),
            JsonRowMeta.toJson(leftKRowMeta),
            JsonRowMeta.toJson(leftVRowMeta),
            JsonRowMeta.toJson(rightKRowMeta),
            JsonRowMeta.toJson(rightVRowMeta),
            transformMeta.getName());

    // Apply the transform to the previous io transform PCollection(s)
    //
    PCollection<HopRow> transformPCollection = kvpCollection.apply(ParDo.of(assemblerFn));

    // Save this in the map
    //
    transformCollectionMap.put(transformMeta.getName(), transformPCollection);

    log.logBasic("Handled Merge Join (TRANSFORM) : " + transformMeta.getName());
  }
}
