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

package org.apache.hop.beam.core.transform;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.shared.VariableValue;
import org.apache.hop.beam.core.util.HopBeamUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransformTransform extends PTransform<PCollection<HopRow>, PCollectionTuple> {

  protected List<VariableValue> variableValues;
  protected String metastoreJson;
  protected int batchSize;
  protected String transformName;
  protected String transformPluginId;
  protected String inputRowMetaJson;
  protected boolean inputTransform;
  protected String transformMetaInterfaceXml;
  protected List<String> targetTransforms;
  protected List<String> infoTransforms;
  protected List<String> infoRowMetaJsons;
  protected int flushIntervalMs;

  // Execution information vectors
  protected String runConfigName;
  protected String dataSamplersJson;
  protected String parentLogChannelId;

  // Used in the private TransformFn class below
  //
  protected List<PCollectionView<List<HopRow>>> infoCollectionViews;

  // Log and count errors.
  protected static final Logger LOG = LoggerFactory.getLogger(TransformTransform.class);
  protected static final Counter numErrors = Metrics.counter("main", "TransformErrors");

  public TransformTransform() {
    variableValues = new ArrayList<>();
  }

  public TransformTransform(
      List<VariableValue> variableValues,
      String metastoreJson,
      int batchSize,
      int flushIntervalMs,
      String transformName,
      String transformPluginId,
      String transformMetaInterfaceXml,
      String inputRowMetaJson,
      boolean inputTransform,
      List<String> targetTransforms,
      List<String> infoTransforms,
      List<String> infoRowMetaJsons,
      List<PCollectionView<List<HopRow>>> infoCollectionViews,
      String runConfigName,
      String dataSamplersJson,
      String parentLogChannelId) {
    this.variableValues = variableValues;
    this.metastoreJson = metastoreJson;
    this.batchSize = batchSize;
    this.flushIntervalMs = flushIntervalMs;
    this.transformName = transformName;
    this.transformPluginId = transformPluginId;
    this.transformMetaInterfaceXml = transformMetaInterfaceXml;
    this.inputRowMetaJson = inputRowMetaJson;
    this.inputTransform = inputTransform;
    this.targetTransforms = targetTransforms;
    this.infoTransforms = infoTransforms;
    this.infoRowMetaJsons = infoRowMetaJsons;
    this.infoCollectionViews = infoCollectionViews;
    this.runConfigName = runConfigName;
    this.dataSamplersJson = dataSamplersJson;
    this.parentLogChannelId = parentLogChannelId;
  }

  @Override
  public PCollectionTuple expand(PCollection<HopRow> input) {
    try {
      // Only initialize once on this node/vm
      //
      BeamHop.init();

      // Similar for the output : treat a TupleTag list for the target transforms...
      //
      TupleTag<HopRow> mainOutputTupleTag =
          new TupleTag<>(HopBeamUtil.createMainOutputTupleId(transformName)) {};
      TupleTagList targetTupleTagList = null;
      for (String targetTransform : targetTransforms) {
        String tupleId = HopBeamUtil.createTargetTupleId(transformName, targetTransform);
        TupleTag<HopRow> tupleTag = new TupleTag<>(tupleId) {};
        if (targetTupleTagList == null) {
          targetTupleTagList = TupleTagList.of(tupleTag);
        } else {
          targetTupleTagList = targetTupleTagList.and(tupleTag);
        }
      }
      if (targetTupleTagList == null) {
        targetTupleTagList = TupleTagList.empty();
      }

      // Create a new transform function, initializes the transform
      //
      TransformFn transformFn =
          new TransformFn(
              variableValues,
              metastoreJson,
              transformName,
              transformPluginId,
              transformMetaInterfaceXml,
              inputRowMetaJson,
              inputTransform,
              targetTransforms,
              infoTransforms,
              infoRowMetaJsons,
              dataSamplersJson,
              runConfigName,
              parentLogChannelId,
              infoCollectionViews);

      // The actual transform functionality
      //
      ParDo.SingleOutput<HopRow, HopRow> parDoTransformFn = ParDo.of(transformFn);

      // Add optional side inputs...
      //
      if (!infoCollectionViews.isEmpty()) {
        parDoTransformFn = parDoTransformFn.withSideInputs(infoCollectionViews);
      }

      // Specify the main output and targeted outputs
      //
      ParDo.MultiOutput<HopRow, HopRow> multiOutput =
          parDoTransformFn.withOutputTags(mainOutputTupleTag, targetTupleTagList);

      // Apply the multi output parallel do transform function to the main input stream
      //

      // In the tuple is everything we need to find.
      // Just make sure to retrieve the PCollections using the correct Tuple ID
      // Use HopBeamUtil.createTargetTupleId()... to make sure
      //
      return input.apply(multiOutput);
    } catch (Exception e) {
      numErrors.inc();
      LOG.error("Error transforming data in transform '" + transformName + "'", e);
      throw new RuntimeException("Error transforming data in transform", e);
    }
  }
}
