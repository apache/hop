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
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.fn.StringToHopRowFn;
import org.apache.hop.beam.core.shared.VariableValue;
import org.apache.hop.beam.core.transform.TransformBatchTransform;
import org.apache.hop.beam.core.transform.TransformTransform;
import org.apache.hop.beam.core.util.HopBeamUtil;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.pipeline.IBeamPipelineTransformHandler;
import org.apache.hop.beam.util.BeamConst;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.JsonRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.w3c.dom.Node;

public class BeamGenericTransformHandler extends BeamBaseTransformHandler
    implements IBeamPipelineTransformHandler {

  /**
   * Transforms that need all rows of their input to give the local engine's result: they hold rows
   * back until the input ends (Blocking transform, Identify last row, Detect empty stream), or they
   * count or sample the whole stream (Sample rows, Reservoir sampling, Unique rows (HashSet)). On a
   * bounded input they get every row at once, on one worker, and see the end of their input once.
   * Row by row, as other transforms run, each worker saw only its own rows and none saw the end.
   *
   * <p>Group by joins them when it has no group fields: it then aggregates the whole stream. With
   * group fields it needs sorted rows, and Beam refuses it.
   */
  public static final Set<String> WHOLE_STREAM_PLUGIN_IDS =
      Set.of(
          "BlockingTransform",
          "DetectEmptyStream",
          "DetectLastRow",
          "ReservoirSampling",
          "SampleRows",
          "UniqueRowsByHashSet");

  /**
   * Whether the transform runs on all rows of its input at once, see {@link
   * #WHOLE_STREAM_PLUGIN_IDS}.
   */
  public static boolean needsWholeStream(TransformMeta transformMeta) throws HopException {
    String pluginId = transformMeta.getTransformPluginId();
    if (GROUP_BY_PLUGIN_ID.equals(pluginId)) {
      return !hasGroupFields(transformMeta);
    }
    return WHOLE_STREAM_PLUGIN_IDS.contains(pluginId);
  }

  /** The plugin id of Group by. */
  public static final String GROUP_BY_PLUGIN_ID = "GroupBy";

  /**
   * Whether a Group by groups on fields. Read from the transform XML: the meta class of an
   * installed transform comes from its own plugin class loader, so the Beam plugin cannot cast it.
   */
  public static boolean hasGroupFields(TransformMeta transformMeta) throws HopException {
    Node transformNode =
        XmlHandler.getSubNode(
            XmlHandler.loadXmlString(transformMeta.getXml()), TransformMeta.XML_TAG);
    Node groupNode = XmlHandler.getSubNode(transformNode, "group");
    return groupNode != null && XmlHandler.countNodes(groupNode, "field") > 0;
  }

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

    // If we have no previous transform, it's an input transform.  We need to start from pipeline
    //
    boolean inputTransform = input == null;

    String transformMetaInterfaceXml =
        XmlHandler.openTag(TransformMeta.XML_TAG)
            + transformMeta.getTransform().getXml()
            + XmlHandler.closeTag(TransformMeta.XML_TAG);

    // See if the transform has Info transforms
    //
    List<TransformMeta> infoTransformMetas =
        pipelineMeta.findPreviousTransforms(transformMeta, true);
    List<String> infoTransforms = new ArrayList<>();
    List<String> infoRowMetaJsons = new ArrayList<>();
    List<PCollectionView<List<HopRow>>> infoCollectionViews = new ArrayList<>();
    for (TransformMeta infoTransformMeta : infoTransformMetas) {
      if (previousTransforms.contains(infoTransformMeta)) {
        continue;
      }
      infoTransforms.add(infoTransformMeta.getName());
      infoRowMetaJsons.add(
          JsonRowMeta.toJson(pipelineMeta.getTransformFields(variables, infoTransformMeta)));
      PCollection<HopRow> infoCollection = transformCollectionMap.get(infoTransformMeta.getName());
      if (infoCollection == null) {
        throw new HopException(
            "Unable to find collection for transform '"
                + infoTransformMeta.getName()
                + " providing info for '"
                + transformMeta.getName()
                + "'");
      }
      infoCollectionViews.add(infoCollection.apply(View.asList()));
    }

    // Get the list of variables from the PipelineMeta variable variables:
    //
    List<VariableValue> variableValues = getVariableValues(variables);

    // Find out all the target transforms for this transform...
    //
    ITransformIOMeta ioMeta = transformMeta.getTransform().getTransformIOMeta();
    List<String> targetTransforms = new ArrayList<>();
    for (IStream targetStream : ioMeta.getTargetStreams()) {
      if (targetStream.getTransformMeta() != null) {
        targetTransforms.add(targetStream.getTransformMeta().getName());
      }
    }

    // For streaming pipelines we need to flush the rows in the buffer of a generic transform (Table
    // Output, Neo4j Output, ...)
    // This is what the BeamJobConfig option "Streaming Hop Transforms Flush Interval" is for...
    // Without a valid value we default to -1 to disable flushing.
    //
    int sizeRowSet = Const.toInt(runConfiguration.getStreamingHopTransformsBufferSize(), 5000);
    int flushIntervalMs =
        Const.toInt(runConfiguration.getStreamingHopTransformsFlushInterval(), -1);

    // Serialize the whole metastore to JSON...
    //
    String metaStoreJson = new SerializableMetadataProvider(metadataProvider).toJson();

    // Send all the information on their way to the right nodes
    //
    boolean wholeStream = !inputTransform && needsWholeStream(transformMeta);
    if (wholeStream && input.isBounded() != PCollection.IsBounded.BOUNDED) {
      throw new HopException(
          "Transform '"
              + transformMeta.getName()
              + "' needs all the rows of its input at once, which an unbounded (streaming) Beam"
              + " pipeline does not have.");
    }

    PTransform<PCollection<HopRow>, PCollectionTuple> transformTransform;
    if (!wholeStream && needsBatching(variables, transformMeta)) {
      transformTransform =
          new TransformBatchTransform(
              variableValues,
              metaStoreJson,
              sizeRowSet,
              flushIntervalMs,
              transformMeta.getName(),
              transformMeta.getTransformPluginId(),
              transformMetaInterfaceXml,
              JsonRowMeta.toJson(rowMeta),
              inputTransform,
              targetTransforms,
              infoTransforms,
              infoRowMetaJsons,
              infoCollectionViews,
              runConfigurationName,
              dataSamplersJson,
              parentLogChannelId);
    } else {
      transformTransform =
          new TransformTransform(
              variableValues,
              metaStoreJson,
              sizeRowSet,
              flushIntervalMs,
              transformMeta.getName(),
              transformMeta.getTransformPluginId(),
              transformMetaInterfaceXml,
              JsonRowMeta.toJson(rowMeta),
              inputTransform,
              targetTransforms,
              infoTransforms,
              infoRowMetaJsons,
              infoCollectionViews,
              runConfigurationName,
              dataSamplersJson,
              parentLogChannelId);
      ((TransformTransform) transformTransform).setWholeStream(wholeStream);
    }

    if (input == null) {
      // Start from a dummy row and group over it.
      // Trick Beam into only running a single thread of the transform that comes next.
      //
      input =
          pipeline
              .apply(Create.of(List.of("hop-single-value")))
              .setCoder(StringUtf8Coder.of())
              .apply(WithKeys.of((Void) null))
              .apply(GroupByKey.create())
              .apply(Values.create())
              .apply(Flatten.iterables())
              .apply(
                  ParDo.of(
                      new StringToHopRowFn(transformMeta.getName(), JsonRowMeta.toJson(rowMeta))));

      // Store this new collection so we can hook up other transforms...
      //
      String tupleId = HopBeamUtil.createMainInputTupleId(transformMeta.getName());
      transformCollectionMap.put(tupleId, input);
    } else if (!wholeStream
        && (checkTransformCopiesForReducedParallelism(transformMeta)
            || needsSingleThreading(transformMeta))) {
      PCollection.IsBounded isBounded = input.isBounded();
      if (isBounded == PCollection.IsBounded.BOUNDED) {
        // group across all fields to get down to a single thread...
        //
        input =
            input
                .apply(WithKeys.of((Void) null))
                .setCoder(KvCoder.of(VoidCoder.of(), input.getCoder()))
                .apply(GroupByKey.create())
                .apply(Values.create())
                .apply(Flatten.iterables());
      } else {
        throw new HopException(
            "Hop is unable to reduce the parallelism of transform '"
                + transformMeta.getName()
                + "' in an unbounded (streaming) pipeline.");
      }
    }

    // Apply the transform transform to the previous io transform PCollection(s)
    //
    PCollectionTuple tuple = input.apply(transformMeta.getName(), transformTransform);

    // The main collection
    //
    PCollection<HopRow> mainPCollection =
        tuple.get(new TupleTag<>(HopBeamUtil.createMainOutputTupleId(transformMeta.getName())));

    // Save this in the map
    //
    transformCollectionMap.put(transformMeta.getName(), mainPCollection);

    // Were there any targeted transforms in this transform?
    //
    for (String targetTransform : targetTransforms) {
      String tupleId = HopBeamUtil.createTargetTupleId(transformMeta.getName(), targetTransform);
      PCollection<HopRow> targetPCollection = tuple.get(new TupleTag<>(tupleId));

      // Store this in the map as well
      //
      transformCollectionMap.put(tupleId, targetPCollection);
    }

    log.logBasic(
        "Handled generic transform (TRANSFORM) : "
            + transformMeta.getName()
            + ", gets data from "
            + previousTransforms.size()
            + " previous transform(s), targets="
            + targetTransforms.size()
            + ", infos="
            + infoTransforms.size());
  }

  public static boolean needsBatching(IVariables variables, TransformMeta transformMeta) {
    String value =
        transformMeta.getAttribute(
            BeamConst.STRING_HOP_BEAM, BeamConst.STRING_TRANSFORM_FLAG_BATCH);

    // If the copies string contains "BATCH" we'll batch the records...
    //
    String copiesString = variables.resolve(transformMeta.getCopiesString());

    return Const.toBoolean(value)
        || copiesString != null && copiesString.toUpperCase().contains("BATCH");
  }

  public static boolean needsSingleThreading(TransformMeta transformMeta) {
    String value =
        transformMeta.getAttribute(
            BeamConst.STRING_HOP_BEAM, BeamConst.STRING_TRANSFORM_FLAG_SINGLE_THREADED);
    return Const.toBoolean(value);
  }

  private boolean checkTransformCopiesForReducedParallelism(TransformMeta transformMeta) {
    if (transformMeta.getCopiesString() == null) {
      return false;
    }
    String copiesString = transformMeta.getCopiesString();

    String[] keyWords = {"BEAM_SINGLE", "SINGLE_BEAM", "BEAM_OUTPUT", "OUTPUT"};

    for (String keyWord : keyWords) {
      if (copiesString.contains(keyWord)) {
        return true;
      }
    }
    return false;
  }

  private List<VariableValue> getVariableValues(IVariables variables) {

    List<VariableValue> variableValues = new ArrayList<>();
    for (String variable : variables.getVariableNames()) {
      String value = variables.getVariable(variable);
      variableValues.add(new VariableValue(variable, value));
    }
    return variableValues;
  }
}
