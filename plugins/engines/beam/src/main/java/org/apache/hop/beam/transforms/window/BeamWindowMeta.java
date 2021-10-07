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

package org.apache.hop.beam.transforms.window;

import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.beam.core.BeamDefaults;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.fn.WindowInfoFn;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.pipeline.IBeamPipelineTransformHandler;
import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.Dummy;
import org.apache.hop.pipeline.transforms.dummy.DummyData;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.joda.time.Duration;

import java.util.List;
import java.util.Map;

@Transform(
    id = "BeamWindow",
    name = "Beam Window",
    description = "Create a Beam Window",
    image = "beam-window.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.BigData",
        keywords = "i18n::BeamWindowMeta.keyword",
    documentationUrl = "/pipeline/transforms/beamwindow.html")
public class BeamWindowMeta extends BaseTransformMeta
    implements ITransformMeta<Dummy, DummyData>, IBeamPipelineTransformHandler {

  @HopMetadataProperty(key = "window_type")
  private String windowType;

  @HopMetadataProperty private String duration;
  @HopMetadataProperty private String every;

  @HopMetadataProperty(key = "max_window_field")
  private String maxWindowField;

  @HopMetadataProperty(key = "start_window_field")
  private String startWindowField;

  @HopMetadataProperty(key = "end_window_field")
  private String endWindowField;

  public BeamWindowMeta() {
    windowType = BeamDefaults.WINDOW_TYPE_FIXED;
    duration = "60";
    every = "";
    startWindowField = "startWindow";
    endWindowField = "endWindow";
    maxWindowField = "maxWindow";
  }

  @Override
  public Dummy createTransform(
      TransformMeta transformMeta,
      DummyData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new Dummy(transformMeta, new DummyMeta(), data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public DummyData getTransformData() {
    return new DummyData();
  }

  @Override
  public String getDialogClassName() {
    return BeamWindowDialog.class.getName();
  }

  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    if (StringUtils.isNotEmpty(startWindowField)) {
      ValueMetaDate valueMeta = new ValueMetaDate(variables.resolve(startWindowField));
      valueMeta.setOrigin(name);
      valueMeta.setConversionMask(ValueMetaBase.DEFAULT_DATE_FORMAT_MASK);
      inputRowMeta.addValueMeta(valueMeta);
    }
    if (StringUtils.isNotEmpty(endWindowField)) {
      ValueMetaDate valueMeta = new ValueMetaDate(variables.resolve(endWindowField));
      valueMeta.setOrigin(name);
      valueMeta.setConversionMask(ValueMetaBase.DEFAULT_DATE_FORMAT_MASK);
      inputRowMeta.addValueMeta(valueMeta);
    }
    if (StringUtils.isNotEmpty(maxWindowField)) {
      ValueMetaDate valueMeta = new ValueMetaDate(variables.resolve(maxWindowField));
      valueMeta.setOrigin(name);
      valueMeta.setConversionMask(ValueMetaBase.DEFAULT_DATE_FORMAT_MASK);
      inputRowMeta.addValueMeta(valueMeta);
    }
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
      IBeamPipelineEngineRunConfiguration runConfiguration,
      IHopMetadataProvider metadataProvider,
      PipelineMeta pipelineMeta,
      List<String> transformPluginClasses,
      List<String> xpPluginClasses,
      TransformMeta transformMeta,
      Map<String, PCollection<HopRow>> transformCollectionMap,
      org.apache.beam.sdk.Pipeline pipeline,
      IRowMeta rowMeta,
      List<TransformMeta> previousTransforms,
      PCollection<HopRow> input)
      throws HopException {
    if (StringUtils.isEmpty(windowType)) {
      throw new HopException(
          "Please specify a window type in Beam Window transform '"
              + transformMeta.getName()
              + "'");
    }

    String realDuration = variables.resolve(duration);
    long durationSeconds = Const.toLong(realDuration, -1L);

    PCollection<HopRow> transformPCollection;

    if (BeamDefaults.WINDOW_TYPE_FIXED.equals(windowType)) {

      if (durationSeconds <= 0) {
        throw new HopException(
            "Please specify a valid positive window size (duration) for Beam window transform '"
                + transformMeta.getName()
                + "'");
      }

      FixedWindows fixedWindows = FixedWindows.of(Duration.standardSeconds(durationSeconds));
      transformPCollection = input.apply(Window.into(fixedWindows));

    } else if (BeamDefaults.WINDOW_TYPE_SLIDING.equals(windowType)) {

      if (durationSeconds <= 0) {
        throw new HopException(
            "Please specify a valid positive window size (duration) for Beam window transform '"
                + transformMeta.getName()
                + "'");
      }

      String realEvery = variables.resolve(every);
      long everySeconds = Const.toLong(realEvery, -1L);

      SlidingWindows slidingWindows =
          SlidingWindows.of(Duration.standardSeconds(durationSeconds))
              .every(Duration.standardSeconds(everySeconds));
      transformPCollection = input.apply(Window.into(slidingWindows));

    } else if (BeamDefaults.WINDOW_TYPE_SESSION.equals(windowType)) {

      if (durationSeconds < 600) {
        throw new HopException(
            "Please specify a window size (duration) of at least 600 (10 minutes) for Beam window transform '"
                + transformMeta.getName()
                + "'.  This is the minimum gap between session windows.");
      }

      Sessions sessionWindows = Sessions.withGapDuration(Duration.standardSeconds(durationSeconds));
      transformPCollection = input.apply(Window.into(sessionWindows));

    } else if (BeamDefaults.WINDOW_TYPE_GLOBAL.equals(windowType)) {

      transformPCollection = input.apply(Window.into(new GlobalWindows()));

    } else {
      throw new HopException(
          "Beam Window type '"
              + windowType
              + " is not supported in transform '"
              + transformMeta.getName()
              + "'");
    }

    // Now get window information about the window if we asked about it...
    //
    if (StringUtils.isNotEmpty(startWindowField)
        || StringUtils.isNotEmpty(endWindowField)
        || StringUtils.isNotEmpty(maxWindowField)) {

      WindowInfoFn windowInfoFn =
          new WindowInfoFn(
              transformMeta.getName(),
              variables.resolve(maxWindowField),
              variables.resolve(startWindowField),
              variables.resolve(endWindowField),
              JsonRowMeta.toJson(rowMeta),
              transformPluginClasses,
              xpPluginClasses);

      transformPCollection = transformPCollection.apply(ParDo.of(windowInfoFn));
    }

    // Save this in the map
    //
    transformCollectionMap.put(transformMeta.getName(), transformPCollection);
    log.logBasic(
        "Handled transform (WINDOW) : "
            + transformMeta.getName()
            + ", gets data from "
            + previousTransforms.size()
            + " previous transform(s)");
  }

  /**
   * Gets windowType
   *
   * @return value of windowType
   */
  public String getWindowType() {
    return windowType;
  }

  /** @param windowType The windowType to set */
  public void setWindowType(String windowType) {
    this.windowType = windowType;
  }

  /**
   * Gets duration
   *
   * @return value of duration
   */
  public String getDuration() {
    return duration;
  }

  /** @param duration The duration to set */
  public void setDuration(String duration) {
    this.duration = duration;
  }

  /**
   * Gets every
   *
   * @return value of every
   */
  public String getEvery() {
    return every;
  }

  /** @param every The every to set */
  public void setEvery(String every) {
    this.every = every;
  }

  /**
   * Gets maxWindowField
   *
   * @return value of maxWindowField
   */
  public String getMaxWindowField() {
    return maxWindowField;
  }

  /** @param maxWindowField The maxWindowField to set */
  public void setMaxWindowField(String maxWindowField) {
    this.maxWindowField = maxWindowField;
  }

  /**
   * Gets startWindowField
   *
   * @return value of startWindowField
   */
  public String getStartWindowField() {
    return startWindowField;
  }

  /** @param startWindowField The startWindowField to set */
  public void setStartWindowField(String startWindowField) {
    this.startWindowField = startWindowField;
  }

  /**
   * Gets endWindowField
   *
   * @return value of endWindowField
   */
  public String getEndWindowField() {
    return endWindowField;
  }

  /** @param endWindowField The endWindowField to set */
  public void setEndWindowField(String endWindowField) {
    this.endWindowField = endWindowField;
  }
}
