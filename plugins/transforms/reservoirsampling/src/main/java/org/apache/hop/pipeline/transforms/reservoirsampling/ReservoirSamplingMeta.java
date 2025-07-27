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

package org.apache.hop.pipeline.transforms.reservoirsampling;

import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataWrapper;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "ReservoirSampling",
    image = "reservoirsampling.svg",
    name = "i18n::ReservoirSampling.Name",
    description = "i18n::ReservoirSampling.Description",
    categoryDescription =
        "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Statistics",
    keywords = "i18n::ReservoirSamplingMeta.keyword",
    documentationUrl = "/pipeline/transforms/reservoirsampling.html")
@HopMetadataWrapper(tag = "reservoir_sampling")
public class ReservoirSamplingMeta
    extends BaseTransformMeta<ReservoirSampling, ReservoirSamplingData> {

  public static final String XML_TAG = "reservoir_sampling";

  // Size of the sample to output
  @HopMetadataProperty(key = "sample_size")
  protected String sampleSize;

  // Seed for the random number generator
  @HopMetadataProperty(key = "seed")
  protected String seed;

  /** Creates a new <code>ReservoirMeta</code> instance. */
  public ReservoirSamplingMeta() {
    super();
  }

  public ReservoirSamplingMeta(ReservoirSamplingMeta m) {
    this();
    this.sampleSize = m.sampleSize;
    this.seed = m.seed;
  }

  /** Set the defaults for this transform. */
  @Override
  public void setDefault() {
    sampleSize = "100";
    seed = "1";
  }

  /**
   * Clone this transform's meta data
   *
   * @return the cloned meta data
   */
  @Override
  public ReservoirSamplingMeta clone() {
    return new ReservoirSamplingMeta(this);
  }

  @Override
  public void getFields(
      IRowMeta row,
      String origin,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {

    // nothing to do, as no fields are added/deleted
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {

    CheckResult cr;

    if ((prev == null) || (prev.isEmpty())) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_WARNING,
              "Not receiving any fields from previous transforms!",
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              "Transform is connected to previous one, receiving " + prev.size() + " fields",
              transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              "Transform is receiving info from other transforms.",
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              "No input received from other transforms!",
              transformMeta);
      remarks.add(cr);
    }
  }

  /**
   * Get the sample size to generate.
   *
   * @return the sample size
   */
  public String getSampleSize() {
    return sampleSize;
  }

  /**
   * Set the size of the sample to generate
   *
   * @param sampleSize the size of the sample
   */
  public void setSampleSize(String sampleSize) {
    this.sampleSize = sampleSize;
  }

  /**
   * Get the random seed
   *
   * @return the random seed
   */
  public String getSeed() {
    return seed;
  }

  /**
   * Set the seed value for the random number generator
   *
   * @param seed the seed value
   */
  public void setSeed(String seed) {
    this.seed = seed;
  }
}
