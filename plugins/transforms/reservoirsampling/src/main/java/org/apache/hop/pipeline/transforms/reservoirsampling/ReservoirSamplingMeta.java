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

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.List;
import java.util.Objects;

@Transform(
    id = "ReservoirSampling",
    image = "reservoirsampling.svg",
    name = "i18n::BaseTransform.TypeLongDesc.ReservoirSampling",
    description = "i18n::BaseTransform.TypeTooltipDesc.ReservoirSampling",
    categoryDescription =
        "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Statistics",
    documentationUrl =
        "https://hop.apache.org/manual/latest/plugins/transforms/reservoirsampling.html")
public class ReservoirSamplingMeta extends BaseTransformMeta
    implements ITransformMeta<ReservoirSampling, ReservoirSamplingData> {

  public static final String XML_TAG = "reservoir_sampling";

  // Size of the sample to output
  protected String m_sampleSize = "100";

  // Seed for the random number generator
  protected String m_randomSeed = "1";

  /** Creates a new <code>ReservoirMeta</code> instance. */
  public ReservoirSamplingMeta() {
    super(); // allocate BaseTransformMeta
  }

  /**
   * Get the sample size to generate.
   *
   * @return the sample size
   */
  public String getSampleSize() {
    return m_sampleSize;
  }

  /**
   * Set the size of the sample to generate
   *
   * @param sampleS the size of the sample
   */
  public void setSampleSize(String sampleS) {
    m_sampleSize = sampleS;
  }

  /**
   * Get the random seed
   *
   * @return the random seed
   */
  public String getSeed() {
    return m_randomSeed;
  }

  /**
   * Set the seed value for the random number generator
   *
   * @param seed the seed value
   */
  public void setSeed(String seed) {
    m_randomSeed = seed;
  }

  /**
   * Return the XML describing this (configured) transform
   *
   * @return a <code>String</code> containing the XML
   */
  public String getXml() {
    StringBuilder retval = new StringBuilder(100);

    retval.append(XmlHandler.openTag(XML_TAG)).append(Const.CR);
    retval.append(XmlHandler.addTagValue("sample_size", m_sampleSize));
    retval.append(XmlHandler.addTagValue("seed", m_randomSeed));
    retval.append(XmlHandler.closeTag(XML_TAG)).append(Const.CR);

    return retval.toString();
  }

  /**
   * Check for equality
   *
   * @param obj an <code>Object</code> to compare with
   * @return true if equal to the supplied object
   */
  public boolean equals(Object obj) {
    if (obj != null && (obj.getClass().equals(this.getClass()))) {
      ReservoirSamplingMeta m = (ReservoirSamplingMeta) obj;
      return (getXml() == m.getXml());
    }

    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(m_sampleSize, m_randomSeed);
  }

  /** Set the defaults for this transform. */
  public void setDefault() {
    m_sampleSize = "100";
    m_randomSeed = "1";
  }

  /**
   * Clone this transform's meta data
   *
   * @return the cloned meta data
   */
  public Object clone() {
    ReservoirSamplingMeta retval = (ReservoirSamplingMeta) super.clone();
    return retval;
  }

  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      ReservoirSamplingData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new ReservoirSampling(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  /**
   * Loads the meta data for this (configured) transform from XML.
   *
   * @param transformNode the transform to load
   * @throws HopXmlException if an error occurs
   */
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {

    int nrTransforms = XmlHandler.countNodes(transformNode, XML_TAG);

    if (nrTransforms > 0) {
      Node reservoirnode = XmlHandler.getSubNodeByNr(transformNode, XML_TAG, 0);

      m_sampleSize = XmlHandler.getTagValue(reservoirnode, "sample_size");
      m_randomSeed = XmlHandler.getTagValue(reservoirnode, "seed");
    }
  }

  public void getFields(
      IRowMeta row,
      String origin,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    // nothing to do, as no fields are added/deleted
  }

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

    if ((prev == null) || (prev.size() == 0)) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_WARNING,
              "Not receiving any fields from previous transforms!",
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              "Transform is connected to previous one, receiving " + prev.size() + " fields",
              transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              "Transform is receiving info from other transforms.",
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              "No input received from other transforms!",
              transformMeta);
      remarks.add(cr);
    }
  }

  /**
   * Get a new instance of the appropriate data class. This data class implements the
   * ITransformData. It basically contains the persisting data that needs to live on, even if a
   * worker thread is terminated.
   *
   * @return a <code>ITransformData</code> value
   */
  public ReservoirSamplingData getTransformData() {
    return new ReservoirSamplingData();
  }
}
