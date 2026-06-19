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

package org.apache.hop.pipeline;

import java.util.List;
import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.base.BaseHopMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

/** Defines a link between 2 transforms in a pipeline */
@Getter
@Setter
public class PipelineHopMeta extends BaseHopMeta<TransformMeta>
    implements Comparable<PipelineHopMeta>, Cloneable {
  private static final Class<?> PKG = Pipeline.class;
  public static final String CONST_SPACES = "      ";

  @HopMetadataProperty(key = "from", storeWithName = true, lookupInList = "transforms")
  protected TransformMeta from;

  @HopMetadataProperty(key = "to", storeWithName = true, lookupInList = "transforms")
  protected TransformMeta to;

  public PipelineHopMeta(TransformMeta from, TransformMeta to, boolean en) {
    this.from = from;
    this.to = to;
    this.enabled = en;
  }

  public PipelineHopMeta(TransformMeta from, TransformMeta to) {
    this.from = from;
    this.to = to;
    this.enabled = true;
  }

  public PipelineHopMeta() {
    this(null, null, false);
  }

  public PipelineHopMeta(Node hopNode, List<TransformMeta> transforms) throws HopXmlException {
    try {
      this.from = searchTransform(transforms, XmlHandler.getTagValue(hopNode, XML_FROM_TAG));
      this.to = searchTransform(transforms, XmlHandler.getTagValue(hopNode, XML_TO_TAG));
      this.enabled = getTagValueAsBoolean(hopNode, XML_ENABLED_TAG, true);
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "PipelineHopMeta.Exception.UnableToLoadHopInfo"), e);
    }
  }

  public PipelineHopMeta(PipelineHopMeta hop) {
    super(
        hop.isSplit(),
        hop.getFromTransform(),
        hop.getToTransform(),
        hop.isEnabled(),
        hop.hasChanged(),
        hop.isErrorHop());
  }

  @Override
  public PipelineHopMeta clone() {
    return new PipelineHopMeta(this);
  }

  public void setFromTransform(TransformMeta from) {
    this.from = from;
  }

  public void setToTransform(TransformMeta to) {
    this.to = to;
  }

  public TransformMeta getFromTransform() {
    return this.from;
  }

  public TransformMeta getToTransform() {
    return this.to;
  }

  private TransformMeta searchTransform(List<TransformMeta> transforms, String name) {
    for (TransformMeta transformMeta : transforms) {
      if (transformMeta.getName().equalsIgnoreCase(name)) {
        return transformMeta;
      }
    }

    return null;
  }

  public boolean equals(Object obj) {
    if (!(obj instanceof PipelineHopMeta other)) {
      return false;
    }
    if (this.from == null || this.to == null) {
      return false;
    }
    return this.from.equals(other.getFromTransform()) && this.to.equals(other.getToTransform());
  }

  public int hashCode() {
    return Objects.hash(to, from);
  }

  /** Compare 2 hops. */
  @Override
  public int compareTo(PipelineHopMeta obj) {
    return toString().compareTo(obj.toString());
  }

  public void flip() {
    TransformMeta dummy = this.from;
    this.from = this.to;
    this.to = dummy;
  }

  public String toString() {
    String strFrom = (this.from == null) ? "(empty)" : this.from.getName();
    String strTo = (this.to == null) ? "(empty)" : this.to.getName();
    return strFrom + " --> " + strTo + " (" + (enabled ? "enabled" : "disabled") + ")";
  }

  public String getXml() throws HopException {
    return XmlHandler.aroundTag(XML_HOP_TAG, XmlMetadataUtil.serializeObjectToXml(this));
  }
}
