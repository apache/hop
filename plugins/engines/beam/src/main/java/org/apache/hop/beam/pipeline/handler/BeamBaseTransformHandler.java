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

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

public class BeamBaseTransformHandler {

  public BeamBaseTransformHandler() {}

  protected Node getTransformXmlNode(TransformMeta transformMeta) throws HopException {
    String xml = transformMeta.getXml();
    Node transformNode =
        XmlHandler.getSubNode(XmlHandler.loadXmlString(xml), TransformMeta.XML_TAG);
    return transformNode;
  }

  protected void loadTransformMetadata(
      ITransformMeta meta,
      TransformMeta transformMeta,
      IHopMetadataProvider metadataProvider,
      PipelineMeta pipelineMeta)
      throws HopException {
    meta.loadXml(getTransformXmlNode(transformMeta), metadataProvider);
    meta.searchInfoAndTargetTransforms(pipelineMeta.getTransforms());
  }
}
