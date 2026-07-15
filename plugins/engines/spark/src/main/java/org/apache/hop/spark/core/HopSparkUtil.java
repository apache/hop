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

package org.apache.hop.spark.core;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.core.xml.XmlHandlerCache;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

public final class HopSparkUtil {

  private static final Object LOAD_LOCK = new Object();

  private HopSparkUtil() {}

  public static void loadTransformMetadataFromXml(
      String transformName,
      ITransformMeta iTransformMeta,
      String transformXml,
      IHopMetadataProvider metadataProvider)
      throws HopException {
    synchronized (LOAD_LOCK) {
      Document transformDocument = XmlHandler.loadXmlString(transformXml);
      if (transformDocument == null) {
        throw new HopException("Unable to load transform XML document from : " + transformXml);
      }
      Node transformNode = XmlHandler.getSubNode(transformDocument, TransformMeta.XML_TAG);
      if (transformNode == null) {
        throw new HopException(
            "Unable to find XML tag " + TransformMeta.XML_TAG + " from : " + transformXml);
      }
      try {
        iTransformMeta.loadXml(transformNode, metadataProvider);
      } catch (Exception e) {
        throw new HopException(
            "There was an error loading transform metadata information (loadXml) for transform '"
                + transformName
                + "'",
            e);
      } finally {
        XmlHandlerCache.getInstance().clear();
      }
    }
  }
}
