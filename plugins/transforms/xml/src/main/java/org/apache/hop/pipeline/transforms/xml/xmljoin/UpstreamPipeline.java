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

package org.apache.hop.pipeline.transforms.xml.xmljoin;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.core.xml.XmlParserFactoryProducer;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * What Load does, without the dialog: a copy of a pipeline with one transform and the transforms
 * that send rows to it, and the XML document in a field of the first row that transform sends.
 */
final class UpstreamPipeline {
  private static final Class<?> PKG = XmlJoinMeta.class;

  private static final String DUMMY_PLUGIN_ID = "Dummy";

  /** Where the transform categories are named. */
  private static final String CATEGORY_PACKAGE = "org.apache.hop.pipeline.transform";

  /** The categories of transforms that read, or work on the rows they get, and write nothing. */
  private static final List<String> READ_ONLY_CATEGORIES =
      List.of(
          "BaseTransform.Category.Input",
          "BaseTransform.Category.Transform",
          "BaseTransform.Category.Flow",
          "BaseTransform.Category.Joins",
          "BaseTransform.Category.Statistics",
          "BaseTransform.Category.Validation",
          "BaseTransform.Category.Inline");

  /**
   * Transforms in those categories that do more: they run other pipelines, workflows or SQL, or
   * take away the mail or messages they read.
   */
  private static final Set<String> NOT_READ_ONLY =
      Set.of(
          "PipelineExecutor",
          "WorkflowExecutor",
          "MetaInject",
          "ExecuteTests",
          "DynamicSqlRow",
          "MailInput",
          "AwsSqSreader");

  private UpstreamPipeline() {}

  /**
   * Copies the pipeline through its XML, the way it is copied elsewhere, and leaves out every
   * transform that doesn't lead to the given one. A preview of the copy runs nothing after that
   * transform: not the join, not an output.
   *
   * <p>A transform that one of the kept transforms sends rows to, over a hop or an error hop, stays
   * as an empty Dummy. A Switch / case or a transform with error handling looks up the transforms
   * it writes to by name, and fails when one of them is gone.
   */
  static PipelineMeta of(PipelineMeta pipelineMeta, IVariables variables, String transformName)
      throws HopException {
    TransformMeta last = pipelineMeta.findTransform(transformName);
    if (last == null) {
      throw new HopException(
          BaseMessages.getString(PKG, "XmlJoin.Exception.TransformNotFound", transformName));
    }
    Set<String> keep = namesUpTo(pipelineMeta, last);

    PipelineMeta copy =
        new PipelineMeta(
            XmlHandler.getSubNode(
                XmlHandler.loadXmlString(pipelineMeta.getXml(variables)), PipelineMeta.XML_TAG),
            pipelineMeta.getMetadataProvider());
    copy.setFilename(pipelineMeta.getFilename());

    Set<String> placeholders = new HashSet<>();
    for (PipelineHopMeta hop : copy.getPipelineHops()) {
      if (keep.contains(hop.getFromTransform().getName())
          && !keep.contains(hop.getToTransform().getName())) {
        placeholders.add(hop.getToTransform().getName());
      }
    }
    for (int i = copy.nrTransforms() - 1; i >= 0; i--) {
      String name = copy.getTransform(i).getName();
      if (!keep.contains(name) && !placeholders.contains(name)) {
        // Also drops the hops and the error handling that point to the transform
        copy.removeTransform(i);
      }
    }
    for (String name : placeholders) {
      makeEmptyDummy(copy, copy.findTransform(name));
    }
    return copy;
  }

  /** Turns a transform into a Dummy that sends its rows nowhere. */
  private static void makeEmptyDummy(PipelineMeta copy, TransformMeta transformMeta) {
    for (PipelineHopMeta hop : new ArrayList<>(copy.getPipelineHops())) {
      if (transformMeta.equals(hop.getFromTransform())) {
        copy.removePipelineHop(hop);
      }
    }
    transformMeta.setTransform(new DummyMeta());
    transformMeta.setTransformPluginId(DUMMY_PLUGIN_ID);
    transformMeta.setTransformErrorMeta(null);
    transformMeta.setCopiesString("1");
  }

  /**
   * The transforms of a pipeline that may write or run something else when it runs: those outside
   * the categories of transforms that only read or work on rows, and the few inside them that do
   * more. A transform whose plugin is unknown is one of them.
   */
  static List<String> mayWrite(PipelineMeta pipelineMeta) {
    Set<String> readOnly = new HashSet<>();
    for (String key : READ_ONLY_CATEGORIES) {
      // Plugins are registered with their category in the language Hop shows
      readOnly.add(BaseMessages.getString(CATEGORY_PACKAGE, key));
    }
    PluginRegistry registry = PluginRegistry.getInstance();
    List<String> names = new ArrayList<>();
    for (TransformMeta transformMeta : pipelineMeta.getTransforms()) {
      String pluginId = transformMeta.getTransformPluginId();
      IPlugin plugin = registry.findPluginWithId(TransformPluginType.class, pluginId);
      if (plugin == null
          || NOT_READ_ONLY.contains(pluginId)
          || !readOnly.contains(plugin.getCategory())) {
        names.add(transformMeta.getName());
      }
    }
    return names;
  }

  /**
   * The XML document in a field of a row, parsed the way the transform parses its target document.
   *
   * @throws HopException when the row has no such field, the field is empty or holds no XML
   */
  static Document documentIn(IRowMeta rowMeta, Object[] row, String transformName, String fieldName)
      throws HopException {
    int index = rowMeta.indexOfValue(fieldName);
    if (index < 0) {
      throw new HopException(
          BaseMessages.getString(PKG, "XmlJoin.Exception.FieldNotFound", fieldName));
    }
    String xml = rowMeta.getString(row, index);
    if (Utils.isEmpty(xml)) {
      throw new HopException(
          BaseMessages.getString(PKG, "XmlJoin.Document.EmptyDocument", fieldName, transformName));
    }
    try {
      return XmlParserFactoryProducer.createSecureDocBuilderFactory()
          .newDocumentBuilder()
          .parse(new InputSource(new StringReader(xml)));
    } catch (ParserConfigurationException | SAXException | IOException e) {
      throw new HopException(
          BaseMessages.getString(PKG, "XmlJoin.Document.NotXml", fieldName, transformName), e);
    }
  }

  /** The transform and every transform with an enabled path of hops to it. */
  static Set<String> namesUpTo(PipelineMeta pipelineMeta, TransformMeta last) {
    Set<String> names = new HashSet<>();
    Deque<TransformMeta> todo = new ArrayDeque<>();
    todo.push(last);
    while (!todo.isEmpty()) {
      TransformMeta transformMeta = todo.pop();
      if (names.add(transformMeta.getName())) {
        todo.addAll(pipelineMeta.findPreviousTransforms(transformMeta, true));
      }
    }
    return names;
  }
}
