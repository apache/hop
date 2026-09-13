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

package org.apache.hop.ai.engine;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.ai.advisor.AiProposal;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.ActionPluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

/** Hop clipboard XML envelopes for AI transform/action proposals. */
public final class AiProposalXmlSupport {

  public static final String PIPELINE_ENVELOPE = "pipeline-transforms";
  public static final String PIPELINE_TRANSFORMS = "transforms";
  public static final String WORKFLOW_ENVELOPE = "workflow-actions";
  public static final int MAX_XML_CHARS = 100_000;

  private AiProposalXmlSupport() {}

  public static String xmlParam(AiProposal proposal) {
    return proposal == null ? "" : Const.NVL(proposal.parameter("xml"), "");
  }

  public static String requireXml(AiProposal proposal) throws HopException {
    String xml = xmlParam(proposal);
    if (Utils.isEmpty(xml)) {
      throw new HopException("xml parameter is required");
    }
    if (xml.length() > MAX_XML_CHARS) {
      throw new HopException("xml exceeds " + MAX_XML_CHARS + " characters");
    }
    return xml;
  }

  public static boolean containsSecrets(String xml) {
    if (Utils.isEmpty(xml)) {
      return false;
    }
    return !xml.equals(AiTextUtil.redactSecrets(xml));
  }

  public static String wrapPipelineClipboard(String xml) {
    if (Utils.isEmpty(xml)) {
      return "";
    }
    if (hasOpenTag(xml, PIPELINE_ENVELOPE)) {
      return withXmlHeader(xml);
    }
    String transformsBlock =
        hasOpenTag(xml, PIPELINE_TRANSFORMS)
            ? xml
            : "<"
                + PIPELINE_TRANSFORMS
                + ">\n"
                + (hasOpenTag(xml, TransformMeta.XML_TAG)
                    ? xml
                    : wrapTag(TransformMeta.XML_TAG, xml))
                + "</"
                + PIPELINE_TRANSFORMS
                + ">\n";
    return withXmlHeader(
        "<"
            + PIPELINE_ENVELOPE
            + ">\n"
            + transformsBlock
            + "<"
            + PipelineMeta.XML_TAG_ORDER
            + ">\n</"
            + PipelineMeta.XML_TAG_ORDER
            + ">\n<"
            + PipelineMeta.XML_TAG_NOTEPADS
            + ">\n</"
            + PipelineMeta.XML_TAG_NOTEPADS
            + ">\n<"
            + PipelineMeta.XML_TAG_TRANSFORM_ERROR_HANDLING
            + ">\n</"
            + PipelineMeta.XML_TAG_TRANSFORM_ERROR_HANDLING
            + ">\n</"
            + PIPELINE_ENVELOPE
            + ">\n");
  }

  public static String wrapWorkflowClipboard(String xml) {
    if (Utils.isEmpty(xml)) {
      return "";
    }
    if (hasOpenTag(xml, WORKFLOW_ENVELOPE)) {
      return withXmlHeader(xml);
    }
    String actionsBlock =
        hasOpenTag(xml, WorkflowMeta.XML_TAG_ACTIONS)
            ? xml
            : "<"
                + WorkflowMeta.XML_TAG_ACTIONS
                + ">\n"
                + (hasOpenTag(xml, ActionMeta.XML_TAG) ? xml : wrapTag(ActionMeta.XML_TAG, xml))
                + "</"
                + WorkflowMeta.XML_TAG_ACTIONS
                + ">\n";
    return withXmlHeader(
        "<"
            + WORKFLOW_ENVELOPE
            + ">\n"
            + actionsBlock
            + "<"
            + WorkflowMeta.XML_TAG_HOPS
            + ">\n</"
            + WorkflowMeta.XML_TAG_HOPS
            + ">\n<"
            + WorkflowMeta.XML_TAG_NOTEPADS
            + ">\n</"
            + WorkflowMeta.XML_TAG_NOTEPADS
            + ">\n</"
            + WORKFLOW_ENVELOPE
            + ">\n");
  }

  public static List<Node> transformNodes(String xml) throws HopException {
    return childNodes(
        wrapPipelineClipboard(xml), PIPELINE_ENVELOPE, PIPELINE_TRANSFORMS, TransformMeta.XML_TAG);
  }

  public static List<Node> actionNodes(String xml) throws HopException {
    return childNodes(
        wrapWorkflowClipboard(xml),
        WORKFLOW_ENVELOPE,
        WorkflowMeta.XML_TAG_ACTIONS,
        ActionMeta.XML_TAG);
  }

  public static List<String> transformPluginIds(String xml) throws HopException {
    return pluginIds(transformNodes(xml));
  }

  public static List<String> actionPluginIds(String xml) throws HopException {
    return pluginIds(actionNodes(xml));
  }

  public static TransformMeta parseFirstTransform(String xml, IHopMetadataProvider metadataProvider)
      throws HopException {
    List<Node> nodes = transformNodes(xml);
    if (nodes.isEmpty()) {
      throw new HopException("No <transform> element in xml");
    }
    try {
      return new TransformMeta(nodes.get(0), metadataProvider);
    } catch (Exception e) {
      throw new HopException("Unable to parse transform XML", e);
    }
  }

  public static ActionMeta parseFirstAction(
      String xml, IHopMetadataProvider metadataProvider, IVariables variables) throws HopException {
    List<Node> nodes = actionNodes(xml);
    if (nodes.isEmpty()) {
      throw new HopException("No <action> element in xml");
    }
    try {
      return new ActionMeta(nodes.get(0), metadataProvider, variables);
    } catch (Exception e) {
      throw new HopException("Unable to parse action XML", e);
    }
  }

  public static String validatePipelineXml(String xml) {
    return validatePluginXml(xml, true);
  }

  public static String validateWorkflowXml(String xml) {
    return validatePluginXml(xml, false);
  }

  private static String validatePluginXml(String xml, boolean pipeline) {
    if (Utils.isEmpty(xml)) {
      return "xml parameter is required";
    }
    if (xml.length() > MAX_XML_CHARS) {
      return "xml exceeds " + MAX_XML_CHARS + " characters";
    }
    try {
      List<String> ids = pipeline ? transformPluginIds(xml) : actionPluginIds(xml);
      if (ids.isEmpty()) {
        return pipeline ? "No <transform> element in xml" : "No <action> element in xml";
      }
      PluginRegistry registry = PluginRegistry.getInstance();
      for (String id : ids) {
        if (Utils.isEmpty(id)) {
          return "Plugin id (type) is missing in xml";
        }
        boolean known =
            pipeline
                ? registry.findPluginWithId(TransformPluginType.class, id) != null
                : registry.findPluginWithId(ActionPluginType.class, id) != null;
        if (!known) {
          return "Unknown plugin in xml: " + id;
        }
      }
    } catch (Exception e) {
      return "Invalid XML: " + Const.NVL(e.getMessage(), e.getClass().getSimpleName());
    }
    return null;
  }

  static List<Node> childNodes(String xml, String envelope, String parentTag, String childTag)
      throws HopException {
    try {
      Document doc = XmlHandler.loadXmlString(xml);
      Node envelopeNode = XmlHandler.getSubNode(doc, envelope);
      if (envelopeNode == null) {
        envelopeNode = doc.getDocumentElement();
      }
      Node parent = XmlHandler.getSubNode(envelopeNode, parentTag);
      if (parent == null) {
        parent = envelopeNode;
      }
      List<Node> nodes = XmlHandler.getNodes(parent, childTag);
      return nodes != null ? nodes : List.of();
    } catch (Exception e) {
      throw new HopException("Unable to parse proposal XML", e);
    }
  }

  static List<String> pluginIds(List<Node> nodes) {
    List<String> ids = new ArrayList<>();
    for (Node node : nodes) {
      ids.add(Const.NVL(XmlHandler.getTagValue(node, "type"), ""));
    }
    return ids;
  }

  static String withXmlHeader(String xml) {
    String trimmed = xml.trim();
    if (trimmed.startsWith("<?xml")) {
      return trimmed;
    }
    return XmlHandler.getXmlHeader() + trimmed;
  }

  static String wrapTag(String tag, String body) {
    return "<" + tag + ">\n" + body + "\n</" + tag + ">\n";
  }

  /**
   * True when {@code xml} contains an element named {@code tag}. Requires a delimiter after the
   * name so {@code <transform} does not match {@code <transforms>}.
   */
  static boolean hasOpenTag(String xml, String tag) {
    if (Utils.isEmpty(xml) || Utils.isEmpty(tag)) {
      return false;
    }
    return xml.contains("<" + tag + ">")
        || xml.contains("<" + tag + " ")
        || xml.contains("<" + tag + "\n")
        || xml.contains("<" + tag + "\r")
        || xml.contains("<" + tag + "\t");
  }
}
