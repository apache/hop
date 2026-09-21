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
package org.apache.hop.pipeline.transforms.chunker.document.hopxml;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.pipeline.transforms.chunker.document.ContentType;
import org.apache.hop.pipeline.transforms.chunker.document.DocumentNode;
import org.apache.hop.pipeline.transforms.chunker.document.DocumentParser;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * Parses Hop pipeline (.hpl) XML into one section per overview, notepad block, transform, and hop
 * graph.
 */
public final class HopPipelineXmlParser implements DocumentParser {

  @Override
  public ContentType getContentType() {
    return ContentType.PIPELINE;
  }

  @Override
  public DocumentNode parse(String text) {
    if (!HopXmlSupport.looksLikeHopXml(text)) {
      return DocumentNode.root(text != null ? text : "");
    }

    Document document = HopXmlSupport.parseDocument(text);
    if (document == null) {
      return DocumentNode.root(text);
    }
    Element root = document.getDocumentElement();
    if (root == null || !"pipeline".equals(root.getTagName())) {
      return DocumentNode.root(text);
    }

    Element info = HopXmlSupport.firstChild(root, "info");
    String pipelineName = info != null ? HopXmlSupport.directChildText(info, "name") : "";
    List<DocumentNode> children = new ArrayList<>();

    StringBuilder overview = new StringBuilder();
    if (!pipelineName.isEmpty()) {
      overview.append("Hop pipeline '").append(pipelineName).append("'\n");
    }
    if (info != null) {
      appendIfPresent(overview, "Description", HopXmlSupport.directChildText(info, "description"));
      appendIfPresent(
          overview,
          "Extended description",
          HopXmlSupport.directChildText(info, "extended_description"));
      String params = HopXmlSupport.formatParameters(HopXmlSupport.firstChild(info, "parameters"));
      appendIfPresent(overview, "Parameters", params);
    }
    if (overview.length() > 0) {
      children.add(DocumentNode.leaf("Overview", overview.toString().strip(), 0));
    }

    String notes = HopXmlSupport.formatNotepads(root, "notepads");
    if (!notes.isEmpty()) {
      children.add(DocumentNode.leaf("Notes", notes, 0));
    }

    for (Element transform : HopXmlSupport.children(root, "transform")) {
      String name = HopXmlSupport.directChildText(transform, "name");
      String type = HopXmlSupport.directChildText(transform, "type");
      String title = "Transform: " + name + " (" + type + ")";
      String body = HopXmlSupport.serializeConfig(transform);
      if (!body.isEmpty()) {
        children.add(DocumentNode.leaf(title, body, 0));
      }
    }

    Element order = HopXmlSupport.firstChild(root, "order");
    if (order != null) {
      StringBuilder hops = new StringBuilder();
      for (Element hop : HopXmlSupport.children(order, "hop")) {
        String from = HopXmlSupport.directChildText(hop, "from");
        String to = HopXmlSupport.directChildText(hop, "to");
        String enabled = HopXmlSupport.directChildText(hop, "enabled");
        if (!from.isEmpty() && !to.isEmpty() && !"N".equalsIgnoreCase(enabled)) {
          hops.append("- ").append(from).append(" → ").append(to).append('\n');
        }
      }
      if (hops.length() > 0) {
        children.add(DocumentNode.leaf("Data flow", hops.toString().strip(), 0));
      }
    }

    if (children.isEmpty()) {
      return DocumentNode.root(text);
    }
    return new DocumentNode(pipelineName, "", 0, children);
  }

  private static void appendIfPresent(StringBuilder sb, String label, String value) {
    if (value != null && !value.isBlank()) {
      if (sb.length() > 0) {
        sb.append('\n');
      }
      sb.append(label).append(":\n").append(value.strip());
    }
  }
}
