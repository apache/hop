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

package org.apache.hop.pipeline.transforms.xml.advancedxmloutput;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;

/**
 * Static structural and semantic validation of an {@link XmlNode} tree against the upstream row
 * metadata. Returns a list of human-readable error messages (empty when the tree is valid).
 */
final class AdvancedXmlOutputValidator {

  private AdvancedXmlOutputValidator() {}

  static List<String> validate(XmlNode root, IRowMeta inputRowMeta) {
    List<String> errors = new ArrayList<>();
    if (root == null) {
      errors.add("XML tree is empty - configure the XML tree first.");
      return errors;
    }
    if (root.getKind() != XmlNode.NodeKind.Element) {
      errors.add("Root node must be an element.");
    }
    if (Utils.isEmpty(root.getName())) {
      errors.add("Root element must have a name.");
    }

    int[] loopCount = new int[1];
    walk(root, null, loopCount, inputRowMeta, errors);

    if (loopCount[0] == 0) {
      errors.add(
          "No loop element configured. Mark exactly one element in the XML tree as the loop.");
    } else if (loopCount[0] > 1) {
      errors.add(
          "Multiple loop elements configured ("
              + loopCount[0]
              + "). Exactly one element in the XML tree may be the loop.");
    }

    return errors;
  }

  private static void walk(
      XmlNode node, XmlNode parent, int[] loopCount, IRowMeta inputRowMeta, List<String> errors) {

    if (Utils.isEmpty(node.getName())) {
      errors.add(
          "Node has no name (parent: " + (parent == null ? "<root>" : parent.getName()) + ")");
    }

    if (node.isLoop()) {
      loopCount[0]++;
      if (node.getKind() != XmlNode.NodeKind.Element) {
        errors.add("Loop node '" + node.getName() + "' must be an element.");
      }
    }

    if (node.isGroupBy() && (node.getMappedField() == null || node.getMappedField().isEmpty())) {
      errors.add(
          "Group-by node '" + node.getName() + "' must reference an input field as its key.");
    }

    if (!Utils.isEmpty(node.getMappedField())
        && inputRowMeta != null
        && inputRowMeta.indexOfValue(node.getMappedField()) < 0) {
      errors.add(
          "Node '"
              + node.getName()
              + "' references unknown input field '"
              + node.getMappedField()
              + "'.");
    }

    if (node.getChildren() != null) {
      // Attributes must come from an Element parent
      if (node.getKind() != XmlNode.NodeKind.Element && !node.getChildren().isEmpty()) {
        errors.add(
            "Non-element node '"
                + node.getName()
                + "' cannot have children (kind="
                + node.getKind()
                + ").");
      }
      for (XmlNode child : node.getChildren()) {
        walk(child, node, loopCount, inputRowMeta, errors);
      }
    }
  }
}
