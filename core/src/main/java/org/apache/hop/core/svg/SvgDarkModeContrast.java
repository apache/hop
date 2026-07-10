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

package org.apache.hop.core.svg;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.batik.anim.dom.SVGDOMImplementation;
import org.apache.batik.dom.util.DOMUtilities;
import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.svg.SVGDocument;
import org.w3c.dom.svg.SVGSVGElement;

/**
 * Applies Hop dark-mode color contrasting to SVG icon documents, matching the logic used by {@code
 * SwtUniversalImageSvg} on the desktop client.
 */
public final class SvgDarkModeContrast {

  private static final List<String> CONTRAST_TAGS =
      Arrays.asList(
          "path",
          "fill",
          "bordercolor",
          "fillcolor",
          "style",
          "text",
          "polygon",
          "rect",
          "circle",
          "ellipse",
          "stop",
          "tspan",
          "polyline",
          "mask");

  private SvgDarkModeContrast() {}

  /**
   * Deep-clones {@code source} and replaces known light-theme color tokens in SVG attributes.
   *
   * @param source original SVG document (not modified)
   * @param colorsMap lower/upper-case hex color replacements from {@code
   *     PropsUi#getContrastingColorStrings()}
   */
  public static SVGDocument cloneWithContrast(SVGDocument source, Map<String, String> colorsMap) {
    DOMImplementation domImplementation = SVGDOMImplementation.getDOMImplementation();
    SVGDocument cloned = (SVGDocument) DOMUtilities.deepCloneDocument(source, domImplementation);
    applyContrast(cloned.getRootElement(), colorsMap);
    return cloned;
  }

  public static void applyContrast(SVGSVGElement root, Map<String, String> colorsMap) {
    if (root == null || colorsMap == null || colorsMap.isEmpty()) {
      return;
    }
    for (String tag : CONTRAST_TAGS) {
      NodeList nodeList = root.getElementsByTagName(tag);
      for (int i = 0; i < nodeList.getLength(); i++) {
        Node node = nodeList.item(i);
        NamedNodeMap namedNodeMap = node.getAttributes();
        for (int x = 0; x < namedNodeMap.getLength(); x++) {
          Node namedNode = namedNodeMap.item(x);
          String value = namedNode.getNodeValue();
          if (StringUtils.isEmpty(value)) {
            continue;
          }
          String changedValue = value.toLowerCase();
          Map<String, String> detectedColors = new HashMap<>();
          for (Map.Entry<String, String> entry : colorsMap.entrySet()) {
            String oldColor = entry.getKey();
            if (changedValue.contains(oldColor)) {
              detectedColors.put(oldColor, entry.getValue());
            }
          }
          if (!detectedColors.isEmpty()) {
            for (Map.Entry<String, String> replacement : detectedColors.entrySet()) {
              changedValue = changedValue.replace(replacement.getKey(), replacement.getValue());
            }
            namedNode.setNodeValue(changedValue);
          }
        }
      }
    }
  }
}
