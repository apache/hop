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
package org.apache.hop.pipeline.transforms.util;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.xml.XmlHandler;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

public class JaninoCheckerUtil {
  List<String> matches = new ArrayList<>();

  public JaninoCheckerUtil() {
    String path = "";

    try {
      path = getJarPath() + File.separator + "codeExclusions.xml";

      Document document = XmlHandler.loadXmlFile(path);
      Node exclusionsNode = XmlHandler.getSubNode(document, "exclusions");
      List<Node> exclusionNodes = XmlHandler.getNodes(exclusionsNode, "exclusion");

      for (Node exclusionNode : exclusionNodes) {
        matches.add(exclusionNode.getTextContent());
      }
    } catch (Exception e) {
      LogChannel.GENERAL.logError("Unable to load exclusions from: '" + path + "'", e);
    }
  }

  public List<String> checkCode(String code) {
    List<String> foundBlocks = new ArrayList<>();
    for (String s : matches) {
      if (code.contains(s)) {
        foundBlocks.add(s);
      }
    }
    return foundBlocks;
  }

  public String getJarPath() {
    return new File(
            JaninoCheckerUtil.class.getProtectionDomain().getCodeSource().getLocation().getPath())
        .getParent();
  }
}
