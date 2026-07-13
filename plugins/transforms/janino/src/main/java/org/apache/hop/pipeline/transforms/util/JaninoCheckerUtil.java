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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.xml.XmlHandler;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

/** Utility that checks Janino user code against a configurable deny-list of substrings. */
public class JaninoCheckerUtil {
  private static final String EXCLUSIONS_FILE_NAME = "codeExclusions.xml";

  List<String> matches = new ArrayList<>();

  /**
   * Loads exclusion patterns from {@value #EXCLUSIONS_FILE_NAME}. An external file next to the
   * plugin jar is tried first (Hop installation layout); if absent, the bundled classpath resource
   * is used (Maven, Spring Boot, and nested jar deployments).
   */
  public JaninoCheckerUtil() {
    if (!loadExclusionsFromExternalFile()) {
      loadExclusionsFromClasspath();
    }
  }

  /**
   * Returns exclusion patterns found as substrings in the given source code.
   *
   * @param code Janino expression or Java class source to validate
   * @return matched exclusion strings, empty when none apply
   */
  public List<String> checkCode(String code) {
    List<String> foundBlocks = new ArrayList<>();
    for (String s : matches) {
      if (code.contains(s)) {
        foundBlocks.add(s);
      }
    }
    return foundBlocks;
  }

  /**
   * Returns the directory containing the janino plugin jar (parent of the code source location).
   *
   * @return absolute path to the jar directory
   */
  public String getJarPath() {
    return new File(
            JaninoCheckerUtil.class.getProtectionDomain().getCodeSource().getLocation().getPath())
        .getParent();
  }

  /** Path to an optional external {@value #EXCLUSIONS_FILE_NAME} beside the plugin jar. */
  private String getExternalExclusionsPath() {
    return getJarPath() + File.separator + EXCLUSIONS_FILE_NAME;
  }

  /**
   * Loads exclusions from a filesystem file when present (custom Hop installation config).
   *
   * @return {@code true} when a regular file was found and parsed successfully
   */
  private boolean loadExclusionsFromExternalFile() {
    String path = getExternalExclusionsPath();
    File file = new File(path);
    if (!file.isFile()) {
      return false;
    }
    try {
      parseExclusions(XmlHandler.loadXmlFile(path));
      return true;
    } catch (Exception e) {
      LogChannel.GENERAL.logError("Unable to load exclusions from: '" + path + "'", e);
      return false;
    }
  }

  /** Loads the bundled {@value #EXCLUSIONS_FILE_NAME} from the plugin classpath. */
  private void loadExclusionsFromClasspath() {
    try (InputStream is = JaninoCheckerUtil.class.getResourceAsStream("/" + EXCLUSIONS_FILE_NAME)) {
      if (is == null) {
        return;
      }

      parseExclusions(XmlHandler.loadXmlFile(is));
    } catch (Exception e) {
      LogChannel.GENERAL.logDebug(
          "Unable to load default exclusions from classpath: " + EXCLUSIONS_FILE_NAME, e);
    }
  }

  /** Parses {@code <exclusion>} entries from a loaded exclusions document into {@link #matches}. */
  private void parseExclusions(Document document) throws HopXmlException {
    if (document == null) {
      throw new HopXmlException("Exclusions document is null");
    }

    Node exclusionsNode = XmlHandler.getSubNode(document, "exclusions");
    List<Node> exclusionNodes = XmlHandler.getNodes(exclusionsNode, "exclusion");
    for (Node exclusionNode : exclusionNodes) {
      matches.add(exclusionNode.getTextContent());
    }
  }
}
