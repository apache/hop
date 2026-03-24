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
package org.apache.hop.pipeline.transforms.userdefinedjavaclass;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

@Getter
@Setter
public class UserDefinedJavaClassCodeSnippets {
  private static final Class<?> PKG = UserDefinedJavaClass.class;
  private static UserDefinedJavaClassCodeSnippets snippetsHelper = null;

  private final List<Snippet> snippets = new ArrayList<>();
  private final Map<String, Snippet> snippetsMap = new HashMap<>();
  private final LogChannel log = new LogChannel("UserDefinedJavaClassCodeSnippets");

  public static synchronized UserDefinedJavaClassCodeSnippets getSnippetsHelper()
      throws HopXmlException {
    if (snippetsHelper == null) {
      snippetsHelper = new UserDefinedJavaClassCodeSnippets();
      snippetsHelper.addSnippets(
          "org/apache/hop/pipeline/transforms/userdefinedjavaclass/codeSnippets.xml");
    }
    return snippetsHelper;
  }

  private UserDefinedJavaClassCodeSnippets() {}

  public void addSnippets(String strFileName) throws HopXmlException {
    Document doc =
        XmlHandler.loadXmlFile(
            UserDefinedJavaClassCodeSnippets.class
                .getClassLoader()
                .getResourceAsStream(strFileName),
            null,
            false,
            false);
    buildSnippetList(doc);
  }

  @Getter
  public enum Category {
    COMMON(BaseMessages.getString(PKG, "UserDefinedJavaClassCodeSnippits.categories.COMMON")),
    STATUS(BaseMessages.getString(PKG, "UserDefinedJavaClassCodeSnippits.categories.STATUS")),
    LOGGING(BaseMessages.getString(PKG, "UserDefinedJavaClassCodeSnippits.categories.LOGGING")),
    LISTENERS(BaseMessages.getString(PKG, "UserDefinedJavaClassCodeSnippits.categories.LISTENERS")),
    ROW(BaseMessages.getString(PKG, "UserDefinedJavaClassCodeSnippits.categories.ROW")),
    OTHER(BaseMessages.getString(PKG, "UserDefinedJavaClassCodeSnippits.categories.OTHER"));

    private final String description;

    Category(String description) {
      this.description = description;
    }

    @Override
    public String toString() {
      return description;
    }
  }

  @Getter
  @Setter
  public static class Snippet {
    private Snippet(Category category, String name, String sample, String code) {
      this.category = category;
      this.name = name;
      this.sample = sample;
      this.code = code;
    }

    private final Category category;
    private final String name;
    private final String sample;
    private final String code;
  }

  public List<Snippet> getSnippets() {
    return Collections.unmodifiableList(snippets);
  }

  public String getDefaultCode() {
    return getCode("Implement processRow");
  }

  public String getCode(String snippetName) {
    Snippet snippet = snippetsMap.get(snippetName);
    return (snippet == null) ? "" : snippet.code;
  }

  public String getSample(String snippetName) {
    Snippet snippet = snippetsMap.get(snippetName);
    return (snippet == null) ? "" : snippet.sample;
  }

  private void buildSnippetList(Document doc) {
    List<Node> nodes =
        XmlHandler.getNodes(XmlHandler.getSubNode(doc, "codeSnippets"), "codeSnippet");
    for (Node node : nodes) {
      Snippet snippet =
          new Snippet(
              Category.valueOf(XmlHandler.getTagValue(node, "category")),
              XmlHandler.getTagValue(node, "name"),
              XmlHandler.getTagValue(node, "sample"),
              XmlHandler.getTagValue(node, "code"));
      snippets.add(snippet);
      Snippet oldSnippet = snippetsMap.put(snippet.name, snippet);
      if (oldSnippet != null) {
        log.logError("Multiple code snippets for name: " + snippet.name);
      }
    }
  }
}
