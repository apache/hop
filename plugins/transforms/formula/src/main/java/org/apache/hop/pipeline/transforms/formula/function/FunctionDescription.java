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

package org.apache.hop.pipeline.transforms.formula.function;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.xml.XmlHandler;
import org.w3c.dom.Node;

@Getter
@Setter
public class FunctionDescription {
  public static final String XML_TAG = "function";
  public static final String CONST_TD = "</td>";

  private String category;
  private String name;
  private String description;
  private String syntax;
  private String returns;
  private String constraints;
  private String semantics;
  private List<FunctionExample> functionExamples;

  /**
   * @param category function category
   * @param name function name
   * @param description function description
   * @param syntax of the function
   * @param returns type of value this function returns
   * @param constraints limitation of this function
   * @param semantics of the functions
   * @param functionExamples examples of how the function can be used
   */
  public FunctionDescription(
      String category,
      String name,
      String description,
      String syntax,
      String returns,
      String constraints,
      String semantics,
      List<FunctionExample> functionExamples) {
    this.category = category;
    this.name = name;
    this.description = description;
    this.syntax = syntax;
    this.returns = returns;
    this.constraints = constraints;
    this.semantics = semantics;
    this.functionExamples = functionExamples;
  }

  public FunctionDescription(Node node) {
    this.category = XmlHandler.getTagValue(node, "category");
    this.name = XmlHandler.getTagValue(node, "name");
    this.description = XmlHandler.getTagValue(node, "description");
    this.syntax = XmlHandler.getTagValue(node, "syntax");
    this.returns = XmlHandler.getTagValue(node, "returns");
    this.constraints = XmlHandler.getTagValue(node, "constraints");
    this.semantics = XmlHandler.getTagValue(node, "semantics");

    this.functionExamples = new ArrayList<>();

    Node examplesNode = XmlHandler.getSubNode(node, "examples");
    int nrExamples = XmlHandler.countNodes(examplesNode, FunctionExample.XML_TAG);
    for (int i = 0; i < nrExamples; i++) {
      Node exampleNode = XmlHandler.getSubNodeByNr(examplesNode, FunctionExample.XML_TAG, i);
      this.functionExamples.add(new FunctionExample(exampleNode));
    }
  }

  /**
   * Create a text version of a report on this function
   *
   * @return an HTML representation of the function description
   */
  public String getHtmlReport() {
    StringBuilder report = new StringBuilder(200);

    // The function name on top
    //
    report.append("<H2>").append(name).append("</H2>").append(Const.CR);

    // Then the description
    //
    report
        .append("<b><u>Description:</u></b> ")
        .append(description)
        .append("<br>")
        .append(Const.CR);

    // Syntax
    //
    if (!Utils.isEmpty(syntax)) {
      report
          .append("<b><u>Syntax:</u></b> <pre>")
          .append(syntax)
          .append("</pre><br>")
          .append(Const.CR);
    }

    // Returns
    //
    if (!Utils.isEmpty(returns)) {
      report.append("<b><u>Returns:</u></b>  ").append(returns).append("<br>").append(Const.CR);
    }

    // Constraints
    //
    if (!Utils.isEmpty(constraints)) {
      report
          .append("<b><u>Constraints:</u></b>  ")
          .append(constraints)
          .append("<br>")
          .append(Const.CR);
    }

    // Semantics
    //
    if (!Utils.isEmpty(semantics)) {
      report.append("<b><u>Semantics:</u></b>  ").append(semantics).append("<br>").append(Const.CR);
    }

    // Examples
    //
    if (!functionExamples.isEmpty()) {
      report.append(Const.CR);
      report.append("<br><b><u>Examples:</u></b><p>  ").append(Const.CR);

      report.append("<table border=\"1\">");

      report.append("<tr>");
      report.append("<th>Expression</th>");
      report.append("<th>Result</th>");
      report.append("<th>Comment</th>");
      report.append("</tr>");

      for (FunctionExample example : functionExamples) {
        // <example><expression>"Hi " &amp; "there"</expression> <result>"Hi there"</result>
        // <level>1</level>
        // <comment>Simple concatenation.</comment></example>

        report.append("<tr>");
        report.append("<td>").append(example.getExpression()).append(CONST_TD);
        report.append("<td>").append(example.getResult()).append(CONST_TD);
        if (!Utils.isEmpty(example.getComment())) {
          report.append("<td>").append(example.getComment()).append(CONST_TD);
        }
        report.append("</tr>");
        report.append(Const.CR);
      }
      report.append("</table>");
    }

    return report.toString();
  }
}
