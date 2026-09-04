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

package org.apache.hop.pipeline.transforms.janino.function;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.xml.XmlHandler;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

/**
 * The reference the expression editor shows next to the fields of the stream: the operators and
 * methods a Java expression can use, and, for the Java Filter, a set of ready to use conditions.
 *
 * <p>Both are read from XML in the same shape the Formula transform uses for its function library,
 * and are handed to the editor as {@link FunctionDescription}s: the name in the tree, the
 * description and the examples in the panel next to it, and the syntax inserted in the editor on a
 * double click.
 *
 * <p>Every syntax and every example in these files is compiled by {@code ExpressionLibraryTest}, so
 * the reference can not drift away from what the transforms accept.
 */
public class ExpressionLibrary {

  /** The operators and methods that can be used in a Janino expression. */
  public static final String FUNCTIONS_FILE_NAME =
      "org/apache/hop/pipeline/transforms/janino/function/expressionFunctions.xml";

  /** Complete conditions for the Java Filter, they all return a boolean. */
  public static final String CONDITION_EXAMPLES_FILE_NAME =
      "org/apache/hop/pipeline/transforms/javafilter/conditionExamples.xml";

  private ExpressionLibrary() {}

  /**
   * The operators and methods a Java expression can use, for the Java Filter as well as the User
   * Defined Java Expression transform.
   *
   * @return the reference entries, grouped in the categories the editor shows them under
   * @throws HopException when the file can not be read
   */
  public static List<FunctionDescription> getFunctions() throws HopException {
    return load(FUNCTIONS_FILE_NAME);
  }

  /**
   * The example conditions of the Java Filter, on top of {@link #getFunctions()}.
   *
   * @return the examples, grouped in the categories the editor shows them under
   * @throws HopException when the file can not be read
   */
  public static List<FunctionDescription> getConditionExamples() throws HopException {
    return load(CONDITION_EXAMPLES_FILE_NAME);
  }

  /** The reference and the example conditions together, in the order the editor shows them. */
  public static List<FunctionDescription> getFunctionsAndConditionExamples() throws HopException {
    List<FunctionDescription> all = new ArrayList<>(getFunctions());
    all.addAll(getConditionExamples());
    return all;
  }

  static List<FunctionDescription> load(String resourceName) throws HopException {
    try (InputStream inputStream =
        ExpressionLibrary.class.getClassLoader().getResourceAsStream(resourceName)) {

      if (inputStream == null) {
        return Collections.emptyList();
      }

      Document document = XmlHandler.loadXmlFile(inputStream, null, false, false);
      List<FunctionDescription> functions = new ArrayList<>();

      Node functionsNode = XmlHandler.getSubNode(document, "functions");
      for (Node functionNode : XmlHandler.getNodes(functionsNode, "function")) {
        functions.add(
            new FunctionDescription(
                XmlHandler.getTagValue(functionNode, "category"),
                XmlHandler.getTagValue(functionNode, "name"),
                XmlHandler.getTagValue(functionNode, "description"),
                XmlHandler.getTagValue(functionNode, "syntax"),
                XmlHandler.getTagValue(functionNode, "returns"),
                XmlHandler.getTagValue(functionNode, "constraints"),
                XmlHandler.getTagValue(functionNode, "semantics"),
                null,
                readExamples(functionNode)));
      }
      return functions;
    } catch (Exception e) {
      throw new HopException("Unable to read the expression library from " + resourceName, e);
    }
  }

  private static List<FunctionExample> readExamples(Node functionNode) {
    List<FunctionExample> examples = new ArrayList<>();

    Node examplesNode = XmlHandler.getSubNode(functionNode, "examples");
    if (examplesNode == null) {
      return examples;
    }

    for (Node exampleNode : XmlHandler.getNodes(examplesNode, "example")) {
      examples.add(
          new FunctionExample(
              XmlHandler.getTagValue(exampleNode, "expression"),
              XmlHandler.getTagValue(exampleNode, "result"),
              XmlHandler.getTagValue(exampleNode, "level"),
              XmlHandler.getTagValue(exampleNode, "comment")));
    }
    return examples;
  }
}
