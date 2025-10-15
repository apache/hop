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

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.xml.XmlHandler;
import org.w3c.dom.Node;

@Getter
@Setter
public class FunctionExample {
  public static final String XML_TAG = "example";

  private String expression;
  private String result;
  private String level;
  private String comment;

  public FunctionExample(String expression, String result, String level, String comment) {
    this.expression = expression;
    this.result = result;
    this.level = level;
    this.comment = comment;
  }

  public FunctionExample(Node node) {
    this.expression = XmlHandler.getTagValue(node, "expression");
    this.result = XmlHandler.getTagValue(node, "result");
    this.level = XmlHandler.getTagValue(node, "level");
    this.comment = XmlHandler.getTagValue(node, "comment");
  }
}
