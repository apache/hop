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

public class HopFunctions {

  private HopFunctions() {}

  @JaninoFunction(
      name = "nvl",
      category = "General",
      description = "Implements Oracle style NVL function.",
      syntax = "HopFunctions.nvl(source, def)",
      returns = "String",
      semantics = "If source == null or length == 0 return def",
      examples =
          """
    [
      {
        "expression": "HopFunctions.nvl(\\"\\", \\"bar\\")",
        "result": "bar",
        "level": "1",
        "comment": "Empty string returns bar."
      },
      {
        "expression": "HopFunctions.nvl(null, \\"bar\\")",
        "result": "bar",
        "level": "1",
        "comment": "null string returns bar."
      },
      {
        "expression": "HopFunctions.nvl(\\"foo\\", \\"bar\\")",
        "result": "foo",
        "level": "1",
        "comment": "foo string returns foo."
      }
    ]
    """)
  public static String nvl(String source, String def) {
    if (source == null || source.isEmpty()) {
      return def;
    }
    return source;
  }
}
