/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.core.widget.highlight;

import java.util.List;

public enum ScriptEngine {
  PYTHON(
      List.of(
          "False",
          "None",
          "True",
          "and",
          "as",
          "assert",
          "async",
          "await",
          "break",
          "class",
          "continue",
          "def",
          "del",
          "elif",
          "else",
          "except",
          "finally",
          "for",
          "from",
          "global",
          "if",
          "import",
          "in",
          "is",
          "lambda",
          "nonlocal",
          "not",
          "or",
          "pass",
          "raise",
          "return",
          "try",
          "while",
          "with",
          "yield"),
      List.of(
          "abs",
          "all",
          "any",
          "ascii",
          "bin",
          "bool",
          "breakpoint",
          "bytearray",
          "bytes",
          "callable",
          "chr",
          "classmethod",
          "compile",
          "complex",
          "delattr",
          "dict",
          "dir",
          "divmod",
          "enumerate",
          "eval",
          "exec",
          "filter",
          "float",
          "format",
          "frozenset",
          "getattr",
          "globals",
          "hasattr",
          "hash",
          "help",
          "hex",
          "id",
          "input",
          "int",
          "isinstance",
          "issubclass",
          "iter",
          "len",
          "list",
          "locals",
          "map",
          "max",
          "memoryview",
          "min",
          "next",
          "object",
          "oct",
          "open",
          "ord",
          "pow",
          "print",
          "property",
          "range",
          "repr",
          "reversed",
          "round",
          "set",
          "setattr",
          "slice",
          "sorted",
          "staticmethod",
          "str",
          "sum",
          "super",
          "tuple",
          "type",
          "vars",
          "zip",
          "__import__")),
  GROOVY(
      List.of(
          "as",
          "assert",
          "break",
          "case",
          "catch",
          "class",
          "const",
          "continue",
          "def",
          "default",
          "do",
          "else",
          "enum",
          "extends",
          "false",
          "finally",
          "for",
          "goto",
          "if",
          "implements",
          "import",
          "in",
          "instanceof",
          "interface",
          "new",
          "null",
          "package",
          "return",
          "super",
          "switch",
          "this",
          "throw",
          "throws",
          "trait",
          "true",
          "try",
          "while"),
      List.of(
          "abs",
          "any",
          "append",
          "asBoolean",
          "asType",
          "collect",
          "complement",
          "count",
          "div",
          "dump",
          "each",
          "eachWithIndex",
          "every",
          "find",
          "findAll",
          "flatten",
          "getAt",
          "getMetaClass",
          "grep",
          "immutable",
          "inject",
          "intersect",
          "invokeMethod",
          "isCase",
          "join",
          "leftShift",
          "minus",
          "plus",
          "pop",
          "power",
          "previous",
          "print",
          "println",
          "push",
          "putAt",
          "remove",
          "reverse",
          "reverseEach",
          "rightShift",
          "size",
          "sort",
          "split",
          "sprintf",
          "step",
          "subMap",
          "take",
          "times",
          "toList",
          "toSet",
          "toSpreadMap",
          "toString",
          "transformChar",
          "traverse",
          "values",
          "with",
          "withPrintWriter",
          "withReader",
          "withStream",
          "withWriter"));

  private final List<String> keywords;

  private final List<String> builtInFunctions;

  ScriptEngine(List<String> keywords, List<String> builtInFunctions) {
    this.keywords = keywords;
    this.builtInFunctions = builtInFunctions;
  }

  public List<String> getKeywords() {
    return keywords;
  }

  public List<String> getBuiltInFunctions() {
    return builtInFunctions;
  }

  public static ScriptEngine fromString(String input) {
    if (input == null) {
      return null;
    }
    try {
      return ScriptEngine.valueOf(input.toUpperCase());
    } catch (IllegalArgumentException e) {
      return null; // or throw an exception if you prefer
    }
  }
}
