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

package org.apache.hop.openformula.parser;

/** Thrown when formula syntax parsing fails. */
public class ParseException extends Exception {
  private static final long serialVersionUID = 1L;

  private final int line;
  private final int column;

  public ParseException(String message) {
    this(message, -1, -1);
  }

  public ParseException(String message, int line, int column) {
    super(message + (line >= 0 ? " (at line " + line + ", column " + column + ")" : ""));
    this.line = line;
    this.column = column;
  }

  public ParseException(String message, Throwable cause) {
    super(message, cause);
    this.line = -1;
    this.column = -1;
  }

  public int getLine() {
    return line;
  }

  public int getColumn() {
    return column;
  }
}
