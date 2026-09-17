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
package org.apache.hop.pgvector.util;

/** Parses embedding vectors stored as JSON float arrays in Hop row fields. */
public final class EmbeddingJsonParser {

  private EmbeddingJsonParser() {}

  public static float[] parse(String json) {
    if (json == null || json.isBlank()) {
      return new float[0];
    }
    String trimmed = json.trim();
    if (!trimmed.startsWith("[") || !trimmed.endsWith("]")) {
      throw new IllegalArgumentException("Embedding must be a JSON array: " + json);
    }
    String inner = trimmed.substring(1, trimmed.length() - 1).trim();
    if (inner.isEmpty()) {
      return new float[0];
    }
    String[] parts = inner.split(",");
    float[] vector = new float[parts.length];
    for (int i = 0; i < parts.length; i++) {
      vector[i] = Float.parseFloat(parts[i].trim());
    }
    return vector;
  }

  /**
   * True when a value read straight out of the row carries no vector to store or search with.
   *
   * @param value the raw row value
   * @return true for null, an empty array, or blank text
   */
  public static boolean isEmpty(Object value) {
    if (value == null) {
      return true;
    }
    if (value instanceof float[] vector) {
      return vector.length == 0;
    }
    if (value instanceof double[] vector) {
      return vector.length == 0;
    }
    return String.valueOf(value).isBlank();
  }

  /**
   * pgvector text input for a value read straight out of the row. A field of the Vector value type
   * arrives as a {@code float[]} and is rendered directly; anything else falls back to its text
   * form, so a String field holding a JSON array keeps working unchanged.
   *
   * @param value the raw row value
   * @return the pgvector literal
   */
  public static String toPgVectorLiteral(Object value) {
    if (value == null) {
      return "[]";
    }
    if (value instanceof float[] vector) {
      return toPgVectorLiteral(vector);
    }
    if (value instanceof double[] vector) {
      float[] floats = new float[vector.length];
      for (int i = 0; i < vector.length; i++) {
        floats[i] = (float) vector[i];
      }
      return toPgVectorLiteral(floats);
    }
    // Parsing and re-rendering rather than passing the text through: it validates the literal
    // before it reaches the database, where a malformed one is a much less obvious error.
    return toPgVectorLiteral(String.valueOf(value));
  }

  /** pgvector text input format, e.g. {@code [0.1,0.2,0.3]}. */
  public static String toPgVectorLiteral(String json) {
    float[] vector = parse(json);
    return toPgVectorLiteral(vector);
  }

  public static String toPgVectorLiteral(float[] vector) {
    if (vector == null || vector.length == 0) {
      return "[]";
    }
    StringBuilder literal = new StringBuilder(vector.length * 8);
    literal.append('[');
    for (int i = 0; i < vector.length; i++) {
      if (i > 0) {
        literal.append(',');
      }
      literal.append(vector[i]);
    }
    literal.append(']');
    return literal.toString();
  }
}
