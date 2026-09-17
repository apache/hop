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

/**
 * pgvector distance operators, together with the expression that turns each one into a comparable
 * similarity score.
 *
 * <p>Every metric yields a <em>higher is better</em> score, so the minimum-score filter means the
 * same thing whichever metric is chosen. Without this normalisation cosine returns a similarity, L2
 * returns a raw distance where lower is better, and pgvector's {@code <#>} returns a negated inner
 * product — three different scales behind one output field.
 */
public enum VectorDistanceMetric {

  /**
   * Cosine similarity in [-1, 1]: {@code 1 - cosine_distance}, where pgvector's cosine distance
   * runs [0, 2]. A minimum score of 0 therefore drops anything more than 90 degrees apart.
   */
  COSINE("<=>", "1 - (embedding <=> ?::vector)"),

  /** Euclidean distance mapped to (0, 1]: {@code 1 / (1 + l2_distance)}. */
  L2("<->", "1 / (1 + (embedding <-> ?::vector))"),

  /** Inner product, un-negated: pgvector's {@code <#>} returns the negative inner product. */
  INNER_PRODUCT("<#>", "(-1 * (embedding <#> ?::vector))");

  private final String operator;
  private final String scoreExpression;

  VectorDistanceMetric(String operator, String scoreExpression) {
    this.operator = operator;
    this.scoreExpression = scoreExpression;
  }

  public String getOperator() {
    return operator;
  }

  /**
   * SQL expression producing the similarity score. Contains one {@code ?} placeholder for the query
   * vector.
   */
  public String getScoreExpression() {
    return scoreExpression;
  }

  public static VectorDistanceMetric fromString(String value) {
    if (value == null || value.isEmpty()) {
      return COSINE;
    }
    for (VectorDistanceMetric metric : values()) {
      if (metric.name().equalsIgnoreCase(value)) {
        return metric;
      }
    }
    return COSINE;
  }
}
