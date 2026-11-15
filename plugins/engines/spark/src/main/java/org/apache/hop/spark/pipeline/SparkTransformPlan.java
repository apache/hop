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

package org.apache.hop.spark.pipeline;

import org.apache.hop.i18n.BaseMessages;

/**
 * How a transform will run on the native Spark engine, as the converter decides it before a run.
 *
 * @param kind native, a Hop transform, part of a fused Hop chain, on one stream or on the driver
 * @param reason why, for a person: the chain it runs in, or why a transform with a Spark version
 *     runs as Hop's own; null when there is nothing to add
 */
public record SparkTransformPlan(Kind kind, String reason) {

  private static final Class<?> PKG = SparkTransformPlan.class;

  /** Spark code, nothing to add. */
  public static final SparkTransformPlan SPARK_CODE = new SparkTransformPlan(Kind.NATIVE, null);

  /** A plan whose reason is a message of this package. */
  public static SparkTransformPlan of(Kind kind, String reasonKey, String... parameters) {
    return new SparkTransformPlan(kind, message(reasonKey, parameters));
  }

  static String message(String key, String... parameters) {
    return BaseMessages.getString(PKG, key, parameters);
  }

  /** The reasons that are there, one sentence after the other. */
  static String join(String reason, String more) {
    if (reason == null) {
      return more;
    }
    return more == null ? reason : reason + " " + more;
  }

  public enum Kind {
    /** Spark code. */
    NATIVE,
    /** Hop's transform on every partition: rows converted from Spark and back. */
    HOP,
    /** Hop's transform in a chain of Hop transforms: one conversion for the chain. */
    HOP_FUSED,
    /** Hop's transform on all rows in one ordered stream: one task, no parallelism. */
    ONE_STREAM,
    /** Hop's transform once on the Spark driver, all rows collected there (Spark Run Mode). */
    DRIVER
  }
}
