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

package org.apache.hop.lineage.openlineage;

/**
 * Maps a Hop value type ({@link org.apache.hop.core.row.IValueMeta#getTypeDesc()}) onto the
 * lowercase type names OpenLineage consumers understand.
 *
 * <p>Before this existed the schema facet carried the Hop description verbatim ({@code String},
 * {@code Integer}, …), which no consumer recognises: DataHub, for one, maps only {@code string},
 * {@code int}, {@code long}, {@code timestamp} and {@code struct} onto its own schema types and
 * turns everything else into {@code NullType}, so every Hop column arrived untyped.
 *
 * <p>The mapping is deliberately conservative: a type that has no honest OpenLineage equivalent
 * ({@code Serializable}, or a name a plugin invented) is left out rather than guessed, and a {@code
 * Date} stays a {@code date} instead of being widened to a timestamp.
 */
final class OpenLineageFieldTypes {

  private OpenLineageFieldTypes() {}

  /**
   * The OpenLineage type name for a Hop type description, or null when there is no honest mapping.
   */
  static String of(String hopTypeName) {
    if (hopTypeName == null) {
      return null;
    }
    return switch (hopTypeName.trim()) {
      case "String", "Internet Address" -> "string";
      case "Integer" -> "int";
      case "Number" -> "double";
      case "BigNumber" -> "decimal";
      case "Date" -> "date";
      case "Timestamp" -> "timestamp";
      case "Boolean" -> "boolean";
      case "Binary" -> "binary";
        // A JSON column is a document: the closest OpenLineage notion of nested data.
      case "Json" -> "struct";
      default -> null;
    };
  }
}
