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

package org.apache.hop.lineage;

import java.util.List;
import org.apache.hop.core.util.Utils;
import org.apache.hop.lineage.model.RelationalTable;

/**
 * The tables a SQL statement reads and writes, for code outside a lineage sink that needs them,
 * such as a catalog lookup of the tables a Table input queries. Hop itself doesn't parse SQL: a
 * plugin that can registers a parser, the OpenLineage plugin does. Without one the tables of a
 * statement stay unknown.
 */
public final class LineageSqlTables {

  /** The tables of a statement, as the statement names them. */
  public record Tables(List<RelationalTable> inputs, List<RelationalTable> outputs) {
    public static final Tables EMPTY = new Tables(List.of(), List.of());
  }

  /** Parses a statement; returns {@link Tables#EMPTY} when it can't. */
  @FunctionalInterface
  public interface IParser {
    /**
     * @param sql the statement
     * @param datasetNamespace the namespace of the connection it runs on, such as {@code
     *     postgres://db:5432}, for the dialect
     */
    Tables parse(String sql, String datasetNamespace);
  }

  private static volatile IParser parser;

  private LineageSqlTables() {}

  /** Sets the parser; null removes it. */
  public static void register(IParser sqlParser) {
    parser = sqlParser;
  }

  /** The tables of a statement; empty when no parser is registered or it can't parse it. */
  public static Tables parse(String sql, String datasetNamespace) {
    IParser current = parser;
    if (current == null || Utils.isEmpty(sql)) {
      return Tables.EMPTY;
    }
    try {
      Tables tables = current.parse(sql, datasetNamespace);
      return tables == null ? Tables.EMPTY : tables;
    } catch (Exception | LinkageError e) {
      return Tables.EMPTY;
    }
  }
}
