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

package org.apache.hop.lineage.model;

/** Relational/JDBC operation for {@link RelationalIoLineagePayload}. */
public enum RelationalIoOperation {
  /** Rows read from one or more tables (e.g. Table Input's {@code SELECT}). */
  READ,
  /** Rows written to a table (e.g. Table Output, Insert/Update). */
  WRITE,
  /**
   * Rows removed from a table (e.g. Delete). The table is affected but no columns are produced, so
   * events of this kind carry neither a column schema nor column provenance.
   */
  DELETE,
  /** Arbitrary SQL whose read/write tables were resolved by parsing (e.g. Execute SQL). */
  EXEC
}
