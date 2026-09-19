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

package org.apache.hop.databases.cockroachdb;

import java.util.List;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.types.IDatabaseTypeRule;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.databases.postgresql.PostgreSqlDatabaseMeta;

/** Contains PostgreSQL specific information through static final members */
@DatabaseMetaPlugin(
    type = "COCKROACHDB",
    typeDescription = "CockroachDB",
    image = "cockroachdb.svg",
    documentationUrl = "/database/databases/postgresql.html",
    classLoaderGroup = "cockroachdb-db")
@GuiPlugin(id = "GUI-CockroachDatabaseMeta")
public class CockroachDatabaseMeta extends PostgreSqlDatabaseMeta implements IDatabase {

  /**
   * CockroachDB has a VECTOR type of its own since 24.2, spelled like pgvector's but not installed
   * as an extension, so the check the PostgreSQL dialect makes for one does not describe it. It has
   * not been verified against a server here - in particular whether it takes a column declared
   * without a dimension, which pgvector does - so it keeps the PostgreSQL rules without the vector
   * ones and writes a vector as text, which is what it did before.
   */
  @Override
  public List<IDatabaseTypeRule> getTypeRules() {
    return PostgreSqlDatabaseMeta.POSTGRES_BASE_TYPE_RULES;
  }
}
