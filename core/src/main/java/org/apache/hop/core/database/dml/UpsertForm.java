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

package org.apache.hop.core.database.dml;

import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;

/** The statement an upsert is written as. */
public enum UpsertForm implements IEnumHasCodeAndDescription {
  /**
   * Standard SQL: {@code MERGE INTO t USING (source) s ON (keys) WHEN MATCHED THEN UPDATE ... WHEN
   * NOT MATCHED THEN INSERT ...}. Needs no index on the key columns.
   */
  MERGE,
  /**
   * PostgreSQL, SQLite, DuckDB: {@code INSERT ... ON CONFLICT (keys) DO UPDATE SET c = EXCLUDED.c}.
   * Needs a unique index or primary key on exactly the key columns.
   */
  ON_CONFLICT,
  /**
   * MySQL, MariaDB: {@code INSERT ... ON DUPLICATE KEY UPDATE c = VALUES(c)}. Reacts to every
   * unique index of the table, so it needs one on exactly the key columns and no other.
   */
  ON_DUPLICATE_KEY;

  private static final Class<?> PKG = UpsertForm.class;

  @Override
  public String getCode() {
    return name();
  }

  @Override
  public String getDescription() {
    return BaseMessages.getString(PKG, "UpsertForm." + name() + ".Description");
  }

  /**
   * Finds a form by its code, ignoring case.
   *
   * @return the form, or null when the code is empty or unknown
   */
  public static UpsertForm lookupCode(String code) {
    if (code == null || code.isBlank()) {
      return null;
    }
    for (UpsertForm form : values()) {
      if (form.name().equalsIgnoreCase(code.trim())) {
        return form;
      }
    }
    return null;
  }
}
