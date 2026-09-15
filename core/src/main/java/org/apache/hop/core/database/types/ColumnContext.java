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
package org.apache.hop.core.database.types;

/**
 * Why a column definition is being generated, rather than just how to format it.
 *
 * <p>This replaces the trailing boolean parameters of {@code IDatabase.getFieldDefinition}. Those
 * booleans do not mean the same thing in every dialect: DuckDB reads {@code addFieldName == false}
 * as "I am inside an ALTER COLUMN" and emits the column name plus the TYPE keyword, which is the
 * opposite of what every other dialect does with it. Carrying the intent explicitly is what lets a
 * rule answer the question it was actually asked.
 */
public final class ColumnContext {

  /** What the generated definition is going into. */
  public enum Purpose {
    /** A CREATE TABLE column list. */
    CREATE,
    /** An ALTER TABLE ... ADD COLUMN. */
    ADD_COLUMN,
    /** An ALTER TABLE ... ALTER/MODIFY COLUMN. */
    MODIFY_COLUMN
  }

  private final Purpose purpose;
  private final String technicalKeyField;
  private final String primaryKeyField;
  private final boolean useAutoIncrement;
  private final boolean addFieldName;
  private final boolean addCarriageReturn;

  public ColumnContext(
      Purpose purpose,
      String technicalKeyField,
      String primaryKeyField,
      boolean useAutoIncrement,
      boolean addFieldName,
      boolean addCarriageReturn) {
    this.purpose = purpose;
    this.technicalKeyField = technicalKeyField;
    this.primaryKeyField = primaryKeyField;
    this.useAutoIncrement = useAutoIncrement;
    this.addFieldName = addFieldName;
    this.addCarriageReturn = addCarriageReturn;
  }

  public Purpose getPurpose() {
    return purpose;
  }

  /**
   * @return the name of the technical key field, or null when there is none.
   */
  public String getTechnicalKeyField() {
    return technicalKeyField;
  }

  /**
   * @return the name of the primary key field, or null when there is none.
   */
  public String getPrimaryKeyField() {
    return primaryKeyField;
  }

  public boolean isUseAutoIncrement() {
    return useAutoIncrement;
  }

  public boolean isAddFieldName() {
    return addFieldName;
  }

  public boolean isAddCarriageReturn() {
    return addCarriageReturn;
  }

  /**
   * @return true when the named column is this table's technical key.
   */
  public boolean isTechnicalKey(String fieldName) {
    return technicalKeyField != null && technicalKeyField.equalsIgnoreCase(fieldName);
  }

  /**
   * @return true when the named column is this table's primary key.
   */
  public boolean isPrimaryKey(String fieldName) {
    return primaryKeyField != null && primaryKeyField.equalsIgnoreCase(fieldName);
  }

  /**
   * @return true when the named column is either the technical key or the primary key.
   */
  public boolean isKey(String fieldName) {
    return isTechnicalKey(fieldName) || isPrimaryKey(fieldName);
  }
}
