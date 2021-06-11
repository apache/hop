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

package org.apache.hop.pipeline.transforms.databaselookup;

import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.metadata.api.HopMetadataProperty;

import java.util.ArrayList;
import java.util.List;

public class Lookup {
  /** what's the lookup schema name? */
  @HopMetadataProperty(
      key = "schema",
      injectionKeyDescription = "DatabaseLookupMeta.Injection.SchemaName")
  private String schemaName;

  /** what's the lookup table? */
  @HopMetadataProperty(
      key = "table",
      injectionKeyDescription = "DatabaseLookupMeta.Injection.TableName")
  private String tableName;

  @HopMetadataProperty(
      key = "key",
      injectionGroupKey = "keys",
      injectionGroupDescription = "DatabaseLookupMeta.Injection.Keys")
  private List<KeyField> keyFields;

  @HopMetadataProperty(
      key = "value",
      injectionGroupKey = "values",
      injectionGroupDescription = "DatabaseLookupMeta.Injection.Returns")
  private List<ReturnValue> returnValues;

  @HopMetadataProperty(
      key = "orderby",
      injectionKeyDescription = "DatabaseLookupMeta.Injection.OrderBy")
  private String orderByClause;

  /** Have the lookup fail if multiple results were found, renders the orderByClause useless */
  @HopMetadataProperty(
      key = "fail_on_multiple",
      injectionKeyDescription = "DatabaseLookupMeta.Injection.FailOnMultiple")
  private boolean failingOnMultipleResults;

  /** Have the lookup eat the incoming row when nothing gets found */
  @HopMetadataProperty(
      key = "eat_row_on_failure",
      injectionKeyDescription = "DatabaseLookupMeta.Injection.EatRowOnFailure")
  private boolean eatingRowOnLookupFailure;

  public Lookup() {
    keyFields = new ArrayList<>();
    returnValues = new ArrayList<>();
  }

  public Lookup clone() {
    return new Lookup(this);
  }

  public Lookup(Lookup l) {
    this();
    this.schemaName = l.schemaName;
    this.tableName = l.tableName;
    this.eatingRowOnLookupFailure = l.eatingRowOnLookupFailure;
    this.failingOnMultipleResults = l.failingOnMultipleResults;
    this.orderByClause = l.orderByClause;
    for (KeyField keyField : l.getKeyFields()) {
      keyFields.add(new KeyField(keyField));
    }
    for (ReturnValue returnValue : l.getReturnValues()) {
      returnValues.add(new ReturnValue(returnValue));
    }
  }

  /**
   * Gets schemaName
   *
   * @return value of schemaName
   */
  public String getSchemaName() {
    return schemaName;
  }

  /** @param schemaName The schemaName to set */
  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  /**
   * Gets tableName
   *
   * @return value of tableName
   */
  public String getTableName() {
    return tableName;
  }

  /** @param tableName The tableName to set */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /**
   * Gets keyFields
   *
   * @return value of keyFields
   */
  public List<KeyField> getKeyFields() {
    return keyFields;
  }

  /** @param keyFields The keyFields to set */
  public void setKeyFields(List<KeyField> keyFields) {
    this.keyFields = keyFields;
  }

  /**
   * Gets returnValues
   *
   * @return value of returnValues
   */
  public List<ReturnValue> getReturnValues() {
    return returnValues;
  }

  /** @param returnValues The returnValues to set */
  public void setReturnValues(List<ReturnValue> returnValues) {
    this.returnValues = returnValues;
  }

  /**
   * Gets orderByClause
   *
   * @return value of orderByClause
   */
  public String getOrderByClause() {
    return orderByClause;
  }

  /** @param orderByClause The orderByClause to set */
  public void setOrderByClause(String orderByClause) {
    this.orderByClause = orderByClause;
  }

  /**
   * Gets failingOnMultipleResults
   *
   * @return value of failingOnMultipleResults
   */
  public boolean isFailingOnMultipleResults() {
    return failingOnMultipleResults;
  }

  /** @param failingOnMultipleResults The failingOnMultipleResults to set */
  public void setFailingOnMultipleResults(boolean failingOnMultipleResults) {
    this.failingOnMultipleResults = failingOnMultipleResults;
  }

  /**
   * Gets eatingRowOnLookupFailure
   *
   * @return value of eatingRowOnLookupFailure
   */
  public boolean isEatingRowOnLookupFailure() {
    return eatingRowOnLookupFailure;
  }

  /** @param eatingRowOnLookupFailure The eatingRowOnLookupFailure to set */
  public void setEatingRowOnLookupFailure(boolean eatingRowOnLookupFailure) {
    this.eatingRowOnLookupFailure = eatingRowOnLookupFailure;
  }

}
