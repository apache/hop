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

package org.apache.hop.pipeline.transforms.salesforcebulk;

import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;

/** Bulk API 2.0 ingest operation. {@link #getCode()} is the value Salesforce stores. */
public enum SalesforceBulkOperation implements IEnumHasCodeAndDescription {
  INSERT("insert", "SalesforceBulkOperation.Insert"),
  UPDATE("update", "SalesforceBulkOperation.Update"),
  UPSERT("upsert", "SalesforceBulkOperation.Upsert"),
  DELETE("delete", "SalesforceBulkOperation.Delete"),
  HARD_DELETE("hardDelete", "SalesforceBulkOperation.HardDelete");

  private final String code;
  private final String descriptionKey;

  SalesforceBulkOperation(String code, String descriptionKey) {
    this.code = code;
    this.descriptionKey = descriptionKey;
  }

  @Override
  public String getCode() {
    return code;
  }

  @Override
  public String getDescription() {
    return BaseMessages.getString(SalesforceBulkOperation.class, descriptionKey);
  }

  public boolean isDelete() {
    return this == DELETE || this == HARD_DELETE;
  }

  public static SalesforceBulkOperation lookup(String descriptionOrCode) {
    SalesforceBulkOperation byDescription =
        IEnumHasCodeAndDescription.lookupDescription(
            SalesforceBulkOperation.class, descriptionOrCode, null);
    if (byDescription != null) {
      return byDescription;
    }
    if (descriptionOrCode == null) {
      return INSERT;
    }
    for (SalesforceBulkOperation operation : values()) {
      if (operation.code.equalsIgnoreCase(descriptionOrCode)
          || operation.name().equalsIgnoreCase(descriptionOrCode)) {
        return operation;
      }
    }
    return INSERT;
  }
}
