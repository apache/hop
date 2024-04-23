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
 *
 */

package org.apache.hop.pipeline.transforms.combinationlookup;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.metadata.api.HopMetadataProperty;

public class CFields {
  /** which fields do we use to look up a value? */
  @HopMetadataProperty(
      key = "key",
      injectionGroupKey = "KEY_FIELDS",
      injectionGroupDescription = "CombinationLookup.Injection.KEY_FIELDS")
  private List<KeyField> keyFields;

  /** Where to get the sequence from... */
  @HopMetadataProperty(
      key = "sequence",
      injectionKey = "SEQUENCE_FROM",
      injectionKeyDescription = "CombinationLookup.Injection.SEQUENCE_FROM")
  private String sequenceFrom;

  @HopMetadataProperty(key = "return")
  private ReturnFields returnFields;

  public CFields() {
    keyFields = new ArrayList<>();
    returnFields = new ReturnFields();
  }

  public CFields(CFields f) {
    this();
    this.sequenceFrom = f.sequenceFrom;
    this.returnFields = f.returnFields;
    for (KeyField keyField : f.keyFields) {
      this.keyFields.add(new KeyField(keyField));
    }
  }

  public int indexOfKeyField(String name) {
    for (int i = 0; i < keyFields.size(); i++) {
      KeyField keyField = keyFields.get(i);
      if (name != null && name.equalsIgnoreCase(keyField.getName())) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Gets keyFields
   *
   * @return value of keyFields
   */
  public List<KeyField> getKeyFields() {
    return keyFields;
  }

  /**
   * Sets keyFields
   *
   * @param keyFields value of keyFields
   */
  public void setKeyFields(List<KeyField> keyFields) {
    this.keyFields = keyFields;
  }

  /**
   * Gets sequenceFrom
   *
   * @return value of sequenceFrom
   */
  public String getSequenceFrom() {
    return sequenceFrom;
  }

  /**
   * Sets sequenceFrom
   *
   * @param sequenceFrom value of sequenceFrom
   */
  public void setSequenceFrom(String sequenceFrom) {
    this.sequenceFrom = sequenceFrom;
  }

  /**
   * Gets returnFields
   *
   * @return value of returnFields
   */
  public ReturnFields getReturnFields() {
    return returnFields;
  }

  /**
   * Sets returnFields
   *
   * @param returnFields value of returnFields
   */
  public void setReturnFields(ReturnFields returnFields) {
    this.returnFields = returnFields;
  }
}
