/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hop.pipeline.transforms.update;

import org.apache.hop.metadata.api.HopMetadataProperty;

import java.util.Objects;

public class UpdateKeyField {
  /** which field in input stream to compare with? */
  @HopMetadataProperty(
      key = "name",
      injectionKeyDescription = "UpdateMeta.Injection.KeyStream",
      injectionKey = "KEY_STREAM")
  private String keyStream;

  /** field in table */
  @HopMetadataProperty(
      key = "field",
      injectionKeyDescription = "UpdateMeta.Injection.KeyLookup",
      injectionKey = "KEY_LOOKUP")
  private String keyLookup;

  /** Comparator: =, <>, BETWEEN, ... */
  @HopMetadataProperty(
      key = "condition",
      injectionKeyDescription = "UpdateMeta.Injection.KeyCondition",
      injectionKey = "KEY_CONDITION")
  private String keyCondition;

  /** Extra field for between... */
  @HopMetadataProperty(
      key = "name2",
      injectionKeyDescription = "UpdateMeta.Injection.KeyStream2",
      injectionKey = "KEY_STREAM2")
  private String keyStream2;

  public UpdateKeyField() {}

  public UpdateKeyField(String keyStream, String keyLookup, String keyCondition) {
    this.keyStream = keyStream;
    this.keyLookup = keyLookup;
    this.keyCondition = keyCondition;
  }

  public UpdateKeyField(
      String keyStream, String keyLookup, String keyCondition, String keyStream2) {
    this.keyStream = keyStream;
    this.keyLookup = keyLookup;
    this.keyCondition = keyCondition;
    this.keyStream2 = keyStream2;
  }

  public String getKeyStream() {
    return keyStream;
  }

  public void setKeyStream(String keyStream) {
    this.keyStream = keyStream;
  }

  public String getKeyLookup() {
    return keyLookup;
  }

  public void setKeyLookup(String keyLookup) {
    this.keyLookup = keyLookup;
  }

  public String getKeyCondition() {
    return keyCondition;
  }

  public void setKeyCondition(String keyCondition) {
    this.keyCondition = keyCondition;
  }

  public String getKeyStream2() {
    return keyStream2;
  }

  public void setKeyStream2(String keyStream2) {
    this.keyStream2 = keyStream2;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    UpdateKeyField that = (UpdateKeyField) o;
    return keyStream.equals(that.keyStream)
        && keyLookup.equals(that.keyLookup)
        && keyCondition.equals(that.keyCondition)
        && Objects.equals(keyStream2, that.keyStream2);
  }

  @Override
  public int hashCode() {
    return Objects.hash(keyStream, keyLookup, keyCondition, keyStream2);
  }
}
