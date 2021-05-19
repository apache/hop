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

package org.apache.hop.pipeline.transforms.mergejoin;

import org.apache.hop.metadata.api.HopMetadataProperty;

import java.util.Objects;

public class LeftKey {
  @HopMetadataProperty(
      key = "key",
      injectionKey = "KEY_FIELD1",
      injectionKeyDescription = "MergeJoin.Injection.KEY_FIELD1")
  private String key;

  public LeftKey() {}

  public LeftKey(String key) {
    this.key = key;
  }

  public LeftKey(LeftKey k) {
    this.key = k.key;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LeftKey leftKey = (LeftKey) o;
    return Objects.equals(key, leftKey.key);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key);
  }

  /**
   * Gets key
   *
   * @return value of key
   */
  public String getKey() {
    return key;
  }

  /** @param key The key to set */
  public void setKey(String key) {
    this.key = key;
  }
}
