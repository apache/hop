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

package org.apache.hop.pipeline.transforms.maskfields;

import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;

/** Where a masking pattern gets the value it writes. */
public enum MaskingValueSource implements IEnumHasCodeAndDescription {
  /** Prefix, a sequence or UUID, and a suffix. */
  SYNTHETIC(
      "SYNTHETIC",
      BaseMessages.getString(MaskingValueSource.class, "MaskingValueSource.Synthetic.Description")),
  /** Always null. */
  SET_NULL(
      "SET_NULL",
      BaseMessages.getString(MaskingValueSource.class, "MaskingValueSource.SetNull.Description")),
  /** Always an empty string. String fields only. */
  SET_EMPTY(
      "SET_EMPTY",
      BaseMessages.getString(MaskingValueSource.class, "MaskingValueSource.SetEmpty.Description"));

  private final String code;
  private final String description;

  MaskingValueSource(String code, String description) {
    this.code = code;
    this.description = description;
  }

  @Override
  public String getCode() {
    return code;
  }

  @Override
  public String getDescription() {
    return description;
  }
}
