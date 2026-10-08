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

/** Whether a masking pattern remembers which replacement a source value received. */
public enum MaskingStorage implements IEnumHasCodeAndDescription {
  /** Every row is assigned on its own. A new run starts again. */
  NONE("NONE", BaseMessages.getString(MaskingStorage.class, "MaskingStorage.None.Description")),
  /** The same source value keeps its replacement for this pipeline execution. */
  MEMORY(
      "MEMORY", BaseMessages.getString(MaskingStorage.class, "MaskingStorage.Memory.Description")),
  /**
   * The same source value keeps its replacement across runs. The mapping table stores the original
   * value.
   */
  DATABASE(
      "DATABASE",
      BaseMessages.getString(MaskingStorage.class, "MaskingStorage.Database.Description"));

  private final String code;
  private final String description;

  MaskingStorage(String code, String description) {
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
