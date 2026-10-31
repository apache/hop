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

package org.apache.hop.pipeline.debug;

import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;

/** What stops when a row on a hop matches a pause condition. */
public enum DebugPauseScope implements IEnumHasCodeAndDescription {
  TRANSFORM("transform", "DebugPauseScope.Transform"),
  PIPELINE("pipeline", "DebugPauseScope.Pipeline"),
  DOWNSTREAM("downstream", "DebugPauseScope.Downstream");

  private final String code;
  private final String descriptionKey;

  DebugPauseScope(String code, String descriptionKey) {
    this.code = code;
    this.descriptionKey = descriptionKey;
  }

  @Override
  public String getCode() {
    return code;
  }

  @Override
  public String getDescription() {
    return BaseMessages.getString(DebugPauseScope.class, descriptionKey);
  }
}
