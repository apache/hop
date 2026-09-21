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
package org.apache.hop.ai.metadata;

import java.util.Objects;
import org.apache.hop.metadata.api.HopMetadataProperty;

/** One model served by an {@link AiProvider}, for one {@link AiModelRole}. */
public class AiProviderModel {

  @HopMetadataProperty(key = "role", injectionKey = "MODEL_ROLE")
  private AiModelRole role = AiModelRole.CHAT;

  @HopMetadataProperty(key = "model_name", injectionKey = "MODEL_NAME")
  private String modelName = "";

  public AiProviderModel() {}

  public AiProviderModel(AiModelRole role, String modelName) {
    this.role = role;
    this.modelName = modelName;
  }

  public AiProviderModel(AiProviderModel other) {
    this.role = other.role;
    this.modelName = other.modelName;
  }

  public AiModelRole getRole() {
    return role;
  }

  public void setRole(AiModelRole role) {
    this.role = role;
  }

  public String getModelName() {
    return modelName;
  }

  public void setModelName(String modelName) {
    this.modelName = modelName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AiProviderModel other)) {
      return false;
    }
    return role == other.role && Objects.equals(modelName, other.modelName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(role, modelName);
  }
}
