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

package org.apache.hop.metadata.refactor;

import java.util.Objects;

/**
 * Describes a reference to a metadata element from another metadata object (e.g. a
 * PipelineRunConfiguration referencing an ExecutionInfoLocation by name).
 */
public class MetadataObjectReference {

  private final String containerMetadataKey;
  private final String containerObjectName;

  public MetadataObjectReference(String containerMetadataKey, String containerObjectName) {
    this.containerMetadataKey = containerMetadataKey;
    this.containerObjectName = containerObjectName;
  }

  public String getContainerMetadataKey() {
    return containerMetadataKey;
  }

  public String getContainerObjectName() {
    return containerObjectName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MetadataObjectReference that = (MetadataObjectReference) o;
    return Objects.equals(containerMetadataKey, that.containerMetadataKey)
        && Objects.equals(containerObjectName, that.containerObjectName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(containerMetadataKey, containerObjectName);
  }
}
