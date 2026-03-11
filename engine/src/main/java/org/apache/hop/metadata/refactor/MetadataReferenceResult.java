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
 * Describes a single file that contains references to a metadata element (e.g. connection name).
 */
public class MetadataReferenceResult {

  private final String filePath;
  private final int referenceCount;

  public MetadataReferenceResult(String filePath, int referenceCount) {
    this.filePath = filePath;
    this.referenceCount = referenceCount;
  }

  public String getFilePath() {
    return filePath;
  }

  public int getReferenceCount() {
    return referenceCount;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MetadataReferenceResult that = (MetadataReferenceResult) o;
    return referenceCount == that.referenceCount && Objects.equals(filePath, that.filePath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(filePath, referenceCount);
  }
}
