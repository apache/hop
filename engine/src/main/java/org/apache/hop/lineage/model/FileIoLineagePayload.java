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

package org.apache.hop.lineage.model;

import java.util.Objects;
import lombok.Getter;

/** File or VFS operation observation. */
@Getter
public final class FileIoLineagePayload implements LineagePayload {

  private final FileIoOperation operation;
  private final String sourceUri;
  private final String targetUri;
  private final Long bytesTransferred;
  private final boolean success;
  private final String message;

  /**
   * Columns and optional structure tree for this read/write; null when not provided (e.g. workflow
   * copy actions).
   */
  private final FileIoContentSchema contentSchema;

  public FileIoLineagePayload(
      FileIoOperation operation,
      String sourceUri,
      String targetUri,
      Long bytesTransferred,
      boolean success,
      String message,
      FileIoContentSchema contentSchema) {
    this.operation = Objects.requireNonNull(operation, "operation");
    this.sourceUri = sourceUri;
    this.targetUri = targetUri;
    this.bytesTransferred = bytesTransferred;
    this.success = success;
    this.message = message;
    this.contentSchema = contentSchema;
  }

  /** Convenience for callers without a content schema. */
  public FileIoLineagePayload(
      FileIoOperation operation,
      String sourceUri,
      String targetUri,
      Long bytesTransferred,
      boolean success,
      String message) {
    this(operation, sourceUri, targetUri, bytesTransferred, success, message, null);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FileIoLineagePayload that = (FileIoLineagePayload) o;
    return success == that.success
        && operation == that.operation
        && Objects.equals(sourceUri, that.sourceUri)
        && Objects.equals(targetUri, that.targetUri)
        && Objects.equals(bytesTransferred, that.bytesTransferred)
        && Objects.equals(message, that.message)
        && Objects.equals(contentSchema, that.contentSchema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        operation, sourceUri, targetUri, bytesTransferred, success, message, contentSchema);
  }
}
