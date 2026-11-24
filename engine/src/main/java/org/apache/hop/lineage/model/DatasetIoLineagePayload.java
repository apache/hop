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
import org.apache.hop.core.util.CredentialRedactor;

/**
 * A read or write of a dataset that is neither a table nor a file, such as a Kafka topic. It is
 * named as the OpenLineage naming spec names it: a namespace whose scheme is the platform ({@code
 * kafka://broker:9092}) and a name within it (the topic). Sinks that know the platform name it as
 * that platform's own integrations do, so the dataset joins up with the one they describe.
 */
@Getter
public final class DatasetIoLineagePayload implements LineagePayload {

  private final DatasetIoOperation operation;

  /** The platform and where it runs, such as {@code kafka://broker:9092}. */
  private final String namespace;

  /** The dataset within the namespace, such as a topic. */
  private final String name;

  private final boolean success;
  private final String message;

  public DatasetIoLineagePayload(
      DatasetIoOperation operation,
      String namespace,
      String name,
      boolean success,
      String message) {
    this.operation = Objects.requireNonNull(operation, "operation");
    // Lineage events leave the process: no credentials, even in a namespace built from a URL.
    this.namespace = CredentialRedactor.redact(namespace);
    this.name = name;
    this.success = success;
    this.message = CredentialRedactor.redact(message);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DatasetIoLineagePayload that = (DatasetIoLineagePayload) o;
    return success == that.success
        && operation == that.operation
        && Objects.equals(namespace, that.namespace)
        && Objects.equals(name, that.name)
        && Objects.equals(message, that.message);
  }

  @Override
  public int hashCode() {
    return Objects.hash(operation, namespace, name, success, message);
  }
}
