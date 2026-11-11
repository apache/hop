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

package org.apache.hop.core.failure;

/**
 * What kind of failure this is, decided by fixed rules on the exception types and SQL states in the
 * cause (see {@link FailureClassifier}). A pipeline, workflow or action failure carries the
 * category of the failure that caused it.
 */
public enum FailureCategory {
  /** A host, database or service could not be reached, or the connection was lost or timed out. */
  CONNECTIVITY,
  /** Credentials were refused, or the user lacks a permission. */
  AUTHENTICATION,
  /** A table, column or field is not what the pipeline expects. */
  SCHEMA_MISMATCH,
  /** A value could not be converted or parsed. */
  DATA_CONVERSION,
  /** The target refused a row: a key, unique, foreign key, not null or check constraint. */
  CONSTRAINT_VIOLATION,
  /** A file or path that was read does not exist. */
  MISSING_INPUT,
  /** Memory, disk, locks or another resource ran out or was contended. */
  RESOURCE,
  /** The pipeline, workflow, plugin or its settings are wrong. */
  CONFIGURATION,
  /** A child execution failed without telling why. Reserved, for children that run remotely. */
  CHILD_FAILURE,
  /** None of the rules matched. */
  UNKNOWN
}
