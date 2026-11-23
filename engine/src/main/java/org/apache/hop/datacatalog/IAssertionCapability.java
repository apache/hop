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

package org.apache.hop.datacatalog;

import java.time.Instant;
import java.util.List;

/**
 * The outcome of the checks the catalog runs on a dataset, such as data quality or freshness
 * assertions. The checks themselves stay in the catalog. Experimental.
 */
public interface IAssertionCapability extends ICatalogCapability {

  enum Outcome {
    PASSED,
    FAILED,
    /** The check could not be evaluated. */
    ERROR,
    /** The check has not run yet, or the catalog reports an outcome Hop doesn't know. */
    UNKNOWN
  }

  /**
   * @param assertionId the id of the check in the catalog
   * @param description what the check verifies, or null
   * @param outcome the outcome of its latest run
   * @param evaluatedAt when that run happened, or null
   */
  record AssertionResult(
      String assertionId, String description, Outcome outcome, Instant evaluatedAt) {}

  /**
   * The latest outcome of every check on the dataset. Not found means the dataset has no checks.
   */
  CatalogAnswer<List<AssertionResult>> latestAssertionResults(CatalogEntityId id);
}
