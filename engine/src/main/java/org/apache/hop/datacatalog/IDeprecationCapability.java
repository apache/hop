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

/** Whether a dataset is deprecated in the catalog. Experimental. */
public interface IDeprecationCapability extends ICatalogCapability {

  /**
   * @param deprecated whether the dataset is deprecated
   * @param note what the catalog says about it, or null
   * @param decommissionTime when the dataset goes away, or null
   */
  record Deprecation(boolean deprecated, String note, Instant decommissionTime) {}

  /** Not found means the catalog has no deprecation recorded: the dataset is not deprecated. */
  CatalogAnswer<Deprecation> deprecation(CatalogEntityId id);
}
