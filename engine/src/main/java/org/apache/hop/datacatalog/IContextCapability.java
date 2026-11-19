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

import java.util.List;

/**
 * What a catalog tells people about a dataset: its description, its owners, its tags and its
 * glossary terms. Experimental.
 */
public interface IContextCapability extends ICatalogCapability {

  /**
   * An owner of a dataset.
   *
   * @param name the name of the person or group, as the catalog shows it
   * @param role what kind of owner, for example {@code Technical Owner}, or null
   */
  record Owner(String name, String role) {}

  /**
   * @param description the description of the dataset, or null
   * @param owners its owners
   * @param tags the names of its tags
   * @param terms the names of its glossary terms
   */
  record DatasetContext(
      String description, List<Owner> owners, List<String> tags, List<String> terms) {
    public DatasetContext {
      owners = owners == null ? List.of() : List.copyOf(owners);
      tags = tags == null ? List.of() : List.copyOf(tags);
      terms = terms == null ? List.of() : List.copyOf(terms);
    }
  }

  /** Not found means the catalog doesn't have the dataset. */
  CatalogAnswer<DatasetContext> context(CatalogEntityId id);
}
