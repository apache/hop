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
import java.util.Locale;
import java.util.Set;

/** The fields of a dataset as the catalog knows them, with their tags and terms. Experimental. */
public interface ISchemaCapability extends ICatalogCapability {

  /**
   * One field of a dataset.
   *
   * @param name the field name; for a nested field its path, for example {@code address.city}
   * @param nativeType the type in the source, for example {@code VARCHAR(255)}, or null
   * @param type the catalog's own type, for example {@code STRING} or {@code NUMBER}, or null
   * @param nullable whether the field can be empty, or null when not known
   * @param tags every way the catalog names the tags on the field, for example both {@code PII} and
   *     {@code urn:li:tag:PII}
   * @param terms every way the catalog names the glossary terms on the field
   */
  record CatalogField(
      String name,
      String nativeType,
      String type,
      Boolean nullable,
      Set<String> tags,
      Set<String> terms) {

    /** Whether the field carries this tag or term, compared without regard to case. */
    public boolean isLabelledWith(String tagOrTerm) {
      String wanted = tagOrTerm.trim().toLowerCase(Locale.ROOT);
      return tags.stream().anyMatch(t -> t.toLowerCase(Locale.ROOT).equals(wanted))
          || terms.stream().anyMatch(t -> t.toLowerCase(Locale.ROOT).equals(wanted));
    }
  }

  /** Not found means the catalog has no schema for the dataset. */
  CatalogAnswer<List<CatalogField>> schema(CatalogEntityId id);
}
