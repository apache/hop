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

package org.apache.hop.naming.metadata;

import java.io.Serializable;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataCategory;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;

/**
 * Reusable naming rules for identifiers such as Hop field names, transform/action names, database
 * tables/columns, and file/folder names. Apply with CTRL-SHIFT-N on a TextVar (or related widget),
 * via the TableView toolbar, or programmatically with {@link
 * org.apache.hop.naming.engine.NamingEngine}.
 */
@Getter
@Setter
@HopMetadata(
    key = "naming-scheme",
    name = "i18n::NamingScheme.Name",
    description = "i18n::NamingScheme.Description",
    image = "naming.svg",
    category = HopMetadataCategory.DATA_DEFINITION,
    documentationUrl = "/metadata-types/naming-scheme.html",
    hopMetadataPropertyType = HopMetadataPropertyType.NAMING_SCHEME)
public class NamingScheme extends HopMetadataBase implements Serializable, IHopMetadata {

  @HopMetadataProperty private String description;

  /** Target kind code: {@link NamingSchemeType} (default {@code general}). */
  @HopMetadataProperty private String type;

  /** Case style code: {@link NamingCaseStyle}. */
  @HopMetadataProperty private String caseStyle;

  /** Word separator code: {@link NamingWordSeparator}. */
  @HopMetadataProperty private String wordSeparator;

  /**
   * Extra characters treated as word boundaries in addition to whitespace, underscore, dash and
   * camelCase edges (for example {@code .#}).
   */
  @HopMetadataProperty private String extraDelimiters;

  /** Strip characters that are not letters or digits from each word. */
  @HopMetadataProperty private boolean removeSpecialCharacters;

  /** Collapse repeated word separators (for example {@code __} → {@code _}). */
  @HopMetadataProperty private boolean collapseRepeatedSeparators;

  /** Remove leading/trailing word separators from the result. */
  @HopMetadataProperty private boolean trimEdgeSeparators;

  @HopMetadataProperty private String prefix;

  @HopMetadataProperty private String suffix;

  public NamingScheme() {
    this.type = NamingSchemeType.GENERAL.getCode();
    this.caseStyle = NamingCaseStyle.LOWER.getCode();
    this.wordSeparator = NamingWordSeparator.UNDERSCORE.getCode();
    this.extraDelimiters = "";
    this.removeSpecialCharacters = true;
    this.collapseRepeatedSeparators = true;
    this.trimEdgeSeparators = true;
    this.prefix = "";
    this.suffix = "";
  }

  public NamingScheme(String name) {
    this();
    this.name = name;
  }
}
