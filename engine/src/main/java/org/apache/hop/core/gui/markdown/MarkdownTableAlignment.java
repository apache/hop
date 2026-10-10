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

package org.apache.hop.core.gui.markdown;

import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;

/**
 * GFM column alignment written into a Markdown table separator row. One value applies to every
 * column the table dialog inserts.
 */
public enum MarkdownTableAlignment implements IEnumHasCodeAndDescription {
  DEFAULT,
  LEFT,
  CENTER,
  RIGHT;

  @Override
  public String getCode() {
    return name();
  }

  @Override
  public String getDescription() {
    return BaseMessages.getString(MarkdownTableAlignment.class, "MarkdownTableAlignment." + name());
  }

  /** Separator cell for this alignment, without the surrounding pipes. */
  public String separator() {
    return switch (this) {
      case LEFT -> ":---";
      case CENTER -> ":---:";
      case RIGHT -> "---:";
      case DEFAULT -> "---";
    };
  }

  public static String[] getDescriptions() {
    return IEnumHasCodeAndDescription.getDescriptions(MarkdownTableAlignment.class);
  }
}
