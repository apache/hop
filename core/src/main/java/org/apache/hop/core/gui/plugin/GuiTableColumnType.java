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

package org.apache.hop.core.gui.plugin;

/** Cell editor used by a {@link GuiTableColumn}. Mapped to a table column by the UI layer. */
public enum GuiTableColumnType {
  /** A text cell. The annotated field must be a {@link String}. */
  TEXT,

  /**
   * A combo cell. An enum field is read-only and uses {@link Enum#name()}. A {@link String} field
   * is editable; its items come from {@link GuiTableColumn#comboValuesMethod()}.
   */
  COMBO,

  /**
   * A yes/no cell ({@code Y} / {@code N}). The annotated field must be {@code boolean} or {@link
   * Boolean}.
   */
  CHECKBOX
}
