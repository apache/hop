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

package org.apache.hop.core.gui.plugin;

/**
 * How {@link GuiWidgetElement} fields that share a {@link GuiWidgetElement#group()} are laid out
 * inside one {@code parentId} tree. {@link #NONE} keeps the single-form layout. {@link #BOXES}
 * stacks one box per group so the boxes fill the parent and each box scrolls its own fields. {@link
 * #LIST} is not implemented and falls back to tabs.
 */
public enum GuiWidgetGroupType {
  NONE,
  TABS,
  LIST,
  BOXES
}
