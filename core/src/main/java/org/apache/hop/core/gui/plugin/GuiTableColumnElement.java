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

import lombok.Getter;
import lombok.Setter;

/**
 * One column of a {@link GuiElementType#TABLE} widget, captured when the GUI registry scans a
 * {@link GuiTableColumn}. SWT-free so the registry can live in core.
 */
@Getter
@Setter
public class GuiTableColumnElement {

  private String id;
  private String order;
  private String label;
  private String toolTip;
  private GuiTableColumnType type;
  private String fieldName;
  private Class<?> fieldClass;
  private String getterMethod;
  private String setterMethod;
  private boolean variables;
  private boolean password;
  private int width = -1;
  private String comboValuesMethod;
}
