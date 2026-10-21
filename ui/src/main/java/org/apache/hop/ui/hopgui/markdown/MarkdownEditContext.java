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

package org.apache.hop.ui.hopgui.markdown;

import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.apache.hop.core.variables.IVariables;
import org.eclipse.swt.widgets.Widget;

/**
 * Where a markdown editor should resolve file links and images, and whether markdown actions are
 * active. Suppliers are read at click time so a note checkbox or a renamed file stays current.
 */
public final class MarkdownEditContext {

  public static final String DATA_KEY = MarkdownEditContext.class.getName();

  private final IVariables variables;
  private final Supplier<String> baseFilename;
  private final BooleanSupplier active;

  private MarkdownEditContext(
      IVariables variables, Supplier<String> baseFilename, BooleanSupplier active) {
    this.variables = variables;
    this.baseFilename = baseFilename;
    this.active = active;
  }

  public static void attach(
      Widget widget, IVariables variables, Supplier<String> baseFilename, BooleanSupplier active) {
    if (widget == null || widget.isDisposed()) {
      return;
    }
    widget.setData(
        DATA_KEY,
        new MarkdownEditContext(
            variables,
            baseFilename == null ? () -> null : baseFilename,
            active == null ? () -> true : active));
  }

  public static MarkdownEditContext from(Widget widget) {
    if (widget == null || widget.isDisposed()) {
      return null;
    }
    Object data = widget.getData(DATA_KEY);
    return data instanceof MarkdownEditContext context ? context : null;
  }

  public IVariables variables() {
    return variables;
  }

  public String baseFilename() {
    return baseFilename.get();
  }

  public boolean active() {
    try {
      return active.getAsBoolean();
    } catch (Exception e) {
      return false;
    }
  }
}
