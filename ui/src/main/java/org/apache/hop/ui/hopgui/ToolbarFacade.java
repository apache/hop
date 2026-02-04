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

package org.apache.hop.ui.hopgui;

import org.apache.hop.ui.core.gui.IToolbarContainer;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;

/**
 * Facade for toolbar creation. Abstracts the difference between desktop (SWT ToolBar) and Hop Web
 * (RAP), where the RAP implementation returns a Composite with RowLayout so toolbars wrap to
 * multiple lines.
 *
 * <p>Use {@link #createToolbarContainer(Composite, int)} with {@link
 * org.apache.hop.ui.core.gui.GuiToolbarWidgets#createToolbarWidgets(IToolbarContainer, String)} for
 * the single code path. {@link #createToolbar(Composite, int)} returns only the control for layout.
 */
public abstract class ToolbarFacade {

  private static final ToolbarFacade IMPL;

  static {
    IMPL = (ToolbarFacade) ImplementationLoader.newInstance(ToolbarFacade.class);
  }

  /**
   * Create a toolbar container. Use {@link IToolbarContainer#getControl()} for layout, then {@link
   * org.apache.hop.ui.core.gui.GuiToolbarWidgets#createToolbarWidgets(IToolbarContainer, String)}
   * to add items in a single path.
   *
   * @param parent the parent composite
   * @param style SWT style (e.g. SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL)
   * @return container whose getControl() is the toolbar (ToolBar or Composite)
   */
  public static IToolbarContainer createToolbarContainer(Composite parent, int style) {
    return IMPL.createToolbarContainerInternal(parent, style);
  }

  /**
   * Create a toolbar parent control. Convenience for {@link #createToolbarContainer(Composite,
   * int)}.{@link IToolbarContainer#getControl() getControl()}.
   */
  public static Control createToolbar(Composite parent, int style) {
    return createToolbarContainer(parent, style).getControl();
  }

  /**
   * Implementation-specific toolbar container creation.
   *
   * @param parent the parent composite
   * @param style SWT style
   * @return container wrapping ToolBar (desktop) or Composite with RowLayout (web)
   */
  protected abstract IToolbarContainer createToolbarContainerInternal(Composite parent, int style);
}
