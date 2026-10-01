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

package org.apache.hop.ui.hopgui;

import org.eclipse.swt.widgets.Widget;

/**
 * Gives a widget a name the browser can see, as {@code data-hop-id} on the element Hop Web renders
 * it to. A no-op on the desktop, where widgets are addressed through SWT itself.
 *
 * <p>Without this, a browser driving Hop Web can only go by what is painted: the text in a label,
 * the position of an icon, the order of the shells on screen. All three change for reasons that
 * have nothing to do with the thing being addressed - a translation, a new toolbar entry, a wider
 * window - so tests written against them break on unrelated work and, worse, quietly click
 * something else. A {@code data-hop-id} is chosen by the code that builds the widget and only
 * changes when that code does.
 *
 * <p>Ids are the ones Hop already uses internally, so there is nothing new to keep in step: a
 * toolbar item carries its {@code GuiToolbarElement} id, a perspective button carries {@code
 * perspective-} plus the perspective's plugin id.
 *
 * <p>Ids are not unique on their own. The same toolbar exists once per open tab, so a selector has
 * to take the visible match; that is a property of the GUI rather than of the id.
 */
public abstract class TestIdFacade {

  /** The attribute Hop Web renders these ids to. */
  public static final String ATTRIBUTE = "data-hop-id";

  private static final TestIdFacade IMPL;

  static {
    IMPL = (TestIdFacade) ImplementationLoader.newInstance(TestIdFacade.class);
  }

  /**
   * Names a widget for the browser. Safe to call from shared UI code: the desktop implementation
   * does nothing, and neither does Hop Web for a widget it cannot reach.
   *
   * @param widget the widget to name, ignored when null or disposed
   * @param testId the name, ignored when null or empty
   */
  public static void set(Widget widget, String testId) {
    if (widget == null || widget.isDisposed() || testId == null || testId.isEmpty()) {
      return;
    }
    IMPL.setInternal(widget, testId);
  }

  protected abstract void setInternal(Widget widget, String testId);
}
