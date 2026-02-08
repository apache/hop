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

import org.apache.hop.core.Props;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.IToolbarContainer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.RowLayout;
import org.eclipse.swt.widgets.Composite;

/** Hop Web (RAP) implementation: returns a container wrapping a Composite with RowLayout (wrap). */
public class ToolbarFacadeImpl extends ToolbarFacade {

  @Override
  protected IToolbarContainer createToolbarContainerInternal(Composite parent, int style) {
    Composite composite = new Composite(parent, SWT.NONE);
    RowLayout rowLayout = new RowLayout(SWT.HORIZONTAL);
    rowLayout.wrap = true;
    rowLayout.spacing = 4;
    rowLayout.marginWidth = 0;
    rowLayout.marginHeight = 0;
    rowLayout.center = true;
    composite.setLayout(rowLayout);
    PropsUi.setLook(composite, Props.WIDGET_STYLE_TOOLBAR);
    return new WebToolbarContainer(composite);
  }
}
