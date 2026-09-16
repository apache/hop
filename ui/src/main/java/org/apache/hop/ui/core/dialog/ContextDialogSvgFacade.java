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

package org.apache.hop.ui.core.dialog;

import org.apache.hop.ui.hopgui.ImplementationLoader;
import org.eclipse.swt.widgets.Canvas;

/**
 * Facade for Hop Web server-side ContextDialog SVG rendering. Desktop (RCP) uses a no-op
 * implementation.
 */
public abstract class ContextDialogSvgFacade {

  private static final ContextDialogSvgFacade IMPL;

  static {
    IMPL = (ContextDialogSvgFacade) ImplementationLoader.newInstance(ContextDialogSvgFacade.class);
  }

  public static boolean isSupported() {
    return IMPL.isSupportedInternal();
  }

  public static void register(Canvas canvas, ContextDialog dialog) {
    IMPL.registerInternal(canvas, dialog);
  }

  public static void unregister(Canvas canvas) {
    IMPL.unregisterInternal(canvas);
  }

  public static void renderAndPublish(Canvas canvas, ContextDialog dialog) {
    IMPL.renderAndPublishInternal(canvas, dialog);
  }

  abstract boolean isSupportedInternal();

  abstract void registerInternal(Canvas canvas, ContextDialog dialog);

  abstract void unregisterInternal(Canvas canvas);

  abstract void renderAndPublishInternal(Canvas canvas, ContextDialog dialog);
}
