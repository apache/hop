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

import org.eclipse.swt.widgets.Canvas;

/** Desktop RCP implementation of ContextDialogSvgFacade (no-op; uses standard SWT GC). */
public class ContextDialogSvgFacadeImpl extends ContextDialogSvgFacade {

  @Override
  boolean isSupportedInternal() {
    return false;
  }

  @Override
  void registerInternal(Canvas canvas, ContextDialog dialog) {}

  @Override
  void unregisterInternal(Canvas canvas) {}

  @Override
  void renderAndPublishInternal(Canvas canvas, ContextDialog dialog) {}
}
