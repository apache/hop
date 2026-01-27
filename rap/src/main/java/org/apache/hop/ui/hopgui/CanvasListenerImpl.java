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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.apache.commons.io.IOUtils;
import org.apache.hop.core.logging.LogChannel;
import org.eclipse.rap.rwt.SingletonUtil;
import org.eclipse.rap.rwt.scripting.ClientListener;

public class CanvasListenerImpl extends ClientListener implements ISingletonProvider {

  @Override
  public Object getInstanceInternal() {
    return SingletonUtil.getSessionInstance(CanvasListenerImpl.class);
  }

  public CanvasListenerImpl() {
    super(getText());
  }

  private static String getText() {
    String canvasScript = null;
    try {
      canvasScript =
          IOUtils.toString(
              Objects.requireNonNull(CanvasListenerImpl.class.getResourceAsStream("canvas.js")),
              StandardCharsets.UTF_8);
    } catch (IOException e) {
      LogChannel.UI.logError("Error loading canvas.js", e);
    }
    return canvasScript;
  }
}
