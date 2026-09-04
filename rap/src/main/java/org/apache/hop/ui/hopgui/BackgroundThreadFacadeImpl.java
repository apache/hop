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

import org.eclipse.rap.rwt.RWT;
import org.eclipse.rap.rwt.service.UISession;

/**
 * Carries the session of the thread that starts the work over to the thread that runs it, so that
 * {@code SingletonUtil.getSessionInstance} keeps answering with this user's GUI state instead of
 * throwing "Invalid thread access".
 */
public class BackgroundThreadFacadeImpl extends BackgroundThreadFacade {

  @Override
  Runnable bindInternal(Runnable runnable) {
    UISession uiSession = currentSession();
    if (uiSession == null) {
      return runnable;
    }
    // exec() binds the session to whatever thread calls it, which is the point here: the work
    // itself runs exactly as before, only now with a session to ask.
    return () -> uiSession.exec(runnable);
  }

  /** The session serving this thread, or null when there is none to carry over. */
  private static UISession currentSession() {
    try {
      return RWT.getUISession();
    } catch (RuntimeException e) {
      // Started outside a request: nothing session scoped to preserve.
      return null;
    }
  }
}
