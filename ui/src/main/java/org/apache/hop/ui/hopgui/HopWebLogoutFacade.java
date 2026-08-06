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

/**
 * Log off / switch user for Hop Web. RAP clears BASIC/session auth and reloads the entry point so
 * the browser prompts for credentials again. Desktop is a no-op message.
 */
public abstract class HopWebLogoutFacade {

  private static final HopWebLogoutFacade IMPL;

  static {
    IMPL = (HopWebLogoutFacade) ImplementationLoader.newInstance(HopWebLogoutFacade.class);
  }

  /**
   * Log the current user off and return to the login challenge (Hop Web). On desktop, shows a short
   * informational dialog.
   */
  public static void logOff() {
    IMPL.logOffInternal();
  }

  abstract void logOffInternal();
}
