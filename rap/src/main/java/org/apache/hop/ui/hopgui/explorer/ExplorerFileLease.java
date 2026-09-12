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

package org.apache.hop.ui.hopgui.explorer;

import jakarta.servlet.http.HttpSession;
import lombok.Getter;
import lombok.Setter;
import org.eclipse.rap.rwt.service.UISession;

/** Session-scoped permission to serve explorer files under a single VFS root. */
@Getter
public final class ExplorerFileLease {

  private final String token;
  private final UISession uiSession;

  @Setter private volatile String rootVfsUri;

  ExplorerFileLease(String token, UISession uiSession, String rootVfsUri) {
    this.token = token;
    this.uiSession = uiSession;
    this.rootVfsUri = rootVfsUri;
  }

  public String getHttpSessionId() {
    if (uiSession == null) {
      return null;
    }
    try {
      HttpSession httpSession = uiSession.getHttpSession();
      return httpSession != null ? httpSession.getId() : null;
    } catch (Exception e) {
      return null;
    }
  }
}
