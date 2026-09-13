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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.eclipse.rap.rwt.service.UISession;

/** Opaque tokens bound to a RAP UI session and HTTP session, mapping to an explorer VFS root. */
public final class ExplorerFileRegistry {

  private static final Map<String, ExplorerFileLease> BY_TOKEN = new ConcurrentHashMap<>();
  private static final Map<String, String> TOKEN_BY_UI_SESSION = new ConcurrentHashMap<>();

  private ExplorerFileRegistry() {}

  public static synchronized ExplorerFileLease getOrCreate(UISession uiSession, String rootVfsUri) {
    if (uiSession == null || rootVfsUri == null) {
      throw new IllegalArgumentException("uiSession and rootVfsUri are required");
    }
    String uiId = uiSession.getId();
    String existingToken = TOKEN_BY_UI_SESSION.get(uiId);
    if (existingToken != null) {
      ExplorerFileLease lease = BY_TOKEN.get(existingToken);
      if (lease != null) {
        lease.setRootVfsUri(rootVfsUri);
        return lease;
      }
      TOKEN_BY_UI_SESSION.remove(uiId, existingToken);
    }
    String token = UUID.randomUUID().toString();
    ExplorerFileLease lease = new ExplorerFileLease(token, uiSession, rootVfsUri);
    BY_TOKEN.put(token, lease);
    TOKEN_BY_UI_SESSION.put(uiId, token);
    uiSession.addUISessionListener(event -> remove(token, uiId));
    return lease;
  }

  public static ExplorerFileLease find(String token) {
    if (token == null) {
      return null;
    }
    return BY_TOKEN.get(token);
  }

  static synchronized void remove(String token, String uiSessionId) {
    BY_TOKEN.remove(token);
    TOKEN_BY_UI_SESSION.remove(uiSessionId, token);
  }

  /** Test helper. */
  static synchronized void clear() {
    BY_TOKEN.clear();
    TOKEN_BY_UI_SESSION.clear();
  }
}
