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

package org.apache.hop.ai.session;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.hop.ai.advisor.AiAdvisorOpenRequest;
import org.apache.hop.core.util.Utils;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.widgets.Shell;

/**
 * Hop-Gui-session store of advisory conversations. Perspective, floating dialog and dock all read
 * the same list so topics survive moving the workbench.
 */
public class AiAdvisorSessionStore {

  static final String SHELL_DATA_KEY = AiAdvisorSessionStore.class.getName();

  private final List<AiAdvisorSession> sessions = new ArrayList<>();
  private final List<Runnable> listeners = new CopyOnWriteArrayList<>();
  private String activeSessionId;
  private boolean firing;

  public static AiAdvisorSessionStore get(HopGui hopGui) {
    if (hopGui == null || hopGui.getShell() == null || hopGui.getShell().isDisposed()) {
      return new AiAdvisorSessionStore();
    }
    Shell shell = hopGui.getShell();
    Object existing = shell.getData(SHELL_DATA_KEY);
    if (existing instanceof AiAdvisorSessionStore store) {
      return store;
    }
    AiAdvisorSessionStore store = new AiAdvisorSessionStore();
    shell.setData(SHELL_DATA_KEY, store);
    return store;
  }

  public List<AiAdvisorSession> getSessions() {
    return sessions;
  }

  public AiAdvisorSession getActiveSession() {
    return find(activeSessionId);
  }

  public String getActiveSessionId() {
    return activeSessionId;
  }

  public void setActiveSessionId(String sessionId) {
    this.activeSessionId = sessionId;
    fireChanged();
  }

  public AiAdvisorSession find(String sessionId) {
    if (sessionId == null) {
      return null;
    }
    for (AiAdvisorSession session : sessions) {
      if (sessionId.equals(session.getId())) {
        return session;
      }
    }
    return null;
  }

  public AiAdvisorSession add(AiAdvisorSession session) {
    sessions.add(session);
    activeSessionId = session.getId();
    fireChanged();
    return session;
  }

  public void remove(String sessionId) {
    sessions.removeIf(session -> session.getId().equals(sessionId));
    if (Objects.equals(activeSessionId, sessionId)) {
      activeSessionId = sessions.isEmpty() ? null : sessions.get(sessions.size() - 1).getId();
    }
    fireChanged();
  }

  public AiAdvisorSession findReusable(AiAdvisorOpenRequest request) {
    if (request == null || !request.isReuseExisting()) {
      return null;
    }
    for (AiAdvisorSession session : sessions) {
      if (!Objects.equals(session.getAdvisorPluginId(), nvl(request.getAdvisorPluginId()))) {
        continue;
      }
      if (!Objects.equals(nvl(session.getLocation()), nvl(request.getLocation()))) {
        continue;
      }
      if (!Objects.equals(nvl(session.getArtifactName()), nvl(request.getArtifactName()))) {
        continue;
      }
      return session;
    }
    return null;
  }

  public AiAdvisorSession open(AiAdvisorOpenRequest request) {
    AiAdvisorSession existing = findReusable(request);
    if (existing != null) {
      if (!Utils.isEmpty(request.getFocusNodeName())) {
        existing.setFocusNodeName(request.getFocusNodeName());
      }
      if (request.getArtifact() != null) {
        existing.setArtifact(request.getArtifact());
      }
      if (request.getLogSupplier() != null) {
        existing.setLogSupplier(request.getLogSupplier());
      }
      activeSessionId = existing.getId();
      fireChanged();
      return existing;
    }
    AiAdvisorSession session = new AiAdvisorSession();
    if (request != null) {
      session.setAdvisorPluginId(nvl(request.getAdvisorPluginId()));
      session.setTitle(nvl(request.getTitle()));
      session.setLocation(
          request.getLocation() != null ? request.getLocation() : session.getLocation());
      session.setAreaLabel(nvl(request.getAreaLabel()));
      session.setArtifactName(nvl(request.getArtifactName()));
      session.setArtifactKind(nvl(request.getArtifactKind()));
      session.setFocusNodeName(nvl(request.getFocusNodeName()));
      session.setArtifact(request.getArtifact());
      session.setLogSupplier(request.getLogSupplier());
    }
    return add(session);
  }

  public void addListener(Runnable listener) {
    if (listener != null) {
      listeners.add(listener);
    }
  }

  public void removeListener(Runnable listener) {
    listeners.remove(listener);
  }

  public void fireChanged() {
    if (firing) {
      return;
    }
    firing = true;
    try {
      for (Runnable listener : listeners) {
        listener.run();
      }
    } finally {
      firing = false;
    }
  }

  private static String nvl(String value) {
    return value == null ? "" : value;
  }
}
