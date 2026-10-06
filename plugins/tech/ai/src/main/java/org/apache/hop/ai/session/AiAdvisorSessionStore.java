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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;
import org.apache.hop.ai.advisor.AiAdvisorOpenRequest;
import org.apache.hop.ai.config.HopAiConfigSingleton;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Display;
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

  /** Keeps conversations in the audit folder; see {@link AiAdvisorSessionArchive}. */
  boolean persistent;

  private final Set<String> loadedScopes = new HashSet<>();
  private boolean saveScheduled;

  /** The provider picked last, for new sessions when no default provider is configured. */
  private String lastProviderName;

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
    // Only the store of Hop GUI keeps conversations on disk, not the throwaway ones of tests.
    store.persistent = true;
    shell.setData(SHELL_DATA_KEY, store);
    // A change made just before Hop GUI closes is still waiting for its delayed save.
    shell.addListener(SWT.Dispose, e -> store.saveNow());
    return store;
  }

  /**
   * The project a session belongs to: Hop GUI's namespace, which is the name of the open project.
   * Replaceable so tests can switch projects.
   */
  Supplier<String> scope = AiAdvisorSessionStore::currentNamespace;

  static String currentNamespace() {
    try {
      return HopNamespace.getNamespace();
    } catch (RuntimeException e) {
      return null;
    }
  }

  /**
   * The sessions of the open project. A session holds its pipeline, its conversation and the
   * provider and metadata it names, which all belong to the project it was started in.
   */
  public List<AiAdvisorSession> getSessions() {
    String current = scope.get();
    loadOnce(current);
    List<AiAdvisorSession> visible = new ArrayList<>();
    for (AiAdvisorSession session : sessions) {
      if (Objects.equals(session.getScope(), current)) {
        visible.add(session);
      }
    }
    return visible;
  }

  public AiAdvisorSession getActiveSession() {
    AiAdvisorSession active = find(activeSessionId);
    if (active == null) {
      // After a project switch the active session can belong to the other project.
      List<AiAdvisorSession> visible = getSessions();
      if (!visible.isEmpty()) {
        active = visible.get(visible.size() - 1);
        activeSessionId = active.getId();
      }
    }
    return active;
  }

  public String getLastProviderName() {
    return lastProviderName;
  }

  public void setLastProviderName(String lastProviderName) {
    this.lastProviderName = lastProviderName;
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
    for (AiAdvisorSession session : getSessions()) {
      if (sessionId.equals(session.getId())) {
        return session;
      }
    }
    return null;
  }

  public AiAdvisorSession add(AiAdvisorSession session) {
    session.setScope(scope.get());
    loadOnce(session.getScope());
    sessions.add(session);
    activeSessionId = session.getId();
    fireChanged();
    return session;
  }

  public void remove(String sessionId) {
    AiAdvisorSession removed = find(sessionId);
    if (removed != null) {
      // A request that is still running would otherwise go on, and keep spending tokens.
      removed.requestCancel();
    }
    sessions.removeIf(session -> session.getId().equals(sessionId));
    if (Objects.equals(activeSessionId, sessionId)) {
      List<AiAdvisorSession> visible = getSessions();
      activeSessionId = visible.isEmpty() ? null : visible.get(visible.size() - 1).getId();
    }
    fireChanged();
  }

  public AiAdvisorSession findReusable(AiAdvisorOpenRequest request) {
    if (request == null || !request.isReuseExisting()) {
      return null;
    }
    AiAdvisorSession sameFile = null;
    for (AiAdvisorSession session : getSessions()) {
      if (!Objects.equals(session.getAdvisorPluginId(), nvl(request.getAdvisorPluginId()))) {
        continue;
      }
      if (!Objects.equals(nvl(session.getLocation()), nvl(request.getLocation()))) {
        continue;
      }
      if (request.getArtifact() == null) {
        // Nothing to identify the pipeline or workflow by, so fall back to the name.
        if (session.getArtifact() == null
            && Utils.isEmpty(session.getArtifactFilename())
            && Objects.equals(nvl(session.getArtifactName()), nvl(request.getArtifactName()))) {
          return session;
        }
        continue;
      }
      if (session.getArtifact() == request.getArtifact()) {
        return session;
      }
      // The same saved file, opened again after its tab was closed. A name is not enough: two
      // files in different folders, or two unsaved pipelines, can have the same name.
      String filename = filenameOf(request.getArtifact());
      if (sameFile == null && !Utils.isEmpty(filename) && filename.equals(filenameOf(session))) {
        sameFile = session;
      }
    }
    return sameFile;
  }

  static String filenameOf(Object artifact) {
    return artifact instanceof IHasFilename hasFilename ? hasFilename.getFilename() : null;
  }

  static String filenameOf(AiAdvisorSession session) {
    String filename = filenameOf(session.getArtifact());
    return filename != null ? filename : session.getArtifactFilename();
  }

  /**
   * A pipeline or workflow tab was closed: its sessions let go of the file and its log, and keep
   * the conversation and the file name for when the file is opened again.
   */
  public void release(Object artifact) {
    boolean changed = false;
    for (AiAdvisorSession session : sessions) {
      if (artifact != null && session.getArtifact() == artifact) {
        String filename = filenameOf(artifact);
        if (!Utils.isEmpty(filename)) {
          session.setArtifactFilename(filename);
        }
        session.setArtifact(null);
        session.setLogSupplier(null);
        changed = true;
      }
    }
    if (changed) {
      fireChanged();
    }
  }

  public AiAdvisorSession open(AiAdvisorOpenRequest request) {
    AiAdvisorSession existing = findReusable(request);
    if (existing != null) {
      // AI Help opened on the pipeline or workflow itself clears a focus set from a transform or
      // action earlier, so that node's XML is no longer sent.
      existing.setFocusNodeName(nvl(request.getFocusNodeName()));
      if (request.getArtifact() != null) {
        existing.setArtifact(request.getArtifact());
        existing.setArtifactFilename(filenameOf(request.getArtifact()));
      }
      // A new pipeline gets its name from the file name when it is first saved.
      if (Objects.equals(existing.getTitle(), existing.getArtifactName())
          && !Utils.isEmpty(request.getTitle())) {
        existing.setTitle(request.getTitle());
      }
      if (!Utils.isEmpty(request.getArtifactName())) {
        existing.setArtifactName(request.getArtifactName());
      }
      if (request.getLogSupplier() != null) {
        existing.setLogSupplier(request.getLogSupplier());
      }
      mergeAttributes(existing, request);
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
      session.setArtifactFilename(filenameOf(request.getArtifact()));
      session.setLogSupplier(request.getLogSupplier());
      session.setAttributes(copyAttributes(request.getAttributes()));
    }
    return add(session);
  }

  /**
   * Link a session that has no pipeline or workflow to one. It takes the advisor, title and log of
   * that file, and keeps its conversation.
   */
  public void link(AiAdvisorSession session, AiAdvisorOpenRequest request) {
    if (session == null || request == null || request.getArtifact() == null) {
      return;
    }
    session.setAdvisorPluginId(nvl(request.getAdvisorPluginId()));
    session.setScenarioId("");
    if (request.getLocation() != null) {
      session.setLocation(request.getLocation());
    }
    session.setAreaLabel(nvl(request.getAreaLabel()));
    session.setArtifact(request.getArtifact());
    session.setArtifactFilename(filenameOf(request.getArtifact()));
    session.setArtifactName(nvl(request.getArtifactName()));
    session.setArtifactKind(nvl(request.getArtifactKind()));
    session.setLogSupplier(request.getLogSupplier());
    session.setFocusNodeName(nvl(request.getFocusNodeName()));
    if (session.isEmpty() || Utils.isEmpty(session.getTitle())) {
      session.setTitle(nvl(request.getTitle()));
    }
    mergeAttributes(session, request);
    activeSessionId = session.getId();
    fireChanged();
  }

  static void mergeAttributes(AiAdvisorSession session, AiAdvisorOpenRequest request) {
    if (session == null || request == null || request.getAttributes() == null) {
      return;
    }
    if (session.getAttributes() == null) {
      session.setAttributes(new LinkedHashMap<>());
    }
    session.getAttributes().putAll(request.getAttributes());
  }

  static Map<String, Object> copyAttributes(Map<String, Object> source) {
    return source == null ? new LinkedHashMap<>() : new LinkedHashMap<>(source);
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
    saveSoon();
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

  private static boolean keepConversations() {
    return HopAiConfigSingleton.getConfig().isKeepConversations();
  }

  /** Bring back the saved conversations of a project the first time its sessions are shown. */
  private void loadOnce(String scopeName) {
    String key = AiAdvisorSessionArchive.group(scopeName);
    if (!persistent || !loadedScopes.add(key) || !keepConversations()) {
      return;
    }
    try {
      List<AiAdvisorSession> saved = AiAdvisorSessionArchive.load(scopeName);
      sessions.addAll(0, saved);
    } catch (Exception e) {
      LogChannel.UI.logError("Unable to read the saved AI Assistant conversations", e);
    }
  }

  /** Save a moment after a change, so a burst of changes is written once. */
  private void saveSoon() {
    if (!persistent || saveScheduled) {
      return;
    }
    Display display = Display.getCurrent();
    if (display == null) {
      saveNow();
      return;
    }
    saveScheduled = true;
    display.timerExec(
        1500,
        () -> {
          saveScheduled = false;
          saveNow();
        });
  }

  void saveNow() {
    if (!persistent || !keepConversations()) {
      return;
    }
    for (String key : loadedScopes) {
      List<AiAdvisorSession> inScope = new ArrayList<>();
      for (AiAdvisorSession session : sessions) {
        if (key.equals(AiAdvisorSessionArchive.group(session.getScope()))) {
          inScope.add(session);
        }
      }
      try {
        AiAdvisorSessionArchive.save(key, inScope);
      } catch (Exception e) {
        LogChannel.UI.logError("Unable to save the AI Assistant conversations", e);
      }
    }
  }
}
