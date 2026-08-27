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

package org.apache.hop.ui.core.gui;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.util.Utils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Display;

/**
 * This keeps track of the currently active namespace for all the current. It makes it easy to see
 * which namespace is active. A namespace is used for plugins like Environment to set the active
 * environment. The standard for HopGUI is hop-gui and for Translator is is hop-translator
 */
public class HopNamespace {

  private static HopNamespace instance;

  private String namespace;

  /**
   * Hop Web has one Display per UISession. Keying by Display keeps project/namespace isolation
   * without calling HopGui.getInstance() from this class (which would construct a GUI).
   */
  private static final Map<Display, String> NAMESPACE_BY_DISPLAY = new ConcurrentHashMap<>();

  private HopNamespace() {}

  public static final HopNamespace getInstance() {
    if (instance == null) {
      instance = new HopNamespace();
    }
    return instance;
  }

  /**
   * Gets namespace
   *
   * @return value of namespace
   */
  public static final String getNamespace() {
    if (hasUserInterface()) {
      String sessionNamespace = namespaceOfCurrentDisplay();
      if (!Utils.isEmpty(sessionNamespace)) {
        return sessionNamespace;
      }
    }
    String namespace = getInstance().namespace;
    if (Utils.isEmpty(namespace)) {
      throw new HopRuntimeException("Please set a namespace before using one");
    }
    return namespace;
  }

  /**
   * @param namespace The namespace to set
   */
  public static final void setNamespace(String namespace) {
    getInstance().namespace = namespace;
    if (hasUserInterface()) {
      rememberForCurrentDisplay(namespace);
    }
  }

  /**
   * Whether this process has a user interface at all.
   *
   * <p>Touching {@link Display} loads the SWT native libraries. A Hop Server has no reason to load
   * them and in a container no way to: there is no GTK, so the attempt fails with an {@link
   * UnsatisfiedLinkError} and the server never starts. It enables a project on startup like every
   * other Hop tool, which is what brings it here.
   *
   * <p>The two methods below are kept apart from the ones above on purpose: it keeps every
   * reference to {@link Display} out of the code path a headless process runs.
   */
  private static boolean hasUserInterface() {
    return "GUI".equalsIgnoreCase(Const.getHopPlatformRuntime());
  }

  /** The namespace of the session on this thread, or null. Only call with a user interface. */
  private static String namespaceOfCurrentDisplay() {
    Display display = Display.getCurrent();
    if (display == null || display.isDisposed()) {
      return null;
    }
    return NAMESPACE_BY_DISPLAY.get(display);
  }

  /** Remember the namespace for the session on this thread. Only call with a user interface. */
  private static void rememberForCurrentDisplay(String namespace) {
    Display display = Display.getCurrent();
    if (display == null || display.isDisposed()) {
      return;
    }
    if (!NAMESPACE_BY_DISPLAY.containsKey(display)) {
      display.addListener(SWT.Dispose, e -> NAMESPACE_BY_DISPLAY.remove(display));
    }
    NAMESPACE_BY_DISPLAY.put(display, namespace);
  }
}
