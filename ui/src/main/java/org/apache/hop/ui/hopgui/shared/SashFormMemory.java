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

package org.apache.hop.ui.hopgui.shared;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.history.AuditList;
import org.apache.hop.history.AuditManager;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;

/**
 * Remembers the weights (panel sizes) of a {@link SashForm} across restarts, stored globally in the
 * audit trail (a layout preference, not per-project).
 *
 * <p>Call {@link #persist(SashForm, String, int...)} once, after the sash form and all of its child
 * panels have been created. It restores the saved weights (or applies the supplied defaults when
 * nothing has been saved yet) and then saves the weights, debounced, whenever the divider is moved.
 */
public final class SashFormMemory {

  /** Global audit group: a sash layout is a UI preference shared across projects. */
  private static final String AUDIT_GROUP = HopGui.DEFAULT_HOP_GUI_NAMESPACE;

  private static final int SAVE_DEBOUNCE_MS = 400;

  /**
   * Shared default weights for a perspective tree panel, so every perspective lines up at the same
   * width when nothing is saved (and after a reset). Callers that need a different split (e.g. a
   * content sash) pass their own defaults.
   */
  private static final int[] DEFAULT_WEIGHTS = {20, 80};

  /** Every tracked sash form, keyed by its audit key, so {@link #resetAll()} can reach them all. */
  private static final Map<String, Tracked> TRACKED = new LinkedHashMap<>();

  private record Tracked(SashForm sashForm, int[] defaultWeights) {}

  private SashFormMemory() {
    // Utility class
  }

  /**
   * Restores the saved weights of {@code sashForm} and persists them whenever the user drags the
   * divider.
   *
   * @param sashForm the sash form to track (must already have its child panels)
   * @param key a unique audit key for this sash, e.g. {@code "explorer-perspective-tree-width"}
   * @param defaultWeights weights to apply when nothing has been saved yet; pass none to keep the
   *     weights the caller has already set
   */
  public static void persist(SashForm sashForm, String key, int... defaultWeights) {
    if (sashForm == null || sashForm.isDisposed()) {
      return;
    }
    int[] defaults = defaultsOrFallback(defaultWeights);
    restore(sashForm, key, defaults);
    TRACKED.put(key, new Tracked(sashForm, defaults));

    Display display = sashForm.getDisplay();
    Runnable saver = () -> save(sashForm, key);
    // A child resizes both when the divider is dragged and when the window resizes; weights are
    // proportional, so saving on a plain window resize is harmless. Debounce to a single write.
    for (Control child : sashForm.getChildren()) {
      if (child != null && !child.isDisposed()) {
        child.addListener(
            SWT.Resize,
            e -> {
              if (!sashForm.isDisposed()) {
                display.timerExec(-1, saver);
                display.timerExec(SAVE_DEBOUNCE_MS, saver);
              }
            });
      }
    }
  }

  /**
   * Applies the saved weights for {@code key} to {@code sashForm}, or {@code defaultWeights} when
   * nothing has been saved. Use this (instead of a hard-coded {@code setWeights(...)}) wherever a
   * perspective re-applies its layout, e.g. when un-maximizing or showing a panel, so the user's
   * remembered width is honoured.
   */
  public static void restore(SashForm sashForm, String key, int... defaultWeights) {
    if (sashForm == null || sashForm.isDisposed()) {
      return;
    }
    sashForm.setWeights(defaultsOrFallback(defaultWeights));
    try {
      AuditList list = AuditManager.getActive().retrieveList(AUDIT_GROUP, key);
      if (list == null || list.getNames() == null || list.getNames().size() < 2) {
        return;
      }
      int[] weights = new int[list.getNames().size()];
      for (int i = 0; i < weights.length; i++) {
        weights[i] = Integer.parseInt(list.getNames().get(i).trim());
      }
      sashForm.setWeights(weights);
    } catch (Exception e) {
      LogChannel.UI.logError(
          "Error reading sash weights for '" + key + "' from the audit trail", e);
    }
  }

  private static void save(SashForm sashForm, String key) {
    if (sashForm.isDisposed()) {
      return;
    }
    int[] weights = sashForm.getWeights();
    if (weights == null || weights.length < 2) {
      return;
    }
    try {
      List<String> values = new ArrayList<>();
      for (int weight : weights) {
        values.add(Integer.toString(weight));
      }
      AuditManager.getActive().storeList(AUDIT_GROUP, key, new AuditList(values));
    } catch (Exception e) {
      LogChannel.UI.logError("Error storing sash weights for '" + key + "' in the audit trail", e);
    }
  }

  /**
   * Resets every tracked sash form to its default weights and forgets the saved widths, returning
   * the panels to their out-of-the-box layout now and after a restart. Tracked perspectives are
   * those whose {@link #persist(SashForm, String, int...)} has run this session (created at
   * startup).
   */
  public static void resetAll() {
    for (Map.Entry<String, Tracked> entry : TRACKED.entrySet()) {
      Tracked tracked = entry.getValue();
      SashForm sashForm = tracked.sashForm();
      int[] defaults = tracked.defaultWeights();
      if (sashForm != null && !sashForm.isDisposed() && defaults != null && defaults.length > 0) {
        sashForm.setWeights(defaults);
      }
      forget(entry.getKey());
    }
  }

  /** The caller's defaults, or the shared {@link #DEFAULT_WEIGHTS} when none were supplied. */
  private static int[] defaultsOrFallback(int[] defaultWeights) {
    return (defaultWeights != null && defaultWeights.length > 0) ? defaultWeights : DEFAULT_WEIGHTS;
  }

  /** Removes the saved weights for a key (stores an empty list so the next load falls back). */
  private static void forget(String key) {
    try {
      AuditManager.getActive().storeList(AUDIT_GROUP, key, new AuditList(new ArrayList<>()));
    } catch (Exception e) {
      LogChannel.UI.logError("Error clearing saved sash weights for '" + key + "'", e);
    }
  }
}
