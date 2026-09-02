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
package org.apache.hop.lint;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.perspective.explorer.IExplorerFilePaintListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;

/** File painter that adds lint status icons to files in the Explorer. */
public class LintStatusFilePainter implements IExplorerFilePaintListener {

  private static final ILogChannel log = LogChannel.GENERAL;

  // Cache configuration
  private static final int MAX_CACHE_SIZE = 1000;
  private static final long CACHE_EXPIRATION_MINUTES = 60;

  // Tree item data keys used to remember the file's real (base) icon and the status we applied,
  // so repeated paints don't re-composite on top of an already-badged icon.
  private static final String BASE_ICON_KEY = "lintBaseIcon";
  private static final String APPLIED_STATUS_KEY = "lintAppliedStatus";

  /** Size of the status badge, matching the small icons the Explorer tree draws. */
  private static final int BADGE_SIZE = 12;

  private final Map<String, LintStatus> fileStatusCache = new ConcurrentHashMap<>();
  private final Map<String, Long> cacheTimestamps = new ConcurrentHashMap<>();

  // Bounded cache of composited (base icon + badge) images, keyed by base/badge identity.
  // Reused across paints and tree items so we never allocate a new Image per paint.
  private final Map<String, Image> compositeIconCache = new ConcurrentHashMap<>();

  /**
   * The display of the GUI this painter belongs to, read when it is built on the UI thread.
   *
   * <p>{@code Display.getDefault()} answers for the session bound to the calling thread, and the
   * results this repaints for arrive on lint threads that may have none.
   */
  private final Display display;

  // Last Explorer tree we painted into; lets us repaint icons on result changes without a full
  // perspective.refresh() (which rebuilds the tree and loses selection/expansion state).
  private volatile Tree lastPaintedTree;

  public enum LintStatus {
    ERROR,
    WARNING,
    CLEAN,
    UNKNOWN
  }

  public LintStatusFilePainter() {
    this.display = Display.getCurrent() != null ? Display.getCurrent() : Display.getDefault();
    updateFileStatusCache();

    LintResultsManager.getInstance()
        .addListener(
            () -> {
              updateFileStatusCache();
              if (display != null && !display.isDisposed()) {
                display.asyncExec(this::repaintExplorerIcons);
              }
            });

    log.logBasic("Lint Status File Painter initialized");
  }

  /**
   * The badge for a status, or null when there is none to draw.
   *
   * <p>These used to be drawn here into off-screen images with a {@code GC}, which Hop Web cannot
   * do: RWT only draws on a control, so the very first call failed and the Explorer showed no lint
   * status at all. Hop ships the three icons as SVG, and {@link GuiResource} loads and caches them
   * per session, which also settles who disposes them - not us.
   */
  private Image badgeIcon(LintStatus status) {
    String location =
        switch (status) {
          case ERROR -> "ui/images/error.svg";
          case WARNING -> "ui/images/warning.svg";
          case CLEAN -> "ui/images/success.svg";
          case UNKNOWN -> null;
        };
    if (location == null) {
      return null;
    }
    try {
      return GuiResource.getInstance().getImage(location, BADGE_SIZE, BADGE_SIZE);
    } catch (Exception e) {
      log.logDetailed("No lint status icon available for " + status + ": " + e.getMessage());
      return null;
    }
  }

  /**
   * Repaint lint icons in the Explorer. Prefers a lightweight tree redraw (which re-runs this
   * painter while preserving selection and expansion) and only falls back to a full perspective
   * refresh when we have not painted a tree yet.
   */
  public void repaintExplorerIcons() {
    Tree tree = lastPaintedTree;
    if (tree != null && !tree.isDisposed()) {
      tree.redraw();
      return;
    }
    ExplorerPerspective perspective = HopGui.getExplorerPerspective();
    if (perspective != null) {
      perspective.refresh();
    }
  }

  @Override
  public void filePainted(Tree tree, TreeItem treeItem, String path, String name) {
    try {
      lastPaintedTree = tree;
      String absolutePath = LintPathUtils.resolveExplorerFilePath(path, name);

      if (!isHopFile(absolutePath)) {
        return;
      }

      LintStatus status = resolveStatus(absolutePath);
      if (status == LintStatus.UNKNOWN) {
        return;
      }

      applyStatusStyle(tree, treeItem, name, absolutePath, status);
    } catch (Exception e) {
      log.logError(
          "Error painting lint status for file " + path + "/" + name + ": " + e.getMessage(), e);
    }
  }

  private void applyStatusStyle(
      Tree tree, TreeItem treeItem, String name, String absolutePath, LintStatus status) {
    org.eclipse.swt.graphics.Color systemDarkGray =
        tree.getDisplay().getSystemColor(SWT.COLOR_DARK_GRAY);
    org.eclipse.swt.graphics.Color currentFg = treeItem.getForeground();
    boolean nonOpenable = currentFg != null && currentFg.equals(systemDarkGray);

    if (!nonOpenable) {
      switch (status) {
        case ERROR:
          treeItem.setForeground(tree.getDisplay().getSystemColor(SWT.COLOR_RED));
          break;
        case WARNING:
          treeItem.setForeground(tree.getDisplay().getSystemColor(SWT.COLOR_DARK_YELLOW));
          break;
        case CLEAN:
          treeItem.setForeground(null);
          break;
        default:
          break;
      }
    }

    Image badge = badgeIcon(status);
    if (badge == null || badge.isDisposed()) {
      return;
    }
    switch (status) {
      case ERROR:
        addOverlayIcon(treeItem, badge, status);
        addLintTooltip(treeItem, name, "Linter errors");
        break;
      case WARNING:
        addOverlayIcon(treeItem, badge, status);
        addLintTooltip(treeItem, name, "Linter warnings");
        break;
      case CLEAN:
        addOverlayIcon(treeItem, badge, status);
        addLintTooltip(treeItem, name, "No linter issues");
        break;
      default:
        break;
    }
  }

  private LintStatus resolveStatus(String filePath) {
    String normalized = LintPathUtils.normalizePath(filePath);
    if (Utils.isEmpty(normalized)) {
      return LintStatus.UNKNOWN;
    }

    LintStatus cached = lookupCachedStatus(normalized);
    if (cached != null) {
      return cached;
    }

    return resolveStatusFromResults(normalized);
  }

  private LintStatus lookupCachedStatus(String normalized) {
    LintStatus direct = fileStatusCache.get(normalized);
    if (direct != null) {
      return direct;
    }

    for (Map.Entry<String, LintStatus> entry : fileStatusCache.entrySet()) {
      if (LintPathUtils.pathsMatch(entry.getKey(), normalized)) {
        return entry.getValue();
      }
    }
    return null;
  }

  private LintStatus resolveStatusFromResults(String normalized) {
    for (Map.Entry<String, List<LintResult>> entry :
        LintResultsManager.getInstance().getAllResultsByFile().entrySet()) {
      if (!LintPathUtils.pathsMatch(entry.getKey(), normalized)) {
        continue;
      }
      List<LintResult> results = entry.getValue();
      if (results == null || results.isEmpty()) {
        return LintStatus.CLEAN;
      }
      boolean hasError = false;
      boolean hasWarning = false;
      for (LintResult result : results) {
        if ("ERROR".equalsIgnoreCase(result.getSeverity())) {
          hasError = true;
          break;
        }
        if ("WARNING".equalsIgnoreCase(result.getSeverity())) {
          hasWarning = true;
        }
      }
      if (hasError) {
        return LintStatus.ERROR;
      }
      if (hasWarning) {
        return LintStatus.WARNING;
      }
      return LintStatus.CLEAN;
    }
    return LintStatus.UNKNOWN;
  }

  private void addOverlayIcon(TreeItem treeItem, Image lintIcon, LintStatus status) {
    try {
      // Already showing this exact status for this item -> nothing to do (avoids re-compositing
      // on every paint, which previously leaked a new Image each time and crashed the GUI).
      if (status == treeItem.getData(APPLIED_STATUS_KEY)) {
        return;
      }

      // Resolve and remember the file's real icon. On the first paint treeItem.getImage()
      // returns the genuine file-type icon; later it would return our composite, so we must
      // composite from the remembered base rather than from the current (badged) image.
      Image base = (Image) treeItem.getData(BASE_ICON_KEY);
      if (base == null || base.isDisposed()) {
        base = treeItem.getImage();
        if (base != null && !base.isDisposed()) {
          treeItem.setData(BASE_ICON_KEY, base);
        }
      }

      if (base == null || base.isDisposed()) {
        treeItem.setImage(lintIcon);
        treeItem.setData(APPLIED_STATUS_KEY, status);
        return;
      }

      org.eclipse.swt.graphics.Rectangle bounds = base.getBounds();
      if (bounds.width > 100 || bounds.height > 100) {
        treeItem.setImage(lintIcon);
        treeItem.setData(APPLIED_STATUS_KEY, status);
        return;
      }

      // No composite (Hop Web cannot draw one): leave the file's own icon alone. The item's
      // colour already says what the status is, and replacing the icon with a bare badge would
      // cost more than it tells.
      Image compositeIcon = getOrCreateComposite(base, lintIcon);
      if (compositeIcon != null) {
        treeItem.setImage(compositeIcon);
      }
      treeItem.setData(APPLIED_STATUS_KEY, status);
    } catch (Exception e) {
      log.logError("Error creating overlay icon: " + e.getMessage(), e);
      treeItem.setImage(lintIcon);
    }
  }

  /** Returns a cached base+badge composite, creating it once on first use. */
  private Image getOrCreateComposite(Image originalIcon, Image lintIcon) {
    String key = System.identityHashCode(originalIcon) + ":" + System.identityHashCode(lintIcon);
    Image cached = compositeIconCache.get(key);
    if (cached != null && !cached.isDisposed()) {
      return cached;
    }
    Image composite = createCompositeIcon(originalIcon, lintIcon);
    if (composite != null) {
      compositeIconCache.put(key, composite);
    }
    return composite;
  }

  private Image createCompositeIcon(Image originalIcon, Image lintIcon) {
    try {
      if (display == null || display.isDisposed()) {
        return null;
      }

      org.eclipse.swt.graphics.Rectangle originalBounds = originalIcon.getBounds();
      org.eclipse.swt.graphics.Rectangle lintBounds = lintIcon.getBounds();

      Image composite = new Image(display, originalBounds.width, originalBounds.height);
      org.eclipse.swt.graphics.GC gc = new org.eclipse.swt.graphics.GC(composite);
      try {
        gc.setBackground(display.getSystemColor(SWT.COLOR_WIDGET_BACKGROUND));
        gc.fillRectangle(0, 0, originalBounds.width, originalBounds.height);
        gc.drawImage(originalIcon, 0, 0);

        int badgeSize = Math.min(10, Math.min(originalBounds.width / 2, originalBounds.height / 2));
        int lintX = originalBounds.width - badgeSize - 1;
        int lintY = originalBounds.height - badgeSize - 1;
        gc.drawImage(
            lintIcon,
            0,
            0,
            lintBounds.width,
            lintBounds.height,
            lintX,
            lintY,
            badgeSize,
            badgeSize);
        return composite;
      } finally {
        gc.dispose();
      }
    } catch (Exception e) {
      log.logError("Error creating composite icon: " + e.getMessage(), e);
      return null;
    }
  }

  private void addLintTooltip(TreeItem treeItem, String fileName, String lintStatus) {
    try {
      treeItem.setData("lintTooltip", fileName + " - " + lintStatus);
      Tree tree = treeItem.getParent();
      if (tree.getData("lintTooltipListener") == null) {
        tree.setToolTipText("");
        tree.addListener(
            SWT.MouseHover,
            event -> {
              TreeItem item = tree.getItem(new org.eclipse.swt.graphics.Point(event.x, event.y));
              if (item != null && item.getData("lintTooltip") != null) {
                tree.setToolTipText((String) item.getData("lintTooltip"));
              } else if (item != null) {
                tree.setToolTipText(item.getText());
              }
            });
        tree.setData("lintTooltipListener", true);
      }
    } catch (Exception e) {
      log.logError("Error setting tooltip: " + e.getMessage(), e);
    }
  }

  private boolean isHopFile(String filePath) {
    return LintEditorGraphHelper.isLintableFilename(filePath);
  }

  /**
   * Clean up expired and excess cache entries.
   *
   * <p>Done when the cache is rebuilt rather than on a timer of its own: a thread per painter is a
   * thread per Hop Web session, and one holding a reference to the painter keeps that session's
   * images alive long after the session is gone.
   */
  private void cleanupExpiredCacheEntries() {
    try {
      long currentTime = System.currentTimeMillis();
      long expirationTime = TimeUnit.MINUTES.toMillis(CACHE_EXPIRATION_MINUTES);

      // Remove expired entries
      cacheTimestamps
          .entrySet()
          .removeIf(
              entry -> {
                if (currentTime - entry.getValue() > expirationTime) {
                  fileStatusCache.remove(entry.getKey());
                  return true;
                }
                return false;
              });

      // If cache is too large, remove oldest entries
      if (fileStatusCache.size() > MAX_CACHE_SIZE) {
        // Find and remove oldest entries beyond MAX_CACHE_SIZE
        List<Map.Entry<String, Long>> entries =
            new java.util.ArrayList<>(cacheTimestamps.entrySet());
        entries.sort(Map.Entry.comparingByValue());

        for (int i = 0; i < entries.size() - MAX_CACHE_SIZE; i++) {
          String keyToRemove = entries.get(i).getKey();
          fileStatusCache.remove(keyToRemove);
          cacheTimestamps.remove(keyToRemove);
        }
      }

      log.logDetailed(
          "LintStatusFilePainter cache cleanup: " + fileStatusCache.size() + " entries remaining");
    } catch (Exception e) {
      log.logError("Error during cache cleanup: " + e.getMessage(), e);
    }
  }

  private void updateFileStatusCache() {
    try {
      fileStatusCache.clear();
      cacheTimestamps.clear();

      for (Map.Entry<String, List<LintResult>> entry :
          LintResultsManager.getInstance().getAllResultsByFile().entrySet()) {
        String normalizedPath = LintPathUtils.normalizePath(entry.getKey());
        if (Utils.isEmpty(normalizedPath)) {
          continue;
        }

        LintStatus status = LintStatus.CLEAN;
        for (LintResult result : entry.getValue()) {
          if ("ERROR".equalsIgnoreCase(result.getSeverity())) {
            status = LintStatus.ERROR;
            break;
          }
          if ("WARNING".equalsIgnoreCase(result.getSeverity())) {
            status = LintStatus.WARNING;
          }
        }

        fileStatusCache.put(normalizedPath, status);
        cacheTimestamps.put(normalizedPath, System.currentTimeMillis());
      }
      cleanupExpiredCacheEntries();
    } catch (Exception e) {
      log.logError("Error updating file status cache: " + e.getMessage(), e);
    }
  }

  public void dispose() {
    // The badge icons belong to GuiResource, which disposes them with the session.

    // Dispose cached composite images
    for (Image composite : compositeIconCache.values()) {
      if (composite != null && !composite.isDisposed()) {
        composite.dispose();
      }
    }
    compositeIconCache.clear();

    // Clean up cache
    fileStatusCache.clear();
    cacheTimestamps.clear();
  }
}
