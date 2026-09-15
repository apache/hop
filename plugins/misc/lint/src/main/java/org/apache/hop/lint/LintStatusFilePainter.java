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
import org.apache.hop.core.SwtUniversalImage;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.SessionDisplay;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.perspective.explorer.IExplorerFilePaintListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.ImageData;
import org.eclipse.swt.graphics.PaletteData;
import org.eclipse.swt.graphics.RGB;
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
   * <p>The default display answers for the session bound to the calling thread, and the results
   * this repaints for arrive on lint threads that may have none.
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
    this.display = SessionDisplay.currentOrDefault();
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
   *
   * <p>Asked for at the size it will occupy, so the SVG is rasterized at that size rather than a
   * bigger bitmap being resampled down into it - resampling is what costs an icon its edges.
   */
  private Image badgeIcon(LintStatus status, int size) {
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
      return GuiResource.getInstance().getImage(location, size, size);
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

    switch (status) {
      case ERROR:
        addOverlayIcon(treeItem, status);
        addLintTooltip(treeItem, name, "Linter errors");
        break;
      case WARNING:
        addOverlayIcon(treeItem, status);
        addLintTooltip(treeItem, name, "Linter warnings");
        break;
      case CLEAN:
        addOverlayIcon(treeItem, status);
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

  private void addOverlayIcon(TreeItem treeItem, LintStatus status) {
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

      boolean noBase = base == null || base.isDisposed();
      org.eclipse.swt.graphics.Rectangle bounds = noBase ? null : base.getBounds();
      // Rasterize the badge at the size it will occupy on this icon, so nothing is resampled.
      Image lintIcon =
          badgeIcon(
              status, bounds == null ? BADGE_SIZE : badgeSizeFor(bounds.width, bounds.height));
      if (lintIcon == null || lintIcon.isDisposed()) {
        return;
      }

      if (bounds == null) {
        treeItem.setImage(lintIcon);
        treeItem.setData(APPLIED_STATUS_KEY, status);
        return;
      }

      if (bounds.width > 100 || bounds.height > 100) {
        treeItem.setImage(lintIcon);
        treeItem.setData(APPLIED_STATUS_KEY, status);
        return;
      }

      // Without a composite, leave the file's own icon alone. The item's colour already says
      // what the status is, and replacing the icon with a bare badge would cost more than it
      // tells.
      Image compositeIcon = getOrCreateComposite(base, lintIcon);
      if (compositeIcon != null) {
        treeItem.setImage(compositeIcon);
      }
      treeItem.setData(APPLIED_STATUS_KEY, status);
    } catch (Exception e) {
      log.logError("Error creating overlay icon: " + e.getMessage(), e);
      // The bare badge still says what the status is, which beats leaving the item unmarked.
      Image fallback = badgeIcon(status, BADGE_SIZE);
      if (fallback != null && !fallback.isDisposed()) {
        treeItem.setImage(fallback);
      }
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

  /**
   * The file's own icon with the lint badge in its bottom right corner.
   *
   * <p>Composited pixel by pixel rather than with a {@code GC}. RWT resolves both the device and
   * the drawing delegate of a {@code GC} from its drawable and understands only a Control or a
   * Device, so a {@code GC} on an Image is left with neither: the drawing fails with a
   * NullPointerException, and disposing it then fails with "A factory-created resource cannot be
   * disposed", which is the exception that reaches the log. Working on the {@link ImageData} of
   * both icons needs no drawing surface at all, so Hop Web gets the same badges as the desktop.
   */
  private Image createCompositeIcon(Image originalIcon, Image lintIcon) {
    try {
      if (display == null || display.isDisposed()) {
        return null;
      }
      ImageData baseData = SwtUniversalImage.getImageDataAtZoom(originalIcon, 100);
      if (baseData == null) {
        return null;
      }
      int badgeSize = badgeSizeFor(baseData.width, baseData.height);
      if (badgeSize <= 0) {
        return null;
      }
      // Composited again for every zoom the platform asks for, out of what both icons themselves
      // have at that zoom. Handing over the 100% pixels alone and letting SWT raster-scale them up
      // is what leaves icons blurry on a HiDPI screen - the very thing createDpiAwareImage exists
      // to avoid.
      return SwtUniversalImage.createDpiAwareImage(
          display,
          zoom ->
              withBadge(
                  SwtUniversalImage.getImageDataAtZoom(originalIcon, zoom),
                  SwtUniversalImage.getImageDataAtZoom(lintIcon, zoom),
                  SwtUniversalImage.pixelSize(badgeSize, zoom),
                  SwtUniversalImage.pixelSize(1, zoom)));
    } catch (Exception e) {
      log.logError("Error creating composite icon: " + e.getMessage(), e);
      return null;
    }
  }

  /** A corner mark on an icon this size: half its width at most, and never more than 10px. */
  private static int badgeSizeFor(int width, int height) {
    return Math.min(10, Math.min(width / 2, height / 2));
  }

  /**
   * The base icon with the badge scaled into its bottom right corner, blended over whatever the
   * base has there rather than punched through it, so a badge with soft edges does not leave a hard
   * outline. What the base leaves transparent stays transparent: the tree paints its own background
   * behind the icon.
   *
   * <p>Sizes are in the pixels of the icons handed in, so that the same badge lands in the same
   * place whichever zoom these pixels came from.
   */
  static ImageData withBadge(ImageData baseData, ImageData badgeData, int badgeSize, int margin) {
    ImageData composite = withPerPixelAlpha(baseData);
    ImageData scaled = badgeData.scaledTo(badgeSize, badgeSize);
    ImageData scaledMask = transparencyMask(scaled);
    int offsetX = composite.width - badgeSize - margin;
    int offsetY = composite.height - badgeSize - margin;

    for (int y = 0; y < badgeSize; y++) {
      int targetY = offsetY + y;
      if (targetY < 0 || targetY >= composite.height) {
        continue;
      }
      for (int x = 0; x < badgeSize; x++) {
        int targetX = offsetX + x;
        if (targetX < 0 || targetX >= composite.width) {
          continue;
        }
        int overAlpha = alphaAt(scaled, scaledMask, x, y);
        if (overAlpha == 0) {
          continue;
        }
        RGB over = scaled.palette.getRGB(scaled.getPixel(x, y));
        if (overAlpha == 255) {
          composite.setPixel(targetX, targetY, composite.palette.getPixel(over));
          composite.setAlpha(targetX, targetY, 255);
          continue;
        }
        RGB under = composite.palette.getRGB(composite.getPixel(targetX, targetY));
        int underAlpha = composite.getAlpha(targetX, targetY);
        int outAlpha = overAlpha + underAlpha * (255 - overAlpha) / 255;
        composite.setPixel(
            targetX,
            targetY,
            composite.palette.getPixel(blend(over, under, overAlpha, underAlpha)));
        composite.setAlpha(targetX, targetY, outAlpha);
      }
    }
    return composite;
  }

  /**
   * The same picture in the one shape we can composite into: direct colour with an alpha value per
   * pixel. An icon can express its transparency in any of several ways and only this one can be
   * written back to, so the base is read through {@link #alphaAt} and rewritten as this.
   */
  private static ImageData withPerPixelAlpha(ImageData source) {
    ImageData copy =
        new ImageData(source.width, source.height, 24, new PaletteData(0xFF0000, 0xFF00, 0xFF));
    copy.alphaData = new byte[source.width * source.height];
    ImageData mask = transparencyMask(source);
    for (int y = 0; y < source.height; y++) {
      for (int x = 0; x < source.width; x++) {
        RGB rgb = source.palette.getRGB(source.getPixel(x, y));
        copy.setPixel(x, y, copy.palette.getPixel(rgb));
        copy.setAlpha(x, y, alphaAt(source, mask, x, y));
      }
    }
    return copy;
  }

  /** The 1-bit mask of an icon that carries one (ICO, BMP), or null - read once, not per pixel. */
  private static ImageData transparencyMask(ImageData data) {
    return data.maskData == null ? null : data.getTransparencyMask();
  }

  /** How opaque one pixel is, whichever of the four ways the icon says so. */
  private static int alphaAt(ImageData data, ImageData mask, int x, int y) {
    if (mask != null && mask.getPixel(x, y) == 0) {
      return 0;
    }
    if (data.transparentPixel != -1 && data.getPixel(x, y) == data.transparentPixel) {
      return 0;
    }
    if (data.alphaData != null) {
      return data.getAlpha(x, y);
    }
    return data.alpha == -1 ? 255 : data.alpha;
  }

  /** Source-over: the colour left when {@code over} is laid on {@code under}. */
  private static RGB blend(RGB over, RGB under, int overAlpha, int underAlpha) {
    return new RGB(
        channel(over.red, under.red, overAlpha, underAlpha),
        channel(over.green, under.green, overAlpha, underAlpha),
        channel(over.blue, under.blue, overAlpha, underAlpha));
  }

  private static int channel(int over, int under, int overAlpha, int underAlpha) {
    int outAlpha = overAlpha + underAlpha * (255 - overAlpha) / 255;
    if (outAlpha == 0) {
      return 0;
    }
    int weighted = over * overAlpha * 255 + under * underAlpha * (255 - overAlpha);
    return Math.min(255, weighted / (outAlpha * 255));
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
