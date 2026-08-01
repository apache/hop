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

package org.apache.hop.core.gui.markdown;

import java.awt.image.BufferedImage;
import java.io.InputStream;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.imageio.ImageIO;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.svg.SvgCache;
import org.apache.hop.core.svg.SvgCacheEntry;
import org.apache.hop.core.svg.SvgFile;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;

/**
 * Resolve and probe images referenced from Markdown notes ({@code ![alt](path)}). Network URLs are
 * rejected; paths are resolved relative to the host pipeline/workflow via HopVfs.
 */
public final class NoteImageSupport {

  /** Soft cap on rendered image height in canvas units. */
  public static final int DEFAULT_MAX_HEIGHT = 200;

  private static final Map<String, ImageInfo> SIZE_CACHE = new ConcurrentHashMap<>();

  private NoteImageSupport() {}

  public record ImageInfo(String path, int width, int height, boolean available, boolean svg) {}

  public static boolean isNetworkUrl(String target) {
    if (Utils.isEmpty(target)) {
      return false;
    }
    String t = target.trim();
    return t.regionMatches(true, 0, "http://", 0, 7)
        || t.regionMatches(true, 0, "https://", 0, 8)
        || t.regionMatches(true, 0, "ftp://", 0, 6);
  }

  public static boolean isAllowedImageTarget(String target) {
    if (Utils.isEmpty(target)) {
      return false;
    }
    String t = target.trim();
    if (isNetworkUrl(t)) {
      return false;
    }
    if (t.regionMatches(true, 0, "javascript:", 0, 11)
        || t.regionMatches(true, 0, "data:", 0, 5)
        || t.regionMatches(true, 0, "vbscript:", 0, 9)) {
      return false;
    }
    return true;
  }

  /**
   * Resolve a Markdown image destination to a VFS URI/path. Returns {@code null} when disallowed or
   * empty.
   */
  public static String resolvePath(IVariables variables, String baseFilename, String target)
      throws HopException {
    if (!isAllowedImageTarget(target)) {
      return null;
    }
    String resolved = variables != null ? variables.resolve(target.trim()) : target.trim();
    if (Utils.isEmpty(resolved) || !isAllowedImageTarget(resolved)) {
      return null;
    }
    if (resolved.regionMatches(true, 0, "file:", 0, 5)) {
      try {
        return HopVfs.getFileObject(resolved, variables).getName().getURI();
      } catch (Exception e) {
        throw new HopException("Unable to resolve image path: " + target, e);
      }
    }
    if (resolved.startsWith("/")
        || resolved.startsWith("\\")
        || resolved.matches("^[A-Za-z]:[\\\\/].*")
        || resolved.contains("://")) {
      return resolved;
    }
    if (Utils.isEmpty(baseFilename)) {
      return resolved;
    }
    try {
      String base = variables != null ? variables.resolve(baseFilename) : baseFilename;
      FileObject baseFile = HopVfs.getFileObject(base, variables);
      FileObject parent = baseFile.getParent();
      if (parent == null) {
        return resolved;
      }
      return parent.resolveFile(resolved).getName().getURI();
    } catch (Exception e) {
      throw new HopException(
          "Unable to resolve image '" + target + "' relative to '" + baseFilename + "'", e);
    }
  }

  /** Probe image dimensions (cached by path + last-modified when available). */
  public static ImageInfo probe(String path) {
    if (Utils.isEmpty(path)) {
      return new ImageInfo(path, 0, 0, false, false);
    }
    long mtime = 0;
    try {
      FileObject fo = HopVfs.getFileObject(path);
      if (fo.exists()) {
        mtime = fo.getContent().getLastModifiedTime();
      }
    } catch (Exception ignored) {
      // cache key falls back to path only
    }
    String key = path + "|" + mtime;
    ImageInfo cached = SIZE_CACHE.get(key);
    if (cached != null) {
      return cached;
    }
    ImageInfo info = readInfo(path);
    SIZE_CACHE.put(key, info);
    return info;
  }

  private static ImageInfo readInfo(String path) {
    boolean svg = isSvgPath(path);
    if (svg) {
      try {
        SvgCacheEntry entry =
            SvgCache.loadSvg(new SvgFile(path, NoteImageSupport.class.getClassLoader()));
        int w = Math.max(1, Math.round(entry.getWidth()));
        int h = Math.max(1, Math.round(entry.getHeight()));
        return new ImageInfo(path, w, h, true, true);
      } catch (Exception e) {
        return new ImageInfo(path, 0, 0, false, true);
      }
    }
    try (InputStream in = HopVfs.getInputStream(path)) {
      BufferedImage image = ImageIO.read(in);
      if (image == null) {
        return new ImageInfo(path, 0, 0, false, false);
      }
      return new ImageInfo(path, image.getWidth(), image.getHeight(), true, false);
    } catch (Exception e) {
      return new ImageInfo(path, 0, 0, false, false);
    }
  }

  public static boolean isSvgPath(String path) {
    if (Utils.isEmpty(path)) {
      return false;
    }
    String lower = path.toLowerCase(Locale.ROOT);
    int q = lower.indexOf('?');
    if (q >= 0) {
      lower = lower.substring(0, q);
    }
    return lower.endsWith(".svg");
  }

  /** Fit image into max box preserving aspect ratio. */
  public static Point fit(int srcW, int srcH, int maxWidth, int maxHeight) {
    if (srcW <= 0 || srcH <= 0) {
      return new Point(Math.max(1, maxWidth), Math.max(1, maxHeight));
    }
    int mw = Math.max(1, maxWidth);
    int mh = Math.max(1, maxHeight);
    double scale = Math.min((double) mw / srcW, (double) mh / srcH);
    if (scale > 1.0) {
      scale = 1.0; // do not upscale small icons excessively on canvas
    }
    int w = Math.max(1, (int) Math.round(srcW * scale));
    int h = Math.max(1, (int) Math.round(srcH * scale));
    return new Point(w, h);
  }

  /** Test helper / memory bound. */
  static void clearCache() {
    SIZE_CACHE.clear();
  }
}
