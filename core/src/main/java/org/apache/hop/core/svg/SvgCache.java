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

package org.apache.hop.core.svg;

import java.awt.geom.Rectangle2D;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.CodeSource;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Stream;
import org.apache.batik.anim.dom.SAXSVGDocumentFactory;
import org.apache.batik.bridge.BridgeContext;
import org.apache.batik.bridge.DocumentLoader;
import org.apache.batik.bridge.GVTBuilder;
import org.apache.batik.bridge.UserAgent;
import org.apache.batik.bridge.UserAgentAdapter;
import org.apache.batik.gvt.GraphicsNode;
import org.apache.batik.util.SVGConstants;
import org.apache.batik.util.XMLResourceDescriptor;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.vfs.HopVfs;
import org.w3c.dom.svg.SVGDocument;
import org.w3c.dom.svg.SVGSVGElement;

public class SvgCache {
  private static SvgCache instance;

  private Map<String, SvgCacheEntry> fileDocumentMap;

  private SvgCache() {
    fileDocumentMap = new HashMap<>();
  }

  /**
   * Gets instance
   *
   * @return value of instance
   */
  public static SvgCache getInstance() {
    if (instance == null) {
      instance = new SvgCache();
    }
    return instance;
  }

  /**
   * Parses the SVG images in a folder of the jar or class folder that holds the given class into
   * this cache, in a background thread, so that loading them later only has to draw them. Needs no
   * display and no GUI classes.
   *
   * @param anchor a class from the jar or class folder with the images
   * @param folder the folder, for example {@code ui/images/}
   */
  public static void preloadFolderInBackground(Class<?> anchor, String folder) {
    Thread thread =
        new Thread(
            () -> {
              ClassLoader classLoader = anchor.getClassLoader();
              for (String name : listSvgResources(anchor, folder)) {
                try {
                  loadSvg(new SvgFile(name, classLoader));
                } catch (Exception e) {
                  // Reported when the image is loaded
                }
              }
            },
            "Hop SVG preload");
    thread.setDaemon(true);
    thread.setPriority(Thread.MIN_PRIORITY);
    thread.start();
  }

  /** The SVG resources in a folder of the jar or class folder that holds the class. */
  static List<String> listSvgResources(Class<?> anchor, String folder) {
    List<String> names = new ArrayList<>();
    try {
      CodeSource codeSource = anchor.getProtectionDomain().getCodeSource();
      if (codeSource == null || codeSource.getLocation() == null) {
        return names;
      }
      Path location = Path.of(codeSource.getLocation().toURI());
      if (Files.isDirectory(location)) {
        Path base = location.resolve(folder);
        if (Files.isDirectory(base)) {
          try (Stream<Path> files = Files.walk(base)) {
            files
                .filter(f -> f.toString().endsWith(".svg"))
                .forEach(f -> names.add(location.relativize(f).toString().replace('\\', '/')));
          }
        }
      } else if (Files.isRegularFile(location)) {
        try (JarFile jar = new JarFile(location.toFile())) {
          Enumeration<JarEntry> entries = jar.entries();
          while (entries.hasMoreElements()) {
            String name = entries.nextElement().getName();
            if (name.startsWith(folder) && name.endsWith(".svg")) {
              names.add(name);
            }
          }
        }
      }
    } catch (Exception e) {
      // Nothing is preloaded: the images are parsed when they are loaded
    }
    return names;
  }

  public static synchronized SvgCacheEntry findSvg(String filename) {
    return getInstance().fileDocumentMap.get(filename);
  }

  /**
   * Loads an SVG file into the cache. The file is parsed without holding the cache's lock: Batik
   * can wait for the main thread (the AWT toolkit on macOS), and the main thread may be loading an
   * image itself. Two threads can then parse the same file; the first result is kept.
   */
  public static SvgCacheEntry loadSvg(SvgFile svgFile) throws HopException {

    SvgCacheEntry cacheEntry = findSvg(svgFile.getFilename());
    if (cacheEntry != null) {
      return cacheEntry;
    }

    try {
      String parser = XMLResourceDescriptor.getXMLParserClassName();
      SAXSVGDocumentFactory factory = new SAXSVGDocumentFactory(parser);
      InputStream svgStream = svgFile.getClassLoader().getResourceAsStream(svgFile.getFilename());
      if (svgStream == null) {
        // Retry on the regular filesystem...
        //
        svgStream = HopVfs.getInputStream(svgFile.getFilename());
      }
      SVGDocument svgDocument = factory.createSVGDocument(svgFile.getFilename(), svgStream);
      SVGSVGElement elSVG = svgDocument.getRootElement();

      float width = -1;
      float height = -1;
      float x = 0;
      float y = 0;

      // See if the element has a "width" and a "height" attribute...
      //
      String widthAttribute = elSVG.getAttribute("width");
      String heightAttribute = elSVG.getAttribute("height");
      if (widthAttribute != null && heightAttribute != null) {
        width = (float) Const.toDouble(widthAttribute.replace("px", "").replace("mm", ""), -1.0d);
        height = (float) Const.toDouble(heightAttribute.replace("px", "").replace("mm", ""), -1.0d);
      }
      String xAttribute = elSVG.getAttribute("x");
      String yAttribute = elSVG.getAttribute("y");
      if (xAttribute != null && yAttribute != null) {
        x = (float) Const.toDouble(xAttribute.replace("px", "").replace("mm", ""), 0d);
        y = (float) Const.toDouble(yAttribute.replace("px", "").replace("mm", ""), 0d);
      }

      // If we don't have width and height we'll have to calculate it...
      //
      if (width <= 1 || height <= 1) {
        // Figure out the primitives bounds...
        //
        UserAgent agent = new UserAgentAdapter();
        DocumentLoader loader = new DocumentLoader(agent);
        BridgeContext context = new BridgeContext(agent, loader);
        context.setDynamic(true);
        GVTBuilder builder = new GVTBuilder();
        GraphicsNode root = builder.build(context, svgDocument);

        // We need to go through the document to figure it out, unfortunately.
        // It is slower but should always work.
        //
        Rectangle2D primitiveBounds = root.getPrimitiveBounds();

        width = (float) primitiveBounds.getWidth();
        height = (float) primitiveBounds.getHeight();
        x = (float) primitiveBounds.getX();
        y = (float) primitiveBounds.getY();

        if (width <= 1 || height <= 1) {
          // See if we can use a viewbox...
          //
          String attributeNS = elSVG.getAttributeNS(null, SVGConstants.SVG_VIEW_BOX_ATTRIBUTE);
          if (StringUtils.isNotEmpty(attributeNS)) {
            String[] parts = attributeNS.split(" ");
            if (parts.length == 4) {
              // Usually this is in the form "0 0 100 200" : x y width height
              //
              width = (float) Const.toDouble(parts[2], 0.0);
              height = (float) Const.toDouble(parts[3], 0.0);
            }
          }
        }
      }

      if (width <= 1 || height <= 1) {
        throw new HopException(
            "Couldn't determine width or height of file : " + svgFile.getFilename());
      }

      cacheEntry =
          new SvgCacheEntry(
              svgFile.getFilename(),
              svgDocument,
              Math.round(width),
              Math.round(height),
              Math.round(x),
              Math.round(y));
      synchronized (SvgCache.class) {
        SvgCacheEntry existing = findSvg(svgFile.getFilename());
        if (existing != null) {
          return existing;
        }
        getInstance().fileDocumentMap.put(svgFile.getFilename(), cacheEntry);
      }
      return cacheEntry;
    } catch (Exception e) {
      throw new HopException("Error loading SVG file " + svgFile.getFilename(), e);
    }
  }

  public static synchronized void addSvg(
      String filename, SVGDocument svgDocument, int width, int height, int x, int y) {
    getInstance()
        .fileDocumentMap
        .put(filename, new SvgCacheEntry(filename, svgDocument, width, height, x, y));
  }

  public void clear() {
    fileDocumentMap.clear();
  }
}
