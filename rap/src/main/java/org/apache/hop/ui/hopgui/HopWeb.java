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

package org.apache.hop.ui.hopgui;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.batik.transcoder.TranscoderException;
import org.apache.batik.transcoder.TranscoderInput;
import org.apache.batik.transcoder.TranscoderOutput;
import org.apache.batik.transcoder.image.PNGTranscoder;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarItem;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.svg.SvgCache;
import org.apache.hop.core.svg.SvgCacheEntry;
import org.apache.hop.core.svg.SvgFile;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.plugin.MetadataPluginType;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.hopgui.perspective.HopPerspectivePluginType;
import org.eclipse.rap.rwt.application.Application;
import org.eclipse.rap.rwt.application.ApplicationConfiguration;
import org.eclipse.rap.rwt.client.WebClient;
import org.eclipse.rap.rwt.service.ResourceLoader;

public class HopWeb implements ApplicationConfiguration {

  public static final String HOP_WEB_THEME = "HOP_WEB_THEME";
  public static final String CONST_LIGHT = "light";

  @Override
  public void configure(Application application) {

    try {
      // Hop initialization is already done here.
      // This means we can simply ask the gui registry for the toolbar images to register.
      // Let's add all toolbar SVG files as static resources in the application.
      // In GuiToolbarWidgets we can then add an exception for it.
      //
      GuiRegistry registry = GuiRegistry.getInstance();
      Map<String, Map<String, GuiToolbarItem>> guiToolbarMap = registry.getGuiToolbarMap();
      for (String toolbarId : guiToolbarMap.keySet()) {
        Map<String, GuiToolbarItem> itemMap = guiToolbarMap.get(toolbarId);
        for (String itemId : itemMap.keySet()) {
          final GuiToolbarItem item = itemMap.get(itemId);
          addResource(application, item.getImage(), item.getClassLoader());
        }
      }

      // Register alternate images for toolbar toggles (e.g. show/hide, show-all/show-selected)
      // so setToolbarItemImage() can switch icons in RWT without "Resource does not exist"
      ClassLoader uiClassLoader = HopWeb.class.getClassLoader();
      for (String path :
          new String[] {
            "ui/images/show.svg",
            "ui/images/hide.svg",
            "ui/images/show-all.svg",
            "ui/images/show-selected.svg"
          }) {
        addResource(application, path, uiClassLoader);
      }

      // Find metadata, perspective plugins
      //
      List<IPlugin> plugins = PluginRegistry.getInstance().getPlugins(MetadataPluginType.class);
      plugins.addAll(PluginRegistry.getInstance().getPlugins(HopPerspectivePluginType.class));

      // Add the plugin images as resources
      //
      for (IPlugin plugin : plugins) {
        ClassLoader classLoader = PluginRegistry.getInstance().getClassLoader(plugin);
        addResource(application, plugin.getImageFile(), classLoader);
      }
    } catch (Exception e) {
      LogChannel.UI.logError("General exception", e);
    }

    application.addResource(
        "ui/images/logo_icon.png",
        new ResourceLoader() {
          @Override
          public InputStream getResourceAsStream(String resourceName) {
            // Convert svg to png without Display
            PNGTranscoder t = new PNGTranscoder();
            InputStream inputStream =
                this.getClass().getClassLoader().getResourceAsStream("ui/images/logo_icon.svg");
            TranscoderInput input = new TranscoderInput(inputStream);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            TranscoderOutput output = new TranscoderOutput(outputStream);
            try {
              t.transcode(input, output);
            } catch (TranscoderException e) {
              LogChannel.UI.logError("Transcoder exception", e);
            }
            return new ByteArrayInputStream(outputStream.toByteArray());
          }
        });
    Stream.of("org/apache/hop/ui/hopgui/clipboard.js", "org/apache/hop/ui/hopgui/canvas-zoom.js")
        .forEach(
            str ->
                application.addResource(
                    "js/" + FilenameUtils.getName(str),
                    new ResourceLoader() {
                      @Override
                      public InputStream getResourceAsStream(String resourceName) {
                        return this.getClass().getClassLoader().getResourceAsStream(str);
                      }
                    }));

    // Only 2 choices for now
    //
    application.addStyleSheet("dark", "org/apache/hop/ui/hopgui/dark-mode.css");
    application.addStyleSheet(CONST_LIGHT, "org/apache/hop/ui/hopgui/light-mode.css");

    String themeId = System.getProperty(HOP_WEB_THEME, CONST_LIGHT);
    if ("dark".equalsIgnoreCase(themeId)) {
      themeId = "dark";
      PropsUi.getInstance().setDarkMode(true);
      LogChannel.UI.logBasic("Hop web: enabled dark mode rendering");
    } else {
      themeId = CONST_LIGHT;
      PropsUi.getInstance().setDarkMode(false);
    }
    LogChannel.UI.logBasic("Hop web: selected theme is: " + themeId);

    Map<String, String> properties = new HashMap<>();
    properties.put(WebClient.PAGE_TITLE, "Apache Hop Web");
    properties.put(WebClient.FAVICON, "ui/images/logo_icon.png");
    properties.put(WebClient.THEME_ID, themeId);
    properties.put(WebClient.HEAD_HTML, readTextFromResource("head.html"));
    application.addEntryPoint("/ui", HopWebEntryPoint.class, properties);
    application.setOperationMode(Application.OperationMode.SWT_COMPATIBILITY);

    // Print some important system settings...
    //
    LogChannel.UI.logBasic("HOP_CONFIG_FOLDER: " + Const.HOP_CONFIG_FOLDER);
    LogChannel.UI.logBasic("HOP_AUDIT_FOLDER: " + Const.HOP_AUDIT_FOLDER);
    LogChannel.UI.logBasic("HOP_GUI_ZOOM_FACTOR: " + System.getProperty("HOP_GUI_ZOOM_FACTOR"));
  }

  private void addResource(
      Application application, final String imageFilename, final ClassLoader classLoader) {
    if (StringUtils.isEmpty(imageFilename)) {
      return;
    }
    // See if the resource was already added.q
    //
    if (SvgCache.findSvg(imageFilename) != null) {
      return;
    }

    ResourceLoader loader =
        filename -> {
          try {
            SvgFile svgFile = new SvgFile(filename, classLoader);
            SvgCacheEntry cacheEntry = SvgCache.loadSvg(svgFile);
            String svgXml = XmlHandler.getXmlString(cacheEntry.getSvgDocument(), false, false);
            return new ByteArrayInputStream(svgXml.getBytes(StandardCharsets.UTF_8));
          } catch (Exception e) {
            throw new RuntimeException("Error loading SVG resource filename: " + imageFilename, e);
          }
        };
    application.addResource(imageFilename, loader);
  }

  private static String readTextFromResource(String resourceName) {
    String result;
    try {
      ClassLoader classLoader = HopWeb.class.getClassLoader();
      InputStream inputStream = classLoader.getResourceAsStream(resourceName);
      try (inputStream) {
        if (inputStream == null) {
          throw new RuntimeException("Resource not found: " + resourceName);
        }
        BufferedReader reader =
            new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        StringBuilder stringBuilder = new StringBuilder();
        String line = reader.readLine();
        while (line != null) {
          stringBuilder.append(line);
          stringBuilder.append('\n');
          line = reader.readLine();
        }
        result = stringBuilder.toString();
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to read text from resource: " + resourceName);
    }
    return result;
  }
}
