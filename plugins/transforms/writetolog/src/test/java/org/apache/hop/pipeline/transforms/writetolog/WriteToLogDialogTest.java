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

package org.apache.hop.pipeline.transforms.writetolog;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.GlobalMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotCCombo;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * SWTBot coverage for the log level combo of {@link WriteToLogDialog} (#8603). The combo shows the
 * translated {@link LogLevel} descriptions, so OK must map the selection back by position and never
 * by label: "Row Level (very detailed)" is not the code "Rowlevel", and in most languages none of
 * the labels match a code at all.
 *
 * <p>The descriptions are resolved once per JVM, so the other languages are exercised by loading
 * each shipped translation into the combo before selecting - exactly what the dialog shows when Hop
 * GUI runs in that language.
 */
@Tag("uitest")
class WriteToLogDialogTest extends SwtBotTestBase {

  private static final String TRANSFORM_NAME = "write to log";
  private static final String SHELL_TITLE = "Write to log";
  private static final String MESSAGES_PATH =
      "/org/apache/hop/core/logging/messages/messages_%s.properties";
  private static final String DEFAULT_LOCALE = "en_US";

  static Stream<String> localeCodes() {
    return Arrays.stream(GlobalMessages.localeCodes);
  }

  /**
   * The dialog now builds its tabs from the annotated widgets on {@link WriteToLogMeta}. Those are
   * looked up in the registry by class name, and the unit-test JVM does not always run the {@code
   * GuiPluginType} scan, so register them the way that scan does.
   */
  @BeforeAll
  static void registerMetaWidgets() {
    GuiRegistry registry = GuiRegistry.getInstance();
    if (registry.findGuiElements(
            WriteToLogMeta.class.getName(), WriteToLogMeta.GUI_PLUGIN_ELEMENT_PARENT_ID)
        != null) {
      return;
    }
    for (Field field : WriteToLogMeta.class.getDeclaredFields()) {
      GuiWidgetElement element = field.getAnnotation(GuiWidgetElement.class);
      if (element != null) {
        registry.addGuiWidgetElement(WriteToLogMeta.class.getName(), element, field);
      }
    }
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("localeCodes")
  void okStoresEverySelectedLogLevel(String localeCode) throws IOException {
    String[] labels = logLevelLabels(localeCode);
    assertEquals(
        LogLevel.values().length,
        Arrays.stream(labels).distinct().count(),
        "log level labels of "
            + localeCode
            + " must be distinguishable: "
            + Arrays.toString(labels));

    for (LogLevel level : LogLevel.values()) {
      WriteToLogMeta meta = new WriteToLogMeta();
      meta.setLogLevel(level == LogLevel.BASIC ? LogLevel.DEBUG : LogLevel.BASIC);
      PipelineMeta pipelineMeta = pipelineWith(meta);

      withDialog(
          parent -> new WriteToLogDialog(parent, new Variables(), meta, pipelineMeta).open(),
          bot -> {
            SWTBot dialog = bot.shell(SHELL_TITLE).activate().bot();
            SWTBotCCombo combo = dialog.ccomboBox(0);
            assertArrayEquals(
                LogLevel.getLogLevelDescriptions(),
                combo.items(),
                "the combo must list the log level descriptions in enum order");

            display.syncExec(() -> combo.widget.setItems(labels));
            combo.setSelection(level.ordinal());
            assertEquals(labels[level.ordinal()], combo.getText());

            dialog.button(buttonLabel("System.Button.OK")).click();
          });

      assertEquals(
          level,
          meta.getLogLevel(),
          "selecting '" + labels[level.ordinal()] + "' (" + localeCode + ") must store " + level);
    }
  }

  @Test
  void reopeningShowsTheStoredLogLevel() {
    for (LogLevel level : LogLevel.values()) {
      WriteToLogMeta meta = new WriteToLogMeta();
      meta.setLogLevel(level);
      PipelineMeta pipelineMeta = pipelineWith(meta);

      withDialog(
          parent -> new WriteToLogDialog(parent, new Variables(), meta, pipelineMeta).open(),
          bot -> {
            SWTBot dialog = bot.shell(SHELL_TITLE).activate().bot();
            assertEquals(level.getDescription(), dialog.ccomboBox(0).getText());
            dialog.button(buttonLabel("System.Button.OK")).click();
          });

      assertEquals(level, meta.getLogLevel(), "OK without changes must keep " + level);
    }
  }

  /**
   * The log level combo lives in an extra group on the Options tab, alongside the annotated fields.
   * Anchoring it to the top of the tab composite instead of below the last annotated widget drew it
   * on top of the Print header row (#8655). Compare the actual control rectangles so the row cannot
   * silently land on another one again.
   */
  @Test
  void theLogLevelRowDoesNotOverlapTheOtherOptionsFields() {
    WriteToLogMeta meta = new WriteToLogMeta();
    PipelineMeta pipelineMeta = pipelineWith(meta);

    withDialog(
        parent -> new WriteToLogDialog(parent, new Variables(), meta, pipelineMeta).open(),
        bot -> {
          SWTBot dialog = bot.shell(SHELL_TITLE).activate().bot();

          Rectangle[] logLevel = new Rectangle[1];
          List<Rectangle> checkboxes = new ArrayList<>();
          display.syncExec(
              () -> {
                for (Rectangle rectangle : controlsOfType(dialogShell(), CCombo.class)) {
                  logLevel[0] = rectangle;
                }
                for (Rectangle rectangle : controlsOfType(dialogShell(), Button.class)) {
                  if (rectangle.width > 0 && rectangle.height > 0) {
                    checkboxes.add(rectangle);
                  }
                }
              });

          assertNotNull(logLevel[0], "expected the log level combo on the Options tab");
          for (Rectangle checkbox : checkboxes) {
            assertFalse(
                logLevel[0].intersects(checkbox),
                "the log level combo at " + logLevel[0] + " overlaps a checkbox at " + checkbox);
          }

          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });
  }

  private static Shell dialogShell() {
    for (Shell shell : display.getShells()) {
      if (SHELL_TITLE.equals(shell.getText()) && !shell.isDisposed()) {
        return shell;
      }
    }
    throw new AssertionError("the '" + SHELL_TITLE + "' dialog is not open");
  }

  /**
   * Every visible control of the given type below the dialog shell, including in tab composites.
   */
  private static List<Rectangle> controlsOfType(Shell shell, Class<? extends Control> type) {
    List<Rectangle> found = new ArrayList<>();
    collect(shell, type, found);
    return found;
  }

  private static void collect(
      Control parent, Class<? extends Control> type, List<Rectangle> found) {
    if (parent instanceof Composite composite) {
      for (Control child : composite.getChildren()) {
        if (!child.isDisposed() && type.isInstance(child)) {
          found.add(child.getBounds());
        }
        collect(child, type, found);
      }
    }
  }

  /**
   * The log level labels a Hop GUI running in the given language shows, in enum order. Keys a
   * translation lacks fall back to en_US, like {@code BaseMessages} does.
   */
  private static String[] logLevelLabels(String localeCode) throws IOException {
    Properties fallback = loadMessages(DEFAULT_LOCALE);
    Properties messages = loadMessages(localeCode);
    return Arrays.stream(LogLevel.values())
        .map(level -> "LogWriter.Level." + level.getCode() + ".LongDesc")
        .map(key -> messages.getProperty(key, fallback.getProperty(key)))
        .toArray(String[]::new);
  }

  private static Properties loadMessages(String localeCode) throws IOException {
    Properties properties = new Properties();
    try (InputStream in =
        LogLevel.class.getResourceAsStream(String.format(MESSAGES_PATH, localeCode))) {
      if (in != null) {
        properties.load(new InputStreamReader(in, StandardCharsets.UTF_8));
      }
    }
    return properties;
  }

  private static PipelineMeta pipelineWith(WriteToLogMeta meta) {
    String pluginId = PluginRegistry.getInstance().getPluginId(TransformPluginType.class, meta);
    assertNotNull(pluginId, "Write to log transform plugin must be registered");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(new TransformMeta(pluginId, TRANSFORM_NAME, meta));
    return pipelineMeta;
  }
}
