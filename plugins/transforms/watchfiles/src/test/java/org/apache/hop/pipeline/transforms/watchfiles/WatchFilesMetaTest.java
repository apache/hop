/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.watchfiles;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.pipeline.PipelineMeta;
import org.junit.jupiter.api.Test;

class WatchFilesMetaTest {
  @Test
  void newWatchIdentityIsPersistentAndLegacyIdentityIsPreserved() throws Exception {
    WatchFilesMeta fresh = new WatchFilesMeta();
    fresh.setDefault();
    String id = fresh.getWatchId();
    assertTrue(id.matches("watch-[a-f0-9-]{36}"));
    assertFalse(fresh.isEditWatchId());
    assertFalse(fresh.getStateDirectory().isBlank());
    WatchFilesMeta another = new WatchFilesMeta();
    another.setDefault();
    assertFalse(id.equals(another.getWatchId()));
    WatchFilesMeta loaded =
        XmlMetadataUtil.deSerializeFromXml(
            XmlHandler.getSubNode(
                XmlHandler.loadXmlString("<transform>" + fresh.getXml() + "</transform>"),
                "transform"),
            WatchFilesMeta.class,
            new MemoryMetadataProvider());
    assertEquals(id, loaded.getWatchId());
    assertEquals(fresh.getStateDirectory(), loaded.getStateDirectory());
    fresh.setDefault();
    assertEquals(id, fresh.getWatchId());
    loaded.setWatchId("sample-watch-files2");
    loaded.setDefault();
    assertEquals("sample-watch-files2", loaded.getWatchId());
  }

  @Test
  void guiLabelsKeepPersistedEnumsAndLegacyMissingSettings() throws Exception {
    for (String setting : List.of("strategy", "patternSyntax", "initialScan")) {
      for (String value :
          switch (setting) {
            case "strategy" -> List.of("AUTO", "NATIVE", "POLLING");
            case "patternSyntax" -> List.of("WILDCARD", "REGEXP");
            default -> List.of("EMIT_EXISTING", "IGNORE_EXISTING", "COMPARE_WITH_STATE");
          }) {
        assertEquals(
            value.equals("COMPARE_WITH_STATE") ? "IGNORE_EXISTING" : value,
            WatchFilesMeta.optionValue(setting, WatchFilesMeta.optionLabel(setting, value)));
        assertEquals(value, WatchFilesMeta.optionValue(setting, value));
      }
    }
    WatchFilesMeta legacy =
        XmlMetadataUtil.deSerializeFromXml(
            XmlHandler.getSubNode(
                XmlHandler.loadXmlString("<transform><watchId>legacy</watchId></transform>"),
                "transform"),
            WatchFilesMeta.class,
            new MemoryMetadataProvider());
    assertEquals("legacy", legacy.getWatchId());
    assertFalse(legacy.isEditWatchId());
    assertEquals("", legacy.getStateDirectory());
    assertEquals("REGEXP", legacy.getPatternSyntax());
    assertEquals("COMPARE_WITH_STATE", legacy.getInitialScan());
    assertEquals("", legacy.getMaximumRunTime());
    assertEquals("MINUTES", legacy.getMaximumRunTimeUnit());
  }

  @Test
  void metadataRoundTripKeepsWatchIdAndOptions() throws Exception {
    WatchFilesMeta meta = new WatchFilesMeta();
    meta.setDirectory("${INPUT_FOLDER}");
    meta.setWatchId("telecom-kpi-input");
    meta.setEditWatchId(true);
    meta.setStateDirectory("${STATE_FOLDER}");
    meta.setIncludeSubdirectories(true);
    meta.setDeleted(true);
    meta.setStrategy("POLLING");
    meta.setPatternSyntax("WILDCARD");
    meta.setInitialScan("EMIT_EXISTING");
    meta.setPollingInterval("${POLL_INTERVAL}");
    WatchFilesMeta loaded =
        XmlMetadataUtil.deSerializeFromXml(
            XmlHandler.getSubNode(
                XmlHandler.loadXmlString("<transform>" + meta.getXml() + "</transform>"),
                "transform"),
            WatchFilesMeta.class,
            new MemoryMetadataProvider());
    assertEquals(meta.getDirectory(), loaded.getDirectory());
    assertEquals(meta.getWatchId(), loaded.getWatchId());
    assertTrue(loaded.isEditWatchId());
    assertEquals(meta.getPollingInterval(), loaded.getPollingInterval());
    assertEquals(meta.getInitialScan(), loaded.getInitialScan());
    assertTrue(loaded.isIncludeSubdirectories());
    assertTrue(loaded.isDeleted());
    assertEquals("POLLING", loaded.getStrategy());
    assertEquals("WILDCARD", loaded.getPatternSyntax());
  }

  @Test
  void everyOptionHasGroupedGuiWidgetAndResourceLabels() {
    ResourceBundle bundle =
        ResourceBundle.getBundle(
            "org.apache.hop.pipeline.transforms.watchfiles.messages.messages", Locale.US);
    int count = 0;
    for (Field field : WatchFilesMeta.class.getDeclaredFields()) {
      if (field.getAnnotation(HopMetadataProperty.class) == null) {
        continue;
      }
      GuiWidgetElement widget = field.getAnnotation(GuiWidgetElement.class);
      assertNotNull(widget, field.getName());
      assertEquals(
          field.getName().equals("replayFilter")
              ? GuiWidgetGroupType.BOXES
              : GuiWidgetGroupType.TABS,
          widget.groupType());
      assertEquals(
          field.getName().equals("replayFilter")
              ? WatchFilesMeta.REPLAY_GUI_PARENT_ID
              : WatchFilesMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
          widget.parentId());
      assertFalse(widget.group().isBlank());
      assertTrue(bundle.containsKey(widget.label().substring("i18n::".length())));
      assertTrue(bundle.containsKey(widget.toolTip().substring("i18n::".length())));
      count++;
    }
    assertEquals(27, count);
  }

  @Test
  void outputTypesAndEngineSupportAreExplicit() throws Exception {
    WatchFilesMeta meta = new WatchFilesMeta();
    RowMeta row = new RowMeta();
    meta.getFields(row, "Watch", null, null, new Variables(), new MemoryMetadataProvider());
    assertEquals(13, row.size());
    assertEquals(IValueMeta.TYPE_STRING, row.searchValueMeta("filename").getType());
    assertEquals(IValueMeta.TYPE_INTEGER, row.searchValueMeta("size").getType());
    assertEquals(IValueMeta.TYPE_DATE, row.searchValueMeta("last_modified").getType());
    assertEquals(IValueMeta.TYPE_BOOLEAN, row.searchValueMeta("is_directory").getType());
    assertArrayEquals(
        new String[] {"Local"},
        WatchFilesMeta.class.getAnnotation(Transform.class).supportedEngines());
    assertArrayEquals(
        new PipelineMeta.PipelineType[] {PipelineMeta.PipelineType.Normal},
        meta.getSupportedPipelineTypes());
    assertFalse(meta.consumesMainInput());
    assertTrue(meta.canStartWithoutInput());
  }

  @Test
  void invalidRegexAndIntervalsAreRejectedAndVariablesResolve() {
    WatchFilesMeta meta = new WatchFilesMeta();
    Variables variables = new Variables();
    variables.setVariable("ROOT", "/input");
    variables.setVariable("POLL", "5000");
    meta.setDirectory("${ROOT}");
    meta.setStateDirectory("/state");
    meta.setWatchId("input");
    meta.setPollingInterval("${POLL}");
    assertDoesNotThrow(() -> meta.validate(variables));
    meta.setIncludeWildcard("[");
    assertThrows(IllegalArgumentException.class, () -> meta.validate(variables));
    meta.setIncludeWildcard("");
    meta.setPollingInterval("0");
    assertThrows(IllegalArgumentException.class, () -> meta.validate(variables));
  }

  @Test
  void optionalRuntimeSupportsMinutesHoursVariablesAndRejectsInvalidValues() throws Exception {
    WatchFilesMeta meta = new WatchFilesMeta();
    Variables variables = new Variables();
    assertEquals(0, meta.maximumRunMillis(variables));
    meta.setMaximumRunTime(" 1.5 ");
    assertEquals(90000, meta.maximumRunMillis(variables));
    meta.setMaximumRunTimeUnit("HOURS");
    assertEquals(5400000, meta.maximumRunMillis(variables));
    variables.setVariable("DURATION", "0.01");
    meta.setMaximumRunTime("${DURATION}");
    assertEquals(36000, meta.maximumRunMillis(variables));
    WatchFilesMeta loaded =
        XmlMetadataUtil.deSerializeFromXml(
            XmlHandler.getSubNode(
                XmlHandler.loadXmlString("<transform>" + meta.getXml() + "</transform>"),
                "transform"),
            WatchFilesMeta.class,
            new MemoryMetadataProvider());
    assertEquals("${DURATION}", loaded.getMaximumRunTime());
    assertEquals("HOURS", loaded.getMaximumRunTimeUnit());
    assertEquals(36000, loaded.maximumRunMillis(variables));
    variables.setVariable("DURATION", " ");
    assertEquals(0, meta.maximumRunMillis(variables));
    for (String invalid :
        List.of("0", "-1", "NaN", "1,5", "${MISSING}", "999999999999999999999", "0.00000000001")) {
      meta.setMaximumRunTime(invalid);
      assertThrows(IllegalArgumentException.class, () -> meta.maximumRunMillis(variables), invalid);
    }
    meta.setMaximumRunTime("1");
    meta.setMaximumRunTimeUnit("SECONDS");
    assertThrows(IllegalArgumentException.class, () -> meta.maximumRunMillis(variables));
    assertEquals(
        "HOURS",
        WatchFilesMeta.optionValue(
            "maximumRunTimeUnit", WatchFilesMeta.optionLabel("maximumRunTimeUnit", "HOURS")));
  }
}
