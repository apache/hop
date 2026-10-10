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
package org.apache.hop.beam.transforms.elasticsearch;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.stream.Stream;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class BeamElasticsearchPluginTest {
  @BeforeAll
  static void init() throws Exception {
    BeamHop.init();
  }

  static Stream<Class<?>> metas() {
    return Stream.of(BeamElasticsearchInputMeta.class, BeamElasticsearchOutputMeta.class);
  }

  @ParameterizedTest
  @MethodSource("metas")
  void discoversBeamOnlyPluginsAndAllMetadataHasGroupedLocalizedWidgets(Class<?> type) {
    Transform annotation = type.getAnnotation(Transform.class);
    assertNotNull(annotation, "transform must be discoverable");
    assertArrayEquals(new String[] {"Beam*"}, annotation.supportedEngines());
    assertNotNull(
        PluginRegistry.getInstance().findPluginWithId(TransformPluginType.class, annotation.id()));
    assertNotNull(type.getAnnotation(GuiPlugin.class));
    for (Field field : type.getDeclaredFields()) {
      if (field.getAnnotation(HopMetadataProperty.class) == null) {
        continue;
      }
      GuiWidgetElement widget = field.getAnnotation(GuiWidgetElement.class);
      assertNotNull(widget, field.getName() + " must be editable in the GUI");
      assertNotEquals(GuiWidgetGroupType.NONE, widget.groupType());
      assertFalse(widget.group().isBlank());
      for (String key : new String[] {widget.label(), widget.toolTip()}) {
        assertTrue(key.startsWith("i18n::"));
        assertNotEquals(key.substring(6), BaseMessages.getString(type, key.substring(6)));
      }
    }
  }
}
