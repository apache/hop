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

package org.apache.hop.core.gui.plugin;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Field;
import org.apache.hop.core.naming.NamingSchemeKinds;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.junit.jupiter.api.Test;

class GuiElementsNamingSchemeTypeTest {

  static class Sample {
    @GuiWidgetElement(id = "file", type = GuiElementType.FILENAME, parentId = "p", label = "File")
    String file;

    @GuiWidgetElement(id = "folder", type = GuiElementType.FOLDER, parentId = "p", label = "Folder")
    String folder;

    @GuiWidgetElement(
        id = "explicit",
        type = GuiElementType.TEXT,
        parentId = "p",
        label = "Var",
        namingSchemeType = "hop-variable")
    String explicit;

    @GuiWidgetElement(id = "fromMeta", type = GuiElementType.TEXT, parentId = "p", label = "Meta")
    @HopMetadataProperty(namingSchemeType = "hop-field")
    String fromMeta;

    @GuiWidgetElement(id = "plain", type = GuiElementType.TEXT, parentId = "p", label = "Plain")
    String plain;
  }

  @Test
  void infersFileAndFolderFromElementType() throws Exception {
    assertEquals(NamingSchemeKinds.FILE, elements("file").getNamingSchemeType());
    assertEquals(NamingSchemeKinds.FOLDER, elements("folder").getNamingSchemeType());
  }

  @Test
  void explicitAnnotationWins() throws Exception {
    assertEquals("hop-variable", elements("explicit").getNamingSchemeType());
  }

  @Test
  void copiesFromHopMetadataProperty() throws Exception {
    assertEquals("hop-field", elements("fromMeta").getNamingSchemeType());
  }

  @Test
  void plainTextIsNotANameField() throws Exception {
    assertEquals("", elements("plain").getNamingSchemeType());
  }

  private static GuiElements elements(String fieldName) throws Exception {
    Field field = Sample.class.getDeclaredField(fieldName);
    GuiWidgetElement annotation = field.getAnnotation(GuiWidgetElement.class);
    return new GuiElements(annotation, field);
  }
}
