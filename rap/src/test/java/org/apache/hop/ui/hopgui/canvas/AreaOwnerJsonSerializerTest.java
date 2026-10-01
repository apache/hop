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

package org.apache.hop.ui.hopgui.canvas;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.DPoint;
import org.apache.hop.core.gui.plugin.action.GuiAction;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.apache.hop.ui.core.dialog.ContextDialog;
import org.eclipse.rap.json.JsonArray;
import org.eclipse.rap.json.JsonObject;
import org.junit.jupiter.api.Test;

class AreaOwnerJsonSerializerTest {

  @Test
  void testSerializeContextDialogItemAndCategory() {
    GuiAction action =
        new GuiAction(
            "pipeline-action-table-input",
            GuiActionType.Create,
            "Table input",
            "Read data from a database table",
            "ui/images/table.svg",
            (a, b, c) -> {});
    action.setCategory("Input");

    ContextDialog.Item item = new ContextDialog.Item(action, null);
    AreaOwner itemArea =
        new AreaOwner(
            AreaOwner.AreaType.CUSTOM,
            10,
            20,
            80,
            60,
            new DPoint(0, 0),
            ContextDialog.OwnerType.ITEM,
            item);

    ContextDialog.CategoryAndOrder category =
        new ContextDialog.CategoryAndOrder("Input", "001", false);
    AreaOwner categoryArea =
        new AreaOwner(
            AreaOwner.AreaType.CUSTOM,
            10,
            5,
            200,
            15,
            new DPoint(0, 0),
            ContextDialog.OwnerType.CATEGORY,
            category);

    JsonArray array = AreaOwnerJsonSerializer.toJsonArray(List.of(itemArea, categoryArea));
    assertNotNull(array);
    assertEquals(2, array.size());

    JsonObject itemJson = array.get(0).asObject();
    assertEquals(10, itemJson.get("x").asInt());
    assertEquals(20, itemJson.get("y").asInt());
    assertEquals(80, itemJson.get("width").asInt());
    assertEquals(60, itemJson.get("height").asInt());
    JsonObject itemOwner = itemJson.get("owner").asObject();
    assertEquals("contextItem", itemOwner.get("kind").asString());
    assertEquals("Table input", itemOwner.get("name").asString());
    assertEquals("pipeline-action-table-input", itemOwner.get("actionId").asString());
    assertEquals("Read data from a database table", itemOwner.get("tooltip").asString());

    JsonObject catJson = array.get(1).asObject();
    JsonObject catOwner = catJson.get("owner").asObject();
    assertEquals("contextCategory", catOwner.get("kind").asString());
    assertEquals("Input", catOwner.get("category").asString());
    assertEquals(false, catOwner.get("collapsed").asBoolean());
  }
}
