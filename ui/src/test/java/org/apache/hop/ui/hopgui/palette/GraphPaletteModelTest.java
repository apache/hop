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

package org.apache.hop.ui.hopgui.palette;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.ui.hopgui.context.GuiActionFavorites;
import org.apache.hop.ui.hopgui.palette.GraphPaletteModel.Category;
import org.apache.hop.ui.hopgui.palette.GraphPaletteModel.Item;
import org.junit.jupiter.api.Test;

class GraphPaletteModelTest {

  private static Item item(String id, String name, String category, String... keywords) {
    return new Item(
        id,
        GuiActionFavorites.createId(GuiActionFavorites.Kind.TRANSFORM, id),
        name,
        category,
        name + " description",
        "ui/images/" + id + ".svg",
        List.of(keywords));
  }

  @Test
  void favoritesCategoryIsFirstAndKeepsStoredOrder() {
    Item csv = item("CsvInput", "CSV file input", "Input");
    Item table = item("TableInput", "Table input", "Input");
    Item text = item("TextFileInput", "Text file input", "Input");
    Item dummy = item("Dummy", "Dummy (do nothing)", "Flow");

    GraphPaletteModel model =
        GraphPaletteModel.fromItems(
            List.of(csv, table, text, dummy), List.of("TextFileInput", "CsvInput"), "Favorites");

    List<Category> categories = model.getCategories();
    assertEquals("Favorites", categories.get(0).name());
    assertEquals(List.of("TextFileInput", "CsvInput"), pluginIds(categories.get(0)));
    assertEquals("Flow", categories.get(1).name());
    assertEquals("Input", categories.get(2).name());
    assertEquals(
        List.of("CSV file input", "Table input", "Text file input"), names(categories.get(2)));
  }

  @Test
  void emptyFavoritesOmitsTheCategory() {
    Item dummy = item("Dummy", "Dummy (do nothing)", "Flow");
    GraphPaletteModel model = GraphPaletteModel.fromItems(List.of(dummy), List.of(), "Favorites");
    assertEquals(1, model.getCategories().size());
    assertEquals("Flow", model.getCategories().get(0).name());
  }

  @Test
  void unknownFavoriteIdsAreIgnored() {
    Item dummy = item("Dummy", "Dummy (do nothing)", "Flow");
    GraphPaletteModel model =
        GraphPaletteModel.fromItems(List.of(dummy), List.of("DoesNotExist"), "Favorites");
    assertEquals(1, model.getCategories().size());
    assertEquals("Flow", model.getCategories().get(0).name());
  }

  @Test
  void itemsAndCategoriesAreAlphabetical() {
    Item zebra = item("Zebra", "Zebra", "Zeta");
    Item alpha = item("Alpha", "Alpha", "Zeta");
    Item mid = item("Mid", "Mid", "Alpha");
    GraphPaletteModel model =
        GraphPaletteModel.fromItems(List.of(zebra, alpha, mid), List.of(), "Favorites");
    assertEquals(
        List.of("Alpha", "Zeta"), model.getCategories().stream().map(Category::name).toList());
    assertEquals(List.of("Alpha", "Zebra"), names(model.getCategories().get(1)));
  }

  @Test
  void filterDropsEmptyCategoriesAndMatchesNameKeywordAndCategory() {
    Item csv = item("CsvInput", "CSV file input", "Input", "file");
    Item table = item("TableInput", "Table input", "Input");
    Item dummy = item("Dummy", "Dummy (do nothing)", "Flow");
    GraphPaletteModel model =
        GraphPaletteModel.fromItems(List.of(csv, table, dummy), List.of(), "Favorites");

    List<Category> csvHit = model.filter("csv");
    assertEquals(1, csvHit.size());
    assertEquals("Input", csvHit.get(0).name());
    assertEquals(List.of("CSV file input"), names(csvHit.get(0)));

    List<Category> fileHit = model.filter("file");
    assertEquals(1, fileHit.size());
    assertEquals(List.of("CSV file input"), names(fileHit.get(0)));

    List<Category> flowHit = model.filter("flow");
    assertEquals(1, flowHit.size());
    assertEquals("Flow", flowHit.get(0).name());
    assertEquals(List.of("Dummy (do nothing)"), names(flowHit.get(0)));

    assertTrue(model.filter("zzz-no-such-plugin").isEmpty());
  }

  @Test
  void blankFilterReturnsEveryCategory() {
    Item dummy = item("Dummy", "Dummy (do nothing)", "Flow");
    GraphPaletteModel model = GraphPaletteModel.fromItems(List.of(dummy), List.of(), "Favorites");
    assertEquals(model.getCategories(), model.filter(""));
    assertEquals(model.getCategories(), model.filter(null));
  }

  @Test
  void actionIdUsesCreatePrefix() {
    Item dummy = item("Dummy", "Dummy (do nothing)", "Flow");
    assertEquals("pipeline-graph-create-transform-Dummy", dummy.actionId());
  }

  private static List<String> pluginIds(Category category) {
    return category.items().stream().map(Item::pluginId).toList();
  }

  private static List<String> names(Category category) {
    return category.items().stream().map(Item::name).toList();
  }
}
