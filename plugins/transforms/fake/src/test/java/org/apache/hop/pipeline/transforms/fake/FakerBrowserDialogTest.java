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

package org.apache.hop.pipeline.transforms.fake;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import net.datafaker.Faker;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * End-to-end SWTBot coverage for the {@link FakerBrowserDialog}, the searchable browser over the
 * {@link FakerCatalog}. It exercises the live search filter, selecting a generator and confirming
 * (OK returns the built {@link FakeField}), the "Add field" callback, and cancelling. The dialog
 * runs its own blocking event loop in {@code open()}, so {@link SwtBotTestBase#withDialog} pumps it
 * on the UI thread while the assertions drive it from a worker thread.
 *
 * <p>Tagged {@code uitest} so it is only run with a display; run with {@code mvn -pl
 * plugins/transforms/fake -Puitest test}.
 */
@Tag("uitest")
class FakerBrowserDialogTest extends SwtBotTestBase {

  private static final Class<?> PKG = FakerBrowserDialog.class;
  private static final String SHELL_TITLE =
      BaseMessages.getString(PKG, "FakerBrowserDialog.DialogTitle");
  private static final String SEARCH_TOOLTIP =
      BaseMessages.getString(PKG, "FakerBrowserDialog.Search.Tooltip");
  private static final String NAME_TOOLTIP =
      BaseMessages.getString(PKG, "FakerBrowserDialog.Name.Tooltip");
  private static final String ADD_FIELD_BUTTON =
      BaseMessages.getString(PKG, "FakerBrowserDialog.AddField.Button");
  private static final String SHOW_ALL_LABEL =
      BaseMessages.getString(PKG, "FakerBrowserDialog.ShowAll.Label");

  @Test
  void searchNarrowsTheCategoryTreeAndCancelReturnsNull() {
    AtomicReference<FakeField> picked = new AtomicReference<>();

    withDialog(
        parent ->
            picked.set(
                new FakerBrowserDialog(
                        parent, new Variables(), new Faker(), new FakeField(), field -> {})
                    .open()),
        bot -> {
          SWTBot dialog = bot.shell(SHELL_TITLE).activate().bot();

          int unfiltered = dialog.tree().getAllItems().length;
          assertTrue(unfiltered > 20, "The tree should list many categories: " + unfiltered);

          dialog.textWithTooltip(SEARCH_TOOLTIP).setText("firstname");
          int filtered = dialog.tree().getAllItems().length;
          assertTrue(filtered > 0, "The search should still match at least one category");
          assertTrue(filtered < unfiltered, "The search should narrow the category tree");

          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });

    assertNull(picked.get(), "Cancel must return no generator");
  }

  @Test
  void selectingAGeneratorAndConfirmingReturnsAUsableField() throws Exception {
    AtomicReference<FakeField> picked = new AtomicReference<>();

    withDialog(
        parent ->
            picked.set(
                new FakerBrowserDialog(
                        parent, new Variables(), new Faker(), new FakeField(), field -> {})
                    .open()),
        bot -> {
          SWTBot dialog = bot.shell(SHELL_TITLE).activate().bot();
          dialog.textWithTooltip(SEARCH_TOOLTIP).setText("firstname");
          dialog.textWithTooltip(NAME_TOOLTIP).setText("myField");
          // Expand the Name category and pick the First Name function.
          dialog.tree().getTreeItem("Name").expand().getNode("First Name").select();
          dialog.button(buttonLabel("System.Button.OK")).click();
        });

    FakeField field = picked.get();
    assertNotNull(field, "OK must return the selected generator");
    assertEquals("myField", field.getName());
    assertEquals("name", field.getType());
    assertEquals("firstName", field.getTopic());
    // The returned field must be a fully resolvable generator.
    assertNotNull(FakerCatalog.bind(new Faker(), field, new Variables()).produce());
  }

  @Test
  void reopeningWithAConfiguredFieldPreselectsItSoOkReturnsItWithoutAClick() {
    // The field already points at name.firstName: the tree must focus it so a bare OK returns it.
    FakeField current = new FakeField("first_name", "name", "firstName");
    AtomicReference<FakeField> picked = new AtomicReference<>();

    withDialog(
        parent ->
            picked.set(
                new FakerBrowserDialog(parent, new Variables(), new Faker(), current, field -> {})
                    .open()),
        bot -> {
          SWTBot dialog = bot.shell(SHELL_TITLE).activate().bot();
          dialog.button(buttonLabel("System.Button.OK")).click();
        });

    FakeField field = picked.get();
    assertNotNull(field, "the configured generator must be pre-selected so OK returns it");
    assertEquals("name", field.getType());
    assertEquals("firstName", field.getTopic());
    assertEquals("first_name", field.getName());
  }

  @Test
  void showAllToggleRevealsTheHiddenGenerators() {
    withDialog(
        parent ->
            new FakerBrowserDialog(parent, new Variables(), new Faker(), new FakeField(), f -> {})
                .open(),
        bot -> {
          SWTBot dialog = bot.shell(SHELL_TITLE).activate().bot();

          int featuredOnly = dialog.tree().getAllItems().length;
          assertTrue(featuredOnly > 0, "the featured providers should show by default");

          dialog.checkBox(SHOW_ALL_LABEL).click();
          int everything = dialog.tree().getAllItems().length;
          assertTrue(
              everything > featuredOnly,
              "'Show all' must reveal more categories ("
                  + everything
                  + " vs "
                  + featuredOnly
                  + ")");

          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });
  }

  @Test
  void aHiddenConfiguredGeneratorAutoEnablesShowAllAndIsPreselected() {
    // harryPotter is not a featured provider, so opening on it must flip "Show all" and focus it.
    FakeField current = new FakeField("hero", "harryPotter", "character");
    AtomicReference<FakeField> picked = new AtomicReference<>();

    withDialog(
        parent ->
            picked.set(
                new FakerBrowserDialog(parent, new Variables(), new Faker(), current, field -> {})
                    .open()),
        bot -> {
          SWTBot dialog = bot.shell(SHELL_TITLE).activate().bot();
          assertTrue(
              dialog.checkBox(SHOW_ALL_LABEL).isChecked(),
              "opening on a hidden generator should turn 'Show all' on");
          dialog.button(buttonLabel("System.Button.OK")).click();
        });

    FakeField field = picked.get();
    assertNotNull(field, "the hidden configured generator must still be pre-selected");
    assertEquals("harryPotter", field.getType());
    assertEquals("character", field.getTopic());
  }

  @Test
  void addFieldInvokesTheCallbackAndClearsTheName() {
    List<FakeField> added = Collections.synchronizedList(new ArrayList<>());
    AtomicReference<FakeField> picked = new AtomicReference<>();

    withDialog(
        parent ->
            picked.set(
                new FakerBrowserDialog(
                        parent, new Variables(), new Faker(), new FakeField(), added::add)
                    .open()),
        bot -> {
          SWTBot dialog = bot.shell(SHELL_TITLE).activate().bot();
          dialog.textWithTooltip(SEARCH_TOOLTIP).setText("firstname");
          dialog.textWithTooltip(NAME_TOOLTIP).setText("one");
          dialog.tree().getTreeItem("Name").expand().getNode("First Name").select();
          dialog.button(ADD_FIELD_BUTTON).click();

          // Add keeps the dialog open, records the field and clears the name for the next one.
          assertEquals("", dialog.textWithTooltip(NAME_TOOLTIP).getText());
          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });

    assertNull(picked.get(), "Cancel after Add must still return null");
    assertEquals(1, added.size(), "Add field must invoke the callback exactly once");
    assertEquals("one", added.get(0).getName());
    assertEquals("firstName", added.get(0).getTopic());
  }
}
