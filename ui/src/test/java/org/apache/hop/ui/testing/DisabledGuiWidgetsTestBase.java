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

package org.apache.hop.ui.testing;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.hop.core.gui.plugin.GuiElements;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.menu.GuiMenuItem;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarItem;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.GuiMenuWidgets;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeWidgetsListener;
import org.apache.hop.ui.core.gui.IToolbarContainer;
import org.apache.hop.ui.hopgui.HopGuiEnvironment;
import org.apache.hop.ui.hopgui.ToolbarFacade;
import org.eclipse.swt.SWT;
import org.eclipse.swt.SWTError;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.Shell;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

/**
 * Anything in Hop that carries an id can be switched off with an {@code <exclusion>} for that id in
 * {@code disabledGuiElements.xml}: a {@code @GuiWidgetElement}, a {@code @GuiToolbarElement}, a
 * {@code @GuiMenuElement}, and more. The filtering happens at registration time, so the net effect
 * is always the same - the element is simply absent from the registry and never gets built. Code
 * that later looks it up by id gets a {@code null} back, and any unguarded dereference turns a
 * supported configuration into a crash.
 *
 * <p>This harness makes that class of bugs testable. It discovers every element registered on the
 * classpath of the module under test and, for each one, rebuilds the thing that holds it -
 * composite, toolbar or menu - with exactly that element disabled, driving the callbacks the
 * framework invokes. One dynamic test per element, so a failure names the element that cannot be
 * disabled.
 *
 * <p>To cover a module, subclass this and narrow {@link #packagesUnderTest()} to the module's own
 * packages - hop-core, hop-engine and hop-ui ride along on every plugin's classpath and are already
 * covered by the hop-ui-rcp test.
 *
 * <p>What is <em>not</em> covered: an element that is only ever dereferenced from an event handler
 * (a tree selection listener, say) is not reached by building the widget, so those call sites have
 * to use the null-safe accessors ({@code GuiMenuWidgets.enableMenuItem(id, enabled)} rather than
 * {@code findMenuItem(id).setEnabled(enabled)}). Keyboard shortcuts, context actions and config
 * plugins are disabled by simply not being registered and are never looked up by id, so there is
 * nothing to break.
 *
 * <p>A container that cannot be built at all in a test JVM (no public no-arg constructor, class not
 * on the classpath, a widget that needs a live back end, ...) is <em>skipped, with the reason</em>
 * rather than failed: each is first built with nothing disabled, and only if that baseline works do
 * the disable cases have to hold. That keeps a failure here meaning one thing - disabling this
 * element broke something.
 */
public abstract class DisabledGuiWidgetsTestBase extends SwtBotTestBase {

  /**
   * Registers the {@code @GuiPlugin} classes so the elements we enumerate below are in the
   * registry. Runs after {@link SwtBotTestBase}'s own setup, which skips the whole class when there
   * is no display.
   */
  @BeforeAll
  static void registerGuiPluginElements() throws Exception {
    HopGuiEnvironment.init();
  }

  /** Per container: why it cannot be built at all here, or "" when it builds fine. */
  private final Map<String, String> baselineFailures = new HashMap<>();

  /**
   * The packages whose elements are tested, subpackages included. Empty - the default - tests every
   * element registered on the classpath. A plugin narrows this to its own packages, because
   * hop-core, hop-engine and hop-ui are on its classpath too and their elements are already covered
   * in hop-ui-rcp.
   */
  protected List<String> packagesUnderTest() {
    return List.of();
  }

  /** One disable-able element: the class that owns it, the container it sits in, and its id. */
  private record Element(String owner, String container, String id) {}

  @Test
  void theModuleRegistersSomethingThatCanBeDisabled() {
    assertFalse(
        compositeElements().isEmpty() && toolbarElements().isEmpty() && menuElements().isEmpty(),
        "Nothing that disabledGuiElements.xml can switch off is registered for "
            + (packagesUnderTest().isEmpty() ? "any package" : packagesUnderTest())
            + ", so the tests below would pass without testing anything");
  }

  @TestFactory
  Stream<DynamicTest> disablingAWidgetMustNotBreakItsComposite() {
    return casesFor(compositeElements(), "composite", this::buildComposite);
  }

  @TestFactory
  Stream<DynamicTest> disablingAToolbarItemMustNotBreakItsToolbar() {
    return casesFor(toolbarElements(), "toolbar", this::buildToolbar);
  }

  @TestFactory
  Stream<DynamicTest> disablingAMenuItemMustNotBreakItsMenu() {
    return casesFor(menuElements(), "menu", this::buildMenu);
  }

  /** Builds one dynamic test per element: disable it, rebuild what holds it, nothing may break. */
  private Stream<DynamicTest> casesFor(List<Element> elements, String kind, Builder builder) {
    return elements.stream()
        .map(
            element ->
                dynamicTest(
                    simpleName(element.owner()) + " " + kind + " without " + element.id(),
                    () -> disablingIsHarmless(element, builder)));
  }

  private void disablingIsHarmless(Element element, Builder builder) {
    String baselineFailure =
        baselineFailures.computeIfAbsent(
            element.owner() + "#" + element.container(), key -> baselineFailure(element, builder));

    Assumptions.assumeTrue(
        baselineFailure.isEmpty(),
        () ->
            element.container()
                + " of "
                + element.owner()
                + " cannot be built in a test JVM even with all its elements present, so disabling"
                + " one of them proves nothing: "
                + baselineFailure);

    try {
      builder.build(element.owner(), element.container(), element.id());
    } catch (Exception | SWTError e) {
      // Surefire reports a dynamic test as "[n]", so the failure itself has to say which element it
      // was: that name is the whole point of the test.
      //
      throw new AssertionError(
          "Disabling '"
              + element.id()
              + "' of "
              + element.owner()
              + " breaks it. Add an exclusion for that id to disabledGuiElements.xml and the GUI"
              + " stops working.",
          e);
    }
  }

  /**
   * Why this container cannot be built with all its elements present, or "" when it builds fine.
   */
  private String baselineFailure(Element element, Builder builder) {
    try {
      builder.build(element.owner(), element.container(), null);
      return "";
    } catch (Exception | SWTError e) {
      return e.toString();
    }
  }

  // ----------------------------------------------------------------------------------------------
  // Discovery: what an exclusion in disabledGuiElements.xml can remove
  // ----------------------------------------------------------------------------------------------

  /** {@code @GuiWidgetElement}: the class declaring them -> the parent id -> the widget tree. */
  private List<Element> compositeElements() {
    List<Element> elements = new ArrayList<>();
    GuiRegistry.getInstance()
        .getDataElementsMap()
        .forEach(
            (className, perParent) -> {
              if (isUnderTest(className)) {
                perParent.forEach(
                    (parentId, tree) ->
                        collectWidgetIds(tree)
                            .forEach(id -> elements.add(new Element(className, parentId, id))));
              }
            });
    return elements;
  }

  /** {@code @GuiToolbarElement}: the toolbar root -> the items on it. */
  private List<Element> toolbarElements() {
    List<Element> elements = new ArrayList<>();
    GuiRegistry.getInstance()
        .getGuiToolbarMap()
        .forEach(
            (root, items) ->
                items
                    .values()
                    .forEach(
                        item -> {
                          if (!item.isIgnored() && isUnderTest(item.getListenerClass())) {
                            elements.add(new Element(item.getListenerClass(), root, item.getId()));
                          }
                        }));
    return elements;
  }

  /** {@code @GuiMenuElement}: the menu root -> the items in it. */
  private List<Element> menuElements() {
    List<Element> elements = new ArrayList<>();
    GuiRegistry.getInstance()
        .getGuiMenuMap()
        .forEach(
            (root, items) ->
                items
                    .values()
                    .forEach(
                        item -> {
                          if (isUnderTest(item.getListenerClassName())) {
                            elements.add(
                                new Element(item.getListenerClassName(), root, item.getId()));
                          }
                        }));
    return elements;
  }

  // ----------------------------------------------------------------------------------------------
  // Builders: recreate what the application builds, with one element taken out of the registry
  // ----------------------------------------------------------------------------------------------

  /** Builds a container with one element disabled, or with everything present when id is null. */
  @FunctionalInterface
  private interface Builder {
    void build(String owner, String container, String disabledId);
  }

  /**
   * Recreates what the application does with a composite: build the widgets, push the values of the
   * source object into them, and let the source object - which is its own listener, the way {@code
   * ConfigPluginOptionsTab} and the database dialogs wire it up - react to them.
   */
  private void buildComposite(String className, String parentId, String disabledWidgetId) {
    ensureDisplay();

    GuiElements tree = GuiRegistry.getInstance().findGuiElements(className, parentId);

    // The element that holds the widget, and where in it the widget sits, so it can go back exactly
    // where it was. Null when we build the composite as-is.
    //
    GuiElements owner = disabledWidgetId == null ? null : findOwner(tree, disabledWidgetId);
    GuiElements disabled = owner == null ? null : owner.findChild(disabledWidgetId);
    int position = owner == null ? -1 : owner.getChildren().indexOf(disabled);

    Shell shell = new Shell(display);
    shell.setLayout(new FormLayout());
    try {
      if (owner != null) {
        owner.getChildren().remove(position);
      }

      Object source = newSourceObject(className);
      GuiCompositeWidgets widgets = new GuiCompositeWidgets(new Variables());
      widgets.createCompositeWidgets(source, null, shell, parentId, null);
      widgets.setWidgetsContents(source, shell, parentId);

      if (source instanceof IGuiPluginCompositeWidgetsListener listener) {
        widgets.setWidgetsListener(listener);
        listener.widgetsCreated(widgets);
        listener.widgetsPopulated(widgets);
        // Every widget that is still there can be modified by the user, and each of those fires the
        // callback that tends to reach for the widget that is no longer there.
        //
        for (Map.Entry<String, Control> widget :
            new ArrayList<>(widgets.getWidgetsMap().entrySet())) {
          listener.widgetModified(widgets, widget.getValue(), widget.getKey());
        }
        listener.persistContents(widgets);
      }
    } finally {
      // Put the widget back: the registry is a singleton shared with every other case.
      //
      if (owner != null) {
        owner.getChildren().add(position, disabled);
      }
      disposeQuietly(shell);
    }
  }

  /** Builds the toolbar the way a perspective or dialog does, minus the disabled item. */
  private void buildToolbar(String owner, String root, String disabledItemId) {
    ensureDisplay();

    Map<String, GuiToolbarItem> items = GuiRegistry.getInstance().getGuiToolbarMap().get(root);
    GuiToolbarItem disabled = disabledItemId == null ? null : items.get(disabledItemId);

    Shell shell = new Shell(display);
    shell.setLayout(new FormLayout());
    try {
      if (disabled != null) {
        items.remove(disabledItemId);
      }

      IToolbarContainer container =
          ToolbarFacade.createToolbarContainer(shell, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL);
      new GuiToolbarWidgets().createToolbarWidgets(container, root);
    } finally {
      if (disabled != null) {
        items.put(disabledItemId, disabled);
      }
      disposeQuietly(shell);
    }
  }

  /** Builds the menu the way HopGui and the perspectives do, minus the disabled item. */
  private void buildMenu(String owner, String root, String disabledItemId) {
    ensureDisplay();

    Map<String, GuiMenuItem> items = GuiRegistry.getInstance().getGuiMenuMap().get(root);
    GuiMenuItem disabled = disabledItemId == null ? null : items.get(disabledItemId);

    Shell shell = new Shell(display);
    try {
      if (disabled != null) {
        items.remove(disabledItemId);
      }

      new GuiMenuWidgets().createMenuWidgets(root, shell, new Menu(shell, SWT.POP_UP));
    } finally {
      if (disabled != null) {
        items.put(disabledItemId, disabled);
      }
      disposeQuietly(shell);
    }
  }

  private void disposeQuietly(Shell shell) {
    shell.dispose();
    while (display.readAndDispatch()) {
      // flush the disposal before the next case builds its own shell
    }
  }

  private Object newSourceObject(String className) {
    try {
      Class<?> sourceClass = Class.forName(className, true, getClass().getClassLoader());

      // The config plugins hand out an instance filled in from the configuration, and that is the
      // one the config dialog builds its widgets around. A bare constructor would leave their
      // Boolean fields null, and the composite blows up on unboxing before we get to test anything.
      //
      Method getInstance = singletonAccessor(sourceClass);
      if (getInstance != null) {
        return getInstance.invoke(null);
      }
      return sourceClass.getConstructor().newInstance();
    } catch (ReflectiveOperationException e) {
      // No usable constructor, or the class is not on this module's classpath. The baseline build
      // hits this first, so the composite ends up skipped rather than failed.
      //
      throw new CompositeBuildException(e);
    }
  }

  /**
   * The class's own {@code static getInstance()}, when it has one that hands out an instance of
   * itself - which is how the config dialog gets the object it builds the widgets around.
   */
  private Method singletonAccessor(Class<?> sourceClass) {
    try {
      Method getInstance = sourceClass.getMethod("getInstance");
      boolean handsOutItself =
          Modifier.isStatic(getInstance.getModifiers())
              && sourceClass.isAssignableFrom(getInstance.getReturnType());
      return handsOutItself ? getInstance : null;
    } catch (NoSuchMethodException e) {
      return null;
    }
  }

  /** The element that holds the widget, which is where it has to be removed from (and restored). */
  private GuiElements findOwner(GuiElements element, String widgetId) {
    for (GuiElements child : element.getChildren()) {
      if (widgetId.equals(child.getId())) {
        return element;
      }
      GuiElements owner = findOwner(child, widgetId);
      if (owner != null) {
        return owner;
      }
    }
    return null;
  }

  /**
   * Every widget in the tree that an exclusion could remove, groups and nested widgets included.
   */
  private List<String> collectWidgetIds(GuiElements element) {
    List<String> ids = new ArrayList<>();
    for (GuiElements child : element.getChildren()) {
      if (child.getId() != null && !child.isIgnored()) {
        ids.add(child.getId());
      }
      ids.addAll(collectWidgetIds(child));
    }
    return ids;
  }

  private String simpleName(String className) {
    return className == null ? "?" : className.substring(className.lastIndexOf('.') + 1);
  }

  private boolean isUnderTest(String className) {
    if (className == null) {
      return false;
    }
    List<String> packages = packagesUnderTest();
    return packages.isEmpty()
        || packages.stream().anyMatch(name -> className.startsWith(name + "."));
  }

  /**
   * A container is built through an unchecked call, so the reflective failure of a source object
   * that has no usable constructor has to travel as one.
   */
  private static class CompositeBuildException extends RuntimeException {
    CompositeBuildException(Throwable cause) {
      super(cause);
    }
  }
}
