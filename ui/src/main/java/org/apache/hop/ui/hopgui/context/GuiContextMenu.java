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

package org.apache.hop.ui.hopgui.context;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.gui.plugin.IGuiActionLambda;
import org.apache.hop.core.gui.plugin.action.GuiAction;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.util.SwtErrorHandler;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.Shell;

/**
 * Shows the actions of an {@link IGuiContextHandler} as a native SWT pop-up menu instead of the
 * {@link org.apache.hop.ui.core.dialog.ContextDialog}: the "use menus instead of the context
 * dialog" canvas option. Actions with a category go into a submenu per category, ordered like the
 * context dialog orders its category headers; actions without a category are listed at the top
 * level below them.
 */
public class GuiContextMenu {

  private GuiContextMenu() {}

  /**
   * Show the actions of the context handler as a pop-up menu at the given display location.
   *
   * @param parent the shell owning the menu, also the parent of error dialogs
   * @param contextHandler the handler supplying the actions
   * @param displayX the x coordinate on the display
   * @param displayY the y coordinate on the display
   * @return true when a menu was shown, false when there were no actions to show
   */
  public static boolean show(
      Shell parent, IGuiContextHandler contextHandler, int displayX, int displayY) {
    List<GuiAction> actions =
        GuiContextUtil.getInstance().filterAllowedActions(contextHandler.getSupportedActions());
    if (actions.isEmpty()) {
      return false;
    }

    Menu menu = new Menu(parent, SWT.POP_UP);

    // Group by category, keeping the first-seen order of the actions inside a category and
    // sorting the categories by their order like the context dialog does.
    //
    Map<String, List<GuiAction>> byCategory = new LinkedHashMap<>();
    Map<String, String> categoryOrder = new LinkedHashMap<>();
    List<GuiAction> uncategorized = new ArrayList<>();
    for (GuiAction action : actions) {
      String category = action.getCategory();
      if (StringUtils.isEmpty(category)) {
        uncategorized.add(action);
        continue;
      }
      byCategory.computeIfAbsent(category, k -> new ArrayList<>()).add(action);
      categoryOrder.putIfAbsent(category, Const.NVL(action.getCategoryOrder(), "0"));
    }
    List<String> categories = new ArrayList<>(byCategory.keySet());
    categories.sort(Comparator.comparing(categoryOrder::get));

    for (String category : categories) {
      MenuItem categoryItem = new MenuItem(menu, SWT.CASCADE);
      categoryItem.setText(category);
      Menu subMenu = new Menu(menu);
      categoryItem.setMenu(subMenu);
      for (GuiAction action : byCategory.get(category)) {
        addActionMenuItem(subMenu, action, parent);
      }
    }

    if (!uncategorized.isEmpty()) {
      if (!categories.isEmpty()) {
        new MenuItem(menu, SWT.SEPARATOR);
      }
      for (GuiAction action : uncategorized) {
        addActionMenuItem(menu, action, parent);
      }
    }

    menu.setLocation(displayX, displayY);

    // Dispose the menu once it is hidden to avoid leaking widgets.
    //
    menu.addListener(SWT.Hide, event -> menu.getDisplay().asyncExec(menu::dispose));

    menu.setVisible(true);
    return true;
  }

  /** Adds a single push menu item for a {@link GuiAction} (icon, label and its action lambda). */
  public static void addActionMenuItem(Menu menu, GuiAction action, Shell shell) {
    MenuItem menuItem = new MenuItem(menu, SWT.PUSH);
    menuItem.setText(Const.NVL(action.getName(), action.getId()));
    if (StringUtils.isNotEmpty(action.getTooltip())) {
      menuItem.setToolTipText(action.getTooltip());
    }

    // Load the action image (SVG) when there is one.
    //
    if (StringUtils.isNotEmpty(action.getImage())) {
      try {
        ClassLoader classLoader = action.getClassLoader();
        if (classLoader == null) {
          classLoader = GuiContextMenu.class.getClassLoader();
        }
        Image image =
            GuiResource.getInstance()
                .getImage(
                    action.getImage(),
                    classLoader,
                    ConstUi.SMALL_ICON_SIZE,
                    ConstUi.SMALL_ICON_SIZE);
        menuItem.setImage(image);
      } catch (Exception e) {
        // Ignore image loading errors, the menu item text is enough.
      }
    }

    menuItem.addListener(
        SWT.Selection,
        event -> {
          boolean shiftClicked = (event.stateMask & SWT.SHIFT) != 0;
          boolean ctrlClicked = (event.stateMask & SWT.CONTROL) != 0;
          // Defer execution until the menu is fully closed.
          //
          shell
              .getDisplay()
              .asyncExec(
                  () -> {
                    try {
                      IGuiActionLambda<?> actionLambda = action.getActionLambda();
                      actionLambda.executeAction(shiftClicked, ctrlClicked);
                    } catch (Exception e) {
                      if (!SwtErrorHandler.handleException(e)) {
                        new ErrorDialog(shell, "Error", "An error occurred executing action", e);
                      }
                    }
                  });
        });
  }
}
