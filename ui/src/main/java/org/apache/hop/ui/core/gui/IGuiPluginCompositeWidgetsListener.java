package org.apache.hop.ui.core.gui;

import org.eclipse.swt.widgets.Control;

public interface IGuiPluginCompositeWidgetsListener {
  /**
   * This method is called when all the widgets are created
   * @param compositeWidgets
   */
  void widgetsCreated(GuiCompositeWidgets compositeWidgets);

  /**
   * This method is called when all the widgets have received data, right before they're shown
   * @param compositeWidgets
   */
  void widgetsPopulated(GuiCompositeWidgets compositeWidgets);

  /**
   * This method is called when a widget was modified
   * @param compositeWidgets
   * @param changedWidget
   */
  void widgetModified( GuiCompositeWidgets compositeWidgets, Control changedWidget);
}
