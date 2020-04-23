package org.apache.hop.env.gui;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.gui.plugin.GuiKeyboardShortcut;
import org.apache.hop.core.gui.plugin.GuiMetaStoreElement;
import org.apache.hop.core.gui.plugin.GuiOsxKeyboardShortcut;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.menu.GuiMenuElement;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElementType;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.env.config.EnvironmentConfigDialog;
import org.apache.hop.env.config.EnvironmentConfigSingleton;
import org.apache.hop.env.environment.Environment;
import org.apache.hop.env.util.EnvironmentUtil;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.ui.cluster.IGuiMetaStorePlugin;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;

import java.util.Collections;
import java.util.List;

@GuiPlugin
@GuiMetaStoreElement(
  name = "Environment",
  description = "A environment to manage your projects",
  iconImage = "ui/images/HTP.svg"
)
public class EnvironmentGuiPlugin implements IGuiMetaStorePlugin<Environment> {

  public static final String ID_TOOLBAR_ENVIRONMENT_COMBO = "toolbar-40000-environment";
  public static final String ID_MENU_TOOLS_CONFIGURE_ENVIRONMENTS = "40100-menu-tools-environment";

  private static EnvironmentGuiPlugin instance;

  /**
   * Gets instance
   *
   * @return value of instance
   */
  public static EnvironmentGuiPlugin getInstance() {
    if ( instance == null ) {
      instance = new EnvironmentGuiPlugin();
    }
    return instance;
  }

  @Override public Class<Environment> getMetaStoreElementClass() {
    return Environment.class;
  }

  /**
   * Not supposed to be instantiated but needs to be public to get the MetaStore element class.
   * The methods below are called on the instance given by getInstance().
   */
  public EnvironmentGuiPlugin() {
  }

  @GuiMenuElement(
    root = HopGui.ID_MAIN_MENU,
    id = ID_MENU_TOOLS_CONFIGURE_ENVIRONMENTS,
    label = "Configure environments",
    parentId = HopGui.ID_MAIN_MENU_TOOLS_PARENT_ID
  )
  @GuiKeyboardShortcut( control = true, key = 'e' )
  @GuiOsxKeyboardShortcut( command = true, key = 'e' )
  public void menuToolsEnvironmentConfig() {
    HopGui hopGui = HopGui.getInstance();
    EnvironmentConfigDialog dialog = new EnvironmentConfigDialog( hopGui.getShell(), EnvironmentConfigSingleton.getConfig() );
    if (dialog.open()) {
      try {
        EnvironmentConfigSingleton.saveConfig();
      } catch(Exception e) {
        new ErrorDialog( hopGui.getShell(), "Error", "Error saving environment system configuration", e );
      }
    }
  }

  @GuiToolbarElement(
    root = HopGui.ID_MAIN_TOOLBAR,
    id = ID_TOOLBAR_ENVIRONMENT_COMBO,
    type = GuiToolbarElementType.COMBO,
    label = "Environment :  ",
    comboValuesMethod = "getEnvironmentsList",
    toolTip = "The active environment",
    separator = true
  )
  public void selectEnvironment() {
    HopGui hopGui = HopGui.getInstance();
    Combo combo = getEnvironmentsCombo();
    if ( combo == null ) {
      return;
    }
    String environmentName = combo.getText();
    if ( StringUtils.isEmpty( environmentName ) ) {
      return;
    }
    try {
      Environment environment = EnvironmentUtil.getEnvironment( environmentName );
      if ( environment != null ) {
        EnvironmentUtil.enableEnvironment( environment, hopGui.getMetaStore() );
      } else {
        hopGui.getLog().logError( "Unable to find environment '" + environmentName + "'" );
      }
    } catch ( Exception e ) {
      new ErrorDialog( hopGui.getShell(), "Error", "Error changing environment to '" + environmentName, e );
    }
  }

  private Combo getEnvironmentsCombo() {
    Control control = HopGui.getInstance().getMainToolbarWidgets().getWidgetsMap().get( EnvironmentGuiPlugin.ID_TOOLBAR_ENVIRONMENT_COMBO );
    if ( ( control != null ) && ( control instanceof Combo ) ) {
      Combo combo = (Combo) control;
      return combo;
    }
    return null;
  }

  /**
   * Called by the Combo in the toolbar
   *
   * @param log
   * @param metaStore
   * @return
   * @throws Exception
   */
  public List<String> getEnvironmentsList( ILogChannel log, IMetaStore metaStore ) throws Exception {
    List<String> names = Environment.createFactory( metaStore ).getElementNames();
    Collections.sort( names );
    return names;
  }

  public static void refreshEnvironmentsList() {
    HopGui.getInstance().getMainToolbarWidgets().refreshComboItemList( ID_TOOLBAR_ENVIRONMENT_COMBO );
  }

  public static void selectEnvironmentInList( String name ) {
    Control control = HopGui.getInstance().getMainToolbarWidgets().getWidgetsMap().get( EnvironmentGuiPlugin.ID_TOOLBAR_ENVIRONMENT_COMBO );
    if ( ( control != null ) && ( control instanceof Combo ) ) {
      Combo combo = (Combo) control;
      combo.setText( Const.NVL(name, "") );
      int index = Const.indexOfString( name, combo.getItems() );
      if ( index >= 0 ) {
        combo.select( index );
      }
    }
  }
}
