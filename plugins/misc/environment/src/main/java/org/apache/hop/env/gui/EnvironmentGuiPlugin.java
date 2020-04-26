package org.apache.hop.env.gui;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiMetaStoreElement;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.key.GuiKeyboardShortcut;
import org.apache.hop.core.gui.plugin.key.GuiOsxKeyboardShortcut;
import org.apache.hop.core.gui.plugin.menu.GuiMenuElement;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElementType;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.env.config.EnvironmentConfigDialog;
import org.apache.hop.env.config.EnvironmentConfigSingleton;
import org.apache.hop.env.environment.Environment;
import org.apache.hop.env.environment.EnvironmentSingleton;
import org.apache.hop.env.util.EnvironmentUtil;
import org.apache.hop.history.AuditEvent;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.ui.cluster.IGuiMetaStorePlugin;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.metastore.MetaStoreManager;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;

import java.util.Collections;
import java.util.Date;
import java.util.List;

@GuiPlugin
@GuiMetaStoreElement(
  name = "Environment",
  description = "An environment to manage your project",
  iconImage = "environment.svg"
)
public class EnvironmentGuiPlugin implements IGuiMetaStorePlugin<Environment> {

  public static final String ID_TOOLBAR_ENVIRONMENT_LABEL = "toolbar-40000-environment";
  public static final String ID_TOOLBAR_ENVIRONMENT_COMBO = "toolbar-40010-environment";

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
    if ( dialog.open() ) {
      try {
        EnvironmentConfigSingleton.saveConfig();
      } catch ( Exception e ) {
        new ErrorDialog( hopGui.getShell(), "Error", "Error saving environment system configuration", e );
      }
    }
  }

  @GuiToolbarElement(
    root = HopGui.ID_MAIN_TOOLBAR,
    id = ID_TOOLBAR_ENVIRONMENT_LABEL,
    type = GuiToolbarElementType.LABEL,
    label = "  Environment : ",
    toolTip = "Click here to edit the active environment",
    separator = true
  )
  public void editEnvironment() {
    HopGui hopGui = HopGui.getInstance();
    Combo combo = getEnvironmentsCombo();
    if ( combo == null ) {
      return;
    }
    String environmentName = combo.getText();
    try {
      MetaStoreManager<Environment> manager = new MetaStoreManager<>( hopGui.getVariables(), EnvironmentSingleton.getEnvironmentMetaStore(), Environment.class );
      if ( manager.editMetadata( environmentName ) ) {
        refreshEnvironmentsList();
        selectEnvironmentInList( environmentName );
      }
    } catch ( Exception e ) {
      new ErrorDialog( hopGui.getShell(), "Error", "Error editing environment '" + environmentName, e );
    }
  }

  @GuiToolbarElement(
    root = HopGui.ID_MAIN_TOOLBAR,
    id = ID_TOOLBAR_ENVIRONMENT_COMBO,
    type = GuiToolbarElementType.COMBO,
    comboValuesMethod = "getEnvironmentsList",
    extraWidth = 200,
    toolTip = "Select the active environment"
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
        enableHopGuiEnvironment( environment );
      } else {
        hopGui.getLog().logError( "Unable to find environment '" + environmentName + "'" );
      }
    } catch ( Exception e ) {
      new ErrorDialog( hopGui.getShell(), "Error", "Error changing environment to '" + environmentName, e );
    }
  }

  public static final void enableHopGuiEnvironment( Environment environment ) throws HopException {
    try {
      HopGui hopGui = HopGui.getInstance();

      // Before we switch the namespace in HopGui, save the state of the perspectives
      //
      hopGui.auditDelegate.writeLastOpenFiles();

      // Now we can close all files if they're all saved (or changes are ignored)
      //
      if ( !hopGui.fileDelegate.saveGuardAllFiles() ) {
        // Abort the environment change
        return;
      }

      // Close 'm all
      //
      hopGui.fileDelegate.closeAllFiles();

      // This is called only in HopGui so we want to start with a new set of variables
      // It avoids variables from one environment showing up in another
      //
      IVariables variables = Variables.getADefaultVariableSpace();

      // Set the variables and so on...
      //
      EnvironmentUtil.enableEnvironment( hopGui.getLog(), environment, hopGui.getMetaStore(), variables );

      // We store the environment in the HopGui namespace
      //
      hopGui.setNamespace( environment.getName() );

      // We need to change the currently set variables in the newly loaded files
      //
      hopGui.setVariables( variables );

      // Re-open last open files for the namespace
      //
      hopGui.auditDelegate.openLastFiles();

      // Clear last used, fill it with something useful.
      //
      IVariables hopGuiVariables = Variables.getADefaultVariableSpace();
      hopGui.setVariables( hopGuiVariables );
      for ( String variable : variables.listVariables() ) {
        String value = variables.getVariable( variable );
        if ( !variable.startsWith( Const.INTERNAL_VARIABLE_PREFIX ) ) {
          hopGuiVariables.setVariable( variable, value );
        }
      }

      // Refresh the currently active file
      //
      hopGui.getActivePerspective().getActiveFileTypeHandler().updateGui();

      // Update the toolbar combo
      //
      EnvironmentGuiPlugin.selectEnvironmentInList( environment.getName() );

      // Also add this as an event so we know what the environment usage history is
      //
      AuditEvent envUsedEvent = new AuditEvent(
        EnvironmentUtil.STRING_ENVIRONMENT_AUDIT_GROUP,
        EnvironmentUtil.STRING_ENVIRONMENT_AUDIT_TYPE, environment
        .getName(),
        "open",
        new Date()
      );
      hopGui.getAuditManager().storeEvent( envUsedEvent );

    } catch ( Exception e ) {
      throw new HopException( "Error enabling environment '" + environment.getName() + "' in HopGui", e );
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
      combo.setText( Const.NVL( name, "" ) );
      int index = Const.indexOfString( name, combo.getItems() );
      if ( index >= 0 ) {
        combo.select( index );
      }
    }
  }
}
