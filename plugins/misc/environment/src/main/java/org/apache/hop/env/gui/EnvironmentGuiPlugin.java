package org.apache.hop.env.gui;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElementType;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.env.config.EnvironmentConfigSingleton;
import org.apache.hop.env.environment.Environment;
import org.apache.hop.env.util.EnvironmentUtil;
import org.apache.hop.history.AuditEvent;
import org.apache.hop.history.AuditManager;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.env.environment.EnvironmentDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;

import java.util.Date;
import java.util.List;

@GuiPlugin
public class EnvironmentGuiPlugin {

  public static final String ID_TOOLBAR_ENVIRONMENT_LABEL = "toolbar-40000-environment-label";
  public static final String ID_TOOLBAR_ENVIRONMENT_COMBO = "toolbar-40010-environment-list";
  public static final String ID_TOOLBAR_ENVIRONMENT_EDIT = "toolbar-40020-environment-edit";
  public static final String ID_TOOLBAR_ENVIRONMENT_ADD = "toolbar-40030-environment-add";
  public static final String ID_TOOLBAR_ENVIRONMENT_DELETE = "toolbar-40040-environment-delete";

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

  /**
   * Not supposed to be instantiated but needs to be public to get the MetaStore element class.
   * The methods below are called on the instance given by getInstance().
   */
  public EnvironmentGuiPlugin() {
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
      Environment environment = EnvironmentConfigSingleton.load( environmentName );
      EnvironmentDialog environmentDialog = new EnvironmentDialog( hopGui.getShell(), hopGui.getMetadataProvider(), environment, hopGui.getVariables() );
      if ( environmentDialog.open() != null ) {
        EnvironmentConfigSingleton.save( environment );
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

  @GuiToolbarElement(
    root = HopGui.ID_MAIN_TOOLBAR,
    id = ID_TOOLBAR_ENVIRONMENT_EDIT,
    toolTip = "Edit the selected environment",
    image = "environment-edit.svg"
  )
  public void editSelectedEnvironment() {
    editEnvironment();
  }

  @GuiToolbarElement(
    root = HopGui.ID_MAIN_TOOLBAR,
    id = ID_TOOLBAR_ENVIRONMENT_ADD,
    toolTip = "Add a new environment",
    image = "environment-add.svg"
  )
  public void addNewEnvironment() {
    HopGui hopGui = HopGui.getInstance();
    try {
      Environment environment = new Environment();
      EnvironmentDialog environmentDialog = new EnvironmentDialog( hopGui.getShell(), hopGui.getMetadataProvider(), environment, hopGui.getVariables() );
      String name = environmentDialog.open();
      if ( name != null ) {
        // TODO: check if new environment exists
        EnvironmentConfigSingleton.save( environment );
        refreshEnvironmentsList();
        selectEnvironmentInList( name );
        enableHopGuiEnvironment( environment );
      }
    } catch ( Exception e ) {
      new ErrorDialog( hopGui.getShell(), "Error", "Error adding environment", e );
    }
  }

  @GuiToolbarElement(
    root = HopGui.ID_MAIN_TOOLBAR,
    id = ID_TOOLBAR_ENVIRONMENT_DELETE,
    toolTip = "Deleted the selected environment (TODO)",
    image = "environment-delete.svg",
    separator = true
  )
  public void deleteSelectedEnvironment() {
    // TODO
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
      EnvironmentUtil.enableEnvironment( hopGui.getLog(), environment, hopGui.getMetadataProvider(), variables );

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
      AuditManager.getActive().storeEvent( envUsedEvent );

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
   * @param metadataProvider
   * @return
   * @throws Exception
   */
  public List<String> getEnvironmentsList( ILogChannel log, IHopMetadataProvider metadataProvider ) throws Exception {
    List<String> names = EnvironmentConfigSingleton.getEnvironmentNames();
    return names;
  }

  public static void refreshEnvironmentsList() {
    HopGui.getInstance().getMainToolbarWidgets().refreshComboItemList( ID_TOOLBAR_ENVIRONMENT_COMBO );
  }

  public static void selectEnvironmentInList( String name ) {
    HopGui.getInstance().getMainToolbarWidgets().selectComboItem( ID_TOOLBAR_ENVIRONMENT_COMBO, name );
  }
}
