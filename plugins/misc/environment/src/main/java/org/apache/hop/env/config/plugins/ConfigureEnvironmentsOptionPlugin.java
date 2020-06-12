package org.apache.hop.env.config.plugins;

import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.env.config.EnvironmentConfigSingleton;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import picocli.CommandLine;

@ConfigPlugin(
  id = "ConfigureEnvironmentsOptionPlugin",
  guiPluginId = "ConfigureEnvironments",
  description = "Allows you to configure the environments system"
)
@GuiPlugin(
  id = "ConfigureEnvironments", // Is the parent ID for the options dialog
  description = "Environments"
)
public class ConfigureEnvironmentsOptionPlugin implements IConfigOptions {
  public static final String[] OPTION_NAMES = { "-s", "--system-properties" };

  @CommandLine.Option( names = { "-ee", "--environments-enable" }, description = "Enable the environments system" )
  @GuiWidgetElement(
    id = "environments-enable",
    type = GuiElementType.CHECKBOX,
    label = "Enable environments system?",
    toolTip = "This options enables or disables the environments system at the start of a hop system",
    parentId = "ConfigureEnvironments"
  )
  private boolean enableEnvironments;

  @CommandLine.Option( names = { "-eo", "--environments-open-last-used" }, description = "Open the last used environment in the Hop GUI" )
  @GuiWidgetElement(
    id = "environments-open-last-used",
    type = GuiElementType.CHECKBOX,
    label = "Open the last used environment at startup of the Hop GUI?",
    parentId = "ConfigureEnvironments"
  )
  private boolean openLastUsedEnvironment;

  public ConfigureEnvironmentsOptionPlugin() {
  }

  public static ConfigureEnvironmentsOptionPlugin getInstance() {
    ConfigureEnvironmentsOptionPlugin optionPlugin = new ConfigureEnvironmentsOptionPlugin();
    optionPlugin.enableEnvironments = EnvironmentConfigSingleton.getConfig().isEnabled();
    optionPlugin.openLastUsedEnvironment = EnvironmentConfigSingleton.getConfig().isOpeningLastEnvironmentAtStartup();
    return optionPlugin;
  }

  @Override public boolean handleOption( ILogChannel log, IHopMetadataProvider metadataProvider, IVariables variables ) throws HopException {
    boolean changed=false;
    if (enableEnvironments) {
      EnvironmentConfigSingleton.getConfig().setEnabled( enableEnvironments );
      HopConfig.saveToFile();
      log.logBasic( "The environment system is now "+(enableEnvironments?"enabled":"disabled") );
      changed=true;
    }

    if (openLastUsedEnvironment) {
      EnvironmentConfigSingleton.getConfig().setOpeningLastEnvironmentAtStartup( openLastUsedEnvironment );
      HopConfig.saveToFile();
      log.logBasic( "The environment system is "+(!openLastUsedEnvironment?"not ":"")+" opening the last used environment in the Hop GUI." );
      changed=true;
    }

    return changed;
  }

  /**
   * Gets enableEnvironments
   *
   * @return value of enableEnvironments
   */
  public boolean isEnableEnvironments() {
    return enableEnvironments;
  }

  /**
   * @param enableEnvironments The enableEnvironments to set
   */
  public void setEnableEnvironments( boolean enableEnvironments ) {
    this.enableEnvironments = enableEnvironments;
    EnvironmentConfigSingleton.getConfig().setEnabled( enableEnvironments );
  }

  /**
   * Gets openLastUsedEnvironment
   *
   * @return value of openLastUsedEnvironment
   */
  public boolean isOpenLastUsedEnvironment() {
    return openLastUsedEnvironment;
  }

  /**
   * @param openLastUsedEnvironment The openLastUsedEnvironment to set
   */
  public void setOpenLastUsedEnvironment( boolean openLastUsedEnvironment ) {
    this.openLastUsedEnvironment = openLastUsedEnvironment;
    EnvironmentConfigSingleton.getConfig().setOpeningLastEnvironmentAtStartup( openLastUsedEnvironment );

  }
}

