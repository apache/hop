package org.apache.hop.env.config.plugins;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.env.config.EnvironmentConfig;
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

  @CommandLine.Option( names = { "-ef", "--environment-config-file" }, description = "Specify the name of the local environment config json file (default: "+ EnvironmentConfig.HOP_CONFIG_ENVIRONMENT_FILE+")" )
  @GuiWidgetElement(
    id = "environment-config-file",
    type = GuiElementType.TEXT,
    label = "Specify the name of the local environment json file",
    parentId = "ConfigureEnvironments"
  )
  private String environmentConfigFile;

  public ConfigureEnvironmentsOptionPlugin() {
  }

  public static ConfigureEnvironmentsOptionPlugin getInstance() {
    ConfigureEnvironmentsOptionPlugin optionPlugin = new ConfigureEnvironmentsOptionPlugin();
    optionPlugin.enableEnvironments = EnvironmentConfigSingleton.getConfig().isEnabled();
    optionPlugin.openLastUsedEnvironment = EnvironmentConfigSingleton.getConfig().isOpeningLastEnvironmentAtStartup();
    return optionPlugin;
  }

  @Override public boolean handleOption( ILogChannel log, IHopMetadataProvider metadataProvider, IVariables variables ) throws HopException {
    EnvironmentConfig config = EnvironmentConfigSingleton.getConfig();

    boolean changed=false;
    if (enableEnvironments) {
      config.setEnabled( enableEnvironments );
      HopConfig.saveToFile();
      log.logBasic( "The environment system is now "+(enableEnvironments?"enabled":"disabled") );
      changed=true;
    }

    if (openLastUsedEnvironment) {
      config.setOpeningLastEnvironmentAtStartup( openLastUsedEnvironment );
      HopConfig.saveToFile();
      log.logBasic( "The environment system is "+(!openLastUsedEnvironment?"not ":"")+" opening the last used environment in the Hop GUI." );
      changed=true;
    }

    if ( StringUtils.isNotEmpty(environmentConfigFile)) {
      config.setEnvironmentConfigFilename( environmentConfigFile );
      HopConfig.saveToFile();
      log.logBasic( "The environment config filename is now set to "+environmentConfigFile+". It will be picked up relative to every environment home folder." );
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

  /**
   * Gets environmentConfigFile
   *
   * @return value of environmentConfigFile
   */
  public String getEnvironmentConfigFile() {
    return environmentConfigFile;
  }

  /**
   * @param environmentConfigFile The environmentConfigFile to set
   */
  public void setEnvironmentConfigFile( String environmentConfigFile ) {
    this.environmentConfigFile = environmentConfigFile;
  }
}

