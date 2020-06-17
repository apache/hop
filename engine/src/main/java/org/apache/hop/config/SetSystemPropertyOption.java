package org.apache.hop.config;

import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import picocli.CommandLine;

public class SetSystemPropertyOption implements IConfigOptions {
  public static final String[] OPTION_NAMES = { "-s", "--system-properties" };

  @CommandLine.Option( names = { "-s", "--system-properties" }, description = "A comma separated list of KEY=VALUE pairs", split = "," )
  private String[] systemProperties = null;

  @Override public boolean handleOption( ILogChannel log, IHopMetadataProvider metadataProvider, IVariables variables ) throws HopException {
    // Is this an option we want to handle?
    //
    if (systemProperties!=null) {
      boolean changed = false;
      for ( String systemProperty : systemProperties ) {
        int equalsIndex = systemProperty.indexOf( '=' );
        if ( equalsIndex > 0 ) {
          String key = systemProperty.substring( 0, equalsIndex );
          String value = systemProperty.substring( equalsIndex + 1 );
          if ( Utils.isEmpty( value ) ) {
            // Remove this property
            //
            HopConfig.getSystemProperties().remove( key );
            HopConfig.saveToFile();
            log.logBasic( "Removed system property '" + key + "'" );
          } else {
            log.logBasic( "Set system property '" + key + "'" );
            HopConfig.saveSystemProperty( key, value );
          }
          changed=true;
        }
      }
      return changed;
    }
    return false;
  }
}

