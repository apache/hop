package org.apache.hop.env.xp;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.cli.HopRun;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.env.environment.Environment;
import org.apache.hop.env.environment.EnvironmentSingleton;
import org.apache.hop.env.util.EnvironmentUtil;

@ExtensionPoint( id = "HopRunStartExtensionPoint",
  extensionPointId = "HopRunStart",
  description = "Enables an environment at the start of the hop execution"
)
public class HopRunStartExtensionPoint implements IExtensionPoint<HopRun> {

  @Override public void callExtensionPoint( ILogChannel log, HopRun hopRun ) throws HopException {

    // If there is no environment specified on the HopRun command line, we stop here.
    //
    if ( Utils.isEmpty(hopRun.getEnvironment())) {
      log.logDebug( "No environment set using -e in hop-run execution" );
      return;
    }

    // initialize the environments system
    //
    EnvironmentSingleton.initializeEnvironments();

    String environmentName = hopRun.getEnvironment();

    try {
      Environment environment = EnvironmentUtil.getEnvironment( environmentName );
      log.logDebug( "Enabling environment '"+environmentName+"'" );

      if (environment==null) {
        throw new HopException( "Environment '"+environmentName+"' couldn't be found" );
      }
      // Now we just enable this environment
      //
      EnvironmentUtil.enableEnvironment( log, environment, hopRun.getMetaStore(), hopRun.getVariables() );
    } catch(Exception e) {
      throw new HopException( "Error enabling environment '"+environmentName+"'", e );
    }

  }
}
