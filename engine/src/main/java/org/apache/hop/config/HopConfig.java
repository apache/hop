/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.config;

import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.config.plugin.ConfigPluginType;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metastore.MetaStoreConst;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.api.exceptions.MetaStoreException;
import org.apache.hop.metastore.stores.delegate.DelegatingMetaStore;
import picocli.CommandLine;
import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;

import java.util.List;
import java.util.Map;

public class HopConfig implements Runnable {

  @Option( names = { "-h", "--help" }, usageHelp = true, description = "Displays this help message and quits." )
  private boolean helpRequested;

  private CommandLine cmd;
  private IVariables variables;
  private ILogChannel log;
  private DelegatingMetaStore metaStore;

  public void run() {

    try {
      log = new LogChannel( "hop-config" );
      variables = Variables.getADefaultVariableSpace();
      buildMetaStore();

      boolean actionTaken = false;

      Map<String, Object> mixins = cmd.getMixins();
      for (String key : mixins.keySet()) {
        Object mixin = mixins.get( key );
        if (mixin instanceof IConfigOptions) {
          IConfigOptions configOptions = (IConfigOptions) mixin;

          actionTaken = configOptions.handleOption( log, metaStore, variables ) || actionTaken;
        }
      }

      if (!actionTaken) {
        CommandLine.printHelpIfRequested( cmd.getParseResult() );
      }

    } catch ( Exception e ) {
      throw new ExecutionException( cmd, "There was an error handling options", e );
    }
  }

  private void buildMetaStore() throws MetaStoreException {
    metaStore = new DelegatingMetaStore();
    IMetaStore localMetaStore = MetaStoreConst.openLocalHopMetaStore( variables );
    metaStore.addMetaStore( localMetaStore );
    metaStore.setActiveMetaStoreName( localMetaStore.getName() );
  }

  /**
   * Gets cmd
   *
   * @return value of cmd
   */
  public CommandLine getCmd() {
    return cmd;
  }

  /**
   * @param cmd The cmd to set
   */
  public void setCmd( CommandLine cmd ) {
    this.cmd = cmd;
  }

  public static void main( String[] args ) {

    try {
      HopEnvironment.init();

      HopConfig hopConfig = new HopConfig();

      CommandLine cmd = new CommandLine( hopConfig );
      List<IPlugin> configPlugins = PluginRegistry.getInstance().getPlugins( ConfigPluginType.class );
      for ( IPlugin configPlugin : configPlugins ) {
        IConfigOptions configOptions = PluginRegistry.getInstance().loadClass( configPlugin, IConfigOptions.class );
        cmd.addMixin( configPlugin.getName(), configOptions );
      }

      hopConfig.setCmd( cmd );
      CommandLine.ParseResult parseResult = cmd.parseArgs( args );
      if ( CommandLine.printHelpIfRequested( parseResult ) ) {
        System.exit( 1 );
      } else {
        hopConfig.run();
        System.exit( 0 );
      }
    } catch ( ParameterException e ) {
      System.err.println( e.getMessage() );
      e.getCommandLine().usage( System.err );
      System.exit( 9 );
    } catch ( ExecutionException e ) {
      System.err.println( "Error found during execution!" );
      System.err.println( Const.getStackTracker( e ) );

      System.exit( 1 );
    } catch ( Exception e ) {
      System.err.println( "General error found, something went horribly wrong!" );
      System.err.println( Const.getStackTracker( e ) );

      System.exit( 2 );
    }

  }
}
