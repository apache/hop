package org.apache.hop.env.environment;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.env.util.Defaults;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.api.exceptions.MetaStoreException;
import org.apache.hop.metastore.persist.MetaStoreFactory;
import org.apache.hop.metastore.stores.xml.XmlMetaStore;

public class EnvironmentSingleton {

  private static EnvironmentSingleton environmentSingleton;

  private String location;

  private EnvironmentSingleton( String location) throws MetaStoreException {
    this.location = location;
  }

  public static boolean isInitialized() {
    return environmentSingleton!=null;
  }

  public static void initialize(String location) throws MetaStoreException {
    environmentSingleton = new EnvironmentSingleton( location );
  }

  public static String getLocation() {
    return environmentSingleton.location;
  }

  public static MetaStoreFactory<Environment> getEnvironmentFactory() throws MetaStoreException {
    return new MetaStoreFactory<>( Environment.class, getEnvironmentMetaStore() );
  }

  public static IMetaStore getEnvironmentMetaStore() throws MetaStoreException {
    return new XmlMetaStore( getLocation() );
  }

  /**
   * Starts from one variable ENVIRONMENT_METASTORE_FOLDER to bootstrap the environments
   * @throws HopException
   */
  public static void initializeEnvironments() throws HopException {
    IVariables variables = Variables.getADefaultVariableSpace();

    // Where is the metastore for the environment
    //
    String environmentMetastoreLocation = variables.getVariable( Defaults.VARIABLE_ENVIRONMENT_METASTORE_FOLDER );
    if ( StringUtils.isEmpty( environmentMetastoreLocation ) ) {
      environmentMetastoreLocation = Const.HOP_CONFIG_DIRECTORY + Const.FILE_SEPARATOR + "environments";
    }

    // Build the metastore for it.
    //
    try {
      EnvironmentSingleton.initialize( environmentMetastoreLocation );
    } catch ( Exception e ) {
      throw new HopException("Error initializing the Environment system", e );
    }
  }
}
