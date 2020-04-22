package org.apache.hop.env.environment;

import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.api.exceptions.MetaStoreException;
import org.apache.hop.metastore.persist.MetaStoreFactory;
import org.apache.hop.metastore.stores.xml.XmlMetaStore;
import org.apache.hop.metastore.util.HopDefaults;

public class EnvironmentSingleton {

  private static EnvironmentSingleton environmentSingleton;

  private String location;

  private EnvironmentSingleton( String location) throws MetaStoreException {
    this.location = location;
  }

  public static void initialize(String location) throws MetaStoreException {
    environmentSingleton = new EnvironmentSingleton( location );
  }

  public static String getLocation() {
    return environmentSingleton.location;
  }

  public static MetaStoreFactory<Environment> getEnvironmentFactory() throws MetaStoreException {
    return new MetaStoreFactory<>( Environment.class, getEnvironmentMetaStore(), HopDefaults.NAMESPACE );
  }

  public static IMetaStore getEnvironmentMetaStore() throws MetaStoreException {
    return new XmlMetaStore( getLocation() );
  }
}
