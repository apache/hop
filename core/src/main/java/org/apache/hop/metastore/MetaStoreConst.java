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

package org.apache.hop.metastore;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.api.exceptions.MetaStoreException;
import org.apache.hop.metastore.stores.xml.XmlMetaStore;
import org.apache.hop.metastore.stores.xml.XmlUtil;

import java.io.File;

public class MetaStoreConst {

  public static final String DB_ATTR_ID_DESCRIPTION = "description";
  public static final String DB_ATTR_ID_PLUGIN_ID = "plugin_id";
  public static final String DB_ATTR_ID_ACCESS_TYPE = "access_type";
  public static final String DB_ATTR_ID_HOSTNAME = "host_name";
  public static final String DB_ATTR_ID_PORT = "port";
  public static final String DB_ATTR_ID_DATABASE_NAME = "database_name";
  public static final String DB_ATTR_ID_USERNAME = "username";
  public static final String DB_ATTR_ID_PASSWORD = "password";
  public static final String DB_ATTR_ID_SERVERNAME = "server_name";
  public static final String DB_ATTR_ID_DATA_TABLESPACE = "data_tablespace";
  public static final String DB_ATTR_ID_INDEX_TABLESPACE = "index_tablespace";
  public static boolean disableMetaStore; // Used for testing only

  // Extra information for 3rd party tools, not used by Hop
  //
  public static final String DB_ATTR_DRIVER_CLASS = "driver_class";
  public static final String DB_ATTR_JDBC_URL = "jdbc_url";

  public static final String DB_ATTR_ID_ATTRIBUTES = "attributes";

  public static final String getDefaultHopMetaStoreLocation() {
    return System.getProperty( "user.home" ) + File.separator + ".hop";
  }

  public static IMetaStore openLocalHopMetaStore() throws MetaStoreException {
    return MetaStoreConst.openLocalHopMetaStore( true );
  }

  public static IMetaStore openLocalHopMetaStore( boolean allowCreate ) throws MetaStoreException {
    if ( disableMetaStore ) {
      return null;
    }
    String rootFolder = System.getProperty( Const.HOP_METASTORE_FOLDER );
    if ( Utils.isEmpty( rootFolder ) ) {
      rootFolder = getDefaultHopMetaStoreLocation();
    }
    File rootFolderFile = new File( rootFolder );
    File metaFolder = new File( rootFolder + File.separator + XmlUtil.META_FOLDER_NAME );
    if ( !allowCreate && !metaFolder.exists() ) {
      return null;
    }
    if ( !rootFolderFile.exists() ) {
      rootFolderFile.mkdirs();
    }

    XmlMetaStore metaStore = new XmlMetaStore( rootFolder );
    if ( allowCreate ) {
      metaStore.setName( Const.HOP_METASTORE_NAME );
    }
    return metaStore;
  }
}
