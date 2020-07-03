package org.apache.hop.metadata.util;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.json.JsonMetadataProvider;

public class HopMetadataUtil {

  public static final IHopMetadataProvider getStandardHopMetadataProvider( IVariables variables ) {

    String folder = variables.getVariable( Const.HOP_METADATA_FOLDER );
    if ( StringUtils.isEmpty( folder ) ) {
      // The folder is the "metadata" folder in the configuration folder...
      //
      String configDirectory = Const.HOP_CONFIG_FOLDER;
      if (!configDirectory.endsWith( Const.FILE_SEPARATOR )) {
        configDirectory+=Const.FILE_SEPARATOR;
      }
      folder=configDirectory+"metadata";
    }
    return new JsonMetadataProvider( new HopTwoWayPasswordEncoder(), folder, variables );
  }


  public static <T extends IHopMetadata> HopMetadata getHopMetadataAnnotation( Class<T> managedClass ) {
    HopMetadata hopMetadata = managedClass.getAnnotation( HopMetadata.class );
    return hopMetadata;
  }
}
