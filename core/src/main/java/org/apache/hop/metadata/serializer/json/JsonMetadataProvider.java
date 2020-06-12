package org.apache.hop.metadata.serializer.json;

import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.encryption.ITwoWayPasswordEncoder;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.BaseMetadataProvider;

import java.io.File;

public class JsonMetadataProvider extends BaseMetadataProvider implements IHopMetadataProvider {

  private ITwoWayPasswordEncoder twoWayPasswordEncoder;
  private String baseFolder;

  public JsonMetadataProvider() {
    twoWayPasswordEncoder = new HopTwoWayPasswordEncoder();
    baseFolder="metadata";
  }

  public JsonMetadataProvider( String baseFolder ) {
    this();
    this.baseFolder = baseFolder;
  }

  public JsonMetadataProvider( ITwoWayPasswordEncoder twoWayPasswordEncoder, String baseFolder ) {
    this.twoWayPasswordEncoder = twoWayPasswordEncoder;
    this.baseFolder = baseFolder;
  }


  @Override public <T extends IHopMetadata> IHopMetadataSerializer<T> getSerializer( Class<T> managedClass ) throws HopException {
    if (managedClass==null) {
      throw new HopException("You need to specify the class to serialize");
    }

    // Is this a metadata class?
    //
    HopMetadata hopMetadata = managedClass.getAnnotation( HopMetadata.class );
    if (hopMetadata==null) {
      throw new HopException("To serialize class "+managedClass.getClass().getName()+" it needs to have annotation "+HopMetadata.class.getName());
    }
    String classFolder = Const.NVL(hopMetadata.key(), hopMetadata.name());
    String serializerBaseFolderName = baseFolder + (baseFolder.endsWith( Const.FILE_SEPARATOR ) ? "" : Const.FILE_SEPARATOR) + classFolder;

    // Check if the folder exists...
    //
    File serializerBaseFolder = new File(serializerBaseFolderName);
    if (!serializerBaseFolder.exists()) {
      if (!serializerBaseFolder.mkdirs()) {
        throw new HopException("Unable to create folder '"+serializerBaseFolderName+"'to store JSON serialized objects in from class "+managedClass.getName());
      }
    }

    return new JsonMetadataSerializer<T>( this, serializerBaseFolderName, managedClass );
  }

  /**
   * Gets twoWayPasswordEncoder
   *
   * @return value of twoWayPasswordEncoder
   */
  @Override public ITwoWayPasswordEncoder getTwoWayPasswordEncoder() {
    return twoWayPasswordEncoder;
  }

  /**
   * @param twoWayPasswordEncoder The twoWayPasswordEncoder to set
   */
  public void setTwoWayPasswordEncoder( ITwoWayPasswordEncoder twoWayPasswordEncoder ) {
    this.twoWayPasswordEncoder = twoWayPasswordEncoder;
  }

  /**
   * Gets baseFolder
   *
   * @return value of baseFolder
   */
  public String getBaseFolder() {
    return baseFolder;
  }

  /**
   * @param baseFolder The baseFolder to set
   */
  public void setBaseFolder( String baseFolder ) {
    this.baseFolder = baseFolder;
  }
}
