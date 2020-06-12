package org.apache.hop.metadata.serializer.memory;

import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.encryption.ITwoWayPasswordEncoder;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.BaseMetadataProvider;

import java.util.HashMap;
import java.util.Map;

public class MemoryMetadataProvider extends BaseMetadataProvider implements IHopMetadataProvider {

  private Map<String, IHopMetadataSerializer<IHopMetadata>> serializerMap;
  private ITwoWayPasswordEncoder twoWayPasswordEncoder;

  public MemoryMetadataProvider() {
    this.serializerMap = new HashMap<>();
    this.twoWayPasswordEncoder = new HopTwoWayPasswordEncoder();
  }

  public MemoryMetadataProvider( ITwoWayPasswordEncoder twoWayPasswordEncoder ) {
    this();
    this.twoWayPasswordEncoder = twoWayPasswordEncoder;
  }

  @Override public <T extends IHopMetadata> IHopMetadataSerializer<T> getSerializer( Class<T> managedClass ) throws HopException {
    IHopMetadataSerializer<IHopMetadata> serializer = serializerMap.get( managedClass.getName() );
    if (serializer==null) {
      serializer = (IHopMetadataSerializer<IHopMetadata>) new MemoryMetadataSerializer<T>( this, managedClass );
      serializerMap.put( managedClass.getName(), serializer);
    }

    return (IHopMetadataSerializer<T>) serializer;
  }

  @Override public ITwoWayPasswordEncoder getTwoWayPasswordEncoder() {
    return twoWayPasswordEncoder;
  }

  /**
   * Gets serializerMap
   *
   * @return value of serializerMap
   */
  public Map<String, IHopMetadataSerializer<IHopMetadata>> getSerializerMap() {
    return serializerMap;
  }

  /**
   * @param serializerMap The serializerMap to set
   */
  public void setSerializerMap(
    Map<String, IHopMetadataSerializer<IHopMetadata>> serializerMap ) {
    this.serializerMap = serializerMap;
  }

  /**
   * @param twoWayPasswordEncoder The twoWayPasswordEncoder to set
   */
  public void setTwoWayPasswordEncoder( ITwoWayPasswordEncoder twoWayPasswordEncoder ) {
    this.twoWayPasswordEncoder = twoWayPasswordEncoder;
  }
}
