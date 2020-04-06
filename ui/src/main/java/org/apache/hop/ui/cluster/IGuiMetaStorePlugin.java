package org.apache.hop.ui.cluster;

import org.apache.hop.metastore.IHopMetaStoreElement;

/**
 * This interface indicates that the given class is a GUI plugin for a certain MetaStore element type
 * @param <T>
 */
public interface IGuiMetaStorePlugin<T extends IHopMetaStoreElement> {

  Class<T> getMetastoreElementClass();

}
