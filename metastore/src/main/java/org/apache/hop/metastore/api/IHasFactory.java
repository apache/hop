package org.apache.hop.metastore.api;

import org.apache.hop.metastore.persist.MetaStoreFactory;

public interface IHasFactory<T> {
  /**
   * Return the factory to manage elements in the given metastore.
   *
   * @param metaStore The metastore
   * @return The factory to use
   */
  MetaStoreFactory<T> getFactory( IMetaStore metaStore );
}
