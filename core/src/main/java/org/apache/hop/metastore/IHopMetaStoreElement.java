package org.apache.hop.metastore;

import org.apache.hop.metastore.api.IHasFactory;
import org.apache.hop.metastore.api.IHasName;

public interface IHopMetaStoreElement<T> extends IHasName, IHasFactory<T> {
}
