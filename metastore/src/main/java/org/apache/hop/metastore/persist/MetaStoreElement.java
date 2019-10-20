package org.apache.hop.metastore.persist;

public @interface MetaStoreElement {
  MetaStoreElementType elementType();

  String name();
}
