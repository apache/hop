package org.apache.hop.metadata.api;

import org.apache.hop.core.exception.HopException;

/**
 * This describes a standard interface for instantiating metadata objects and providing the unique identifier for serialization.
 *
 */
public interface IHopMetadataObjectFactory {

  /**
   * Given an ID (usually a plugin ID), instantiate the appropriate object for it.
   * @param id The ID to use to create the object
   * @param parentObject The optional parent object if certain things like variables need to be inherited from it.
   * @return The instantiated object
   */
  Object createObject( String id, Object parentObject ) throws HopException;

  /**
   * get the ID for the given object.  Usually this is the plugin ID.
   * @param object The object to get the ID for
   * @return The object ID
   * @throws HopException
   */
  String getObjectId(Object object) throws HopException;
}
