package org.apache.hop.metadata.api;

import org.apache.hop.core.exception.HopException;

import java.util.List;

/**
 * This metadata interface describes how an object T can be serialized and analyzed.
 *
 * @param <T>
 */
public interface IHopMetadataSerializer<T extends IHopMetadata> {

  /**
   * Load (de-serialize) an object with the given name
   * @param objectName
   * @return The object with the specified name
   * @throws HopException
   */
  T load(String objectName) throws HopException;

  /**
   * Save (serialize) the provided object.  If an object with the same name exists, it is updated, otherwise it's created.
   * @param object
   * @throws HopException
   */
  void save(T object) throws HopException;

  /**
   * Delete the object with the given name.
   * @param name The name of the object to delete
   * @return The deleted object
   * @throws HopException
   */
  T delete(String name) throws HopException;

  /**
   * List all the object names
   * @return A list of object names
   * @throws HopException
   */
  List<String> listObjectNames() throws HopException;

  /**
   * See if an object with the given name exists.
   * @param name The name of the object to check
   * @return true if an object with the given name exists.
   * @throws HopException
   */
  boolean exists(String name) throws HopException;

  /**
   * @return The class of the objects which are saved and loaded.
   */
  Class<T> getManagedClass();

  /**
   * The metadata provider allowing you to serialize other objects.
   * @return The metadata provider of this serializer
   */
  IHopMetadataProvider getMetadataProvider();

  /**
   * Load all available objects
   * @return A list of all available objects
   */
  List<T> loadAll() throws HopException;
}
