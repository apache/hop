package org.apache.hop.core.search;

/**
 * The searchable object is an object which can be examined by a search engine.
 *
 */
public interface ISearchable<T> {
  /**
   * Where the searchable is
   * @return
   */
  String getLocation();

  /**
   * What it is called
   * @return
   */
  String getName();

  /**
   * @return The type of searchable: pipeline, workflow, type of metadata object, ...
   */
  String getType();

  /**
   * What the filename is (if there is any)
   * @return
   */
  String getFilename();

  /**
   * @return The object to search itself
   */
  T getSearchableObject();

  /**
   * In case you want to have a callback for this searchable.
   * This can be used to open or locate the searchable object.
   *
   * @return The callback
   */
  ISearchableCallback getSearchCallback();
}
