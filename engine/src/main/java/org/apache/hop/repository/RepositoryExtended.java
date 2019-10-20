/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.repository;

import org.apache.hop.cluster.ClusterSchema;
import org.apache.hop.cluster.SlaveServer;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.partition.PartitionSchema;

import java.util.List;

/**
 * Additional methods to be added to Repository in next major revision.
 *
 */
@Deprecated
public interface RepositoryExtended extends Repository {
  /**
   * @deprecated more deprecated than the others 
   * Loads the RepositoryDirectoryTree either Eagerly or Lazilly based on the value passed. This value will
   * override the default and any specified setting value for HOP_LAZY_REPOSITORY.
   *
   * @param eager
   * @return
   * @throws HopException
   */
  @Deprecated
  RepositoryDirectoryInterface loadRepositoryDirectoryTree( boolean eager ) throws HopException;

  /**
   * Loads the RepositoryDirectoryTree, filtering at the server.
   * @param path - relative path  to folder from which we should to download the tree. 
   * Implementation should use "/"  as default path, because it is a root folder of repository
   * @param filter - filter may be a full name or a partial name with one or more wildcard characters ("*"), or a disjunction (using the "|" character to represent logical OR) of these;
   * if null then it should be "*" and return all files and folders from root folder
   * @param depth - 0 fetches just file at path; positive integer n fetches node at path plus n levels of children; 
   * negative integer fetches all children. If n > 0 then only the top level children will be processed
   * @param showHidden - if true we will show hidden files
   * @param includeEmptyFolder include directories without any match
   * @param includeAcls include ACLs 
   * @throws HopException
   */
  RepositoryDirectoryInterface loadRepositoryDirectoryTree(
      String path,
      String filter,
      int depth,
      boolean showHidden,
      boolean includeEmptyFolder,
      boolean includeAcls )
    throws HopException;

  /**
   * Move / rename a repository directory
   *
   * @param dirId
   *          The ObjectId of the repository directory to move
   * @param newParent
   *          The RepositoryDirectoryInterface that will be the new parent of the repository directory (May be null if a
   *          move is not desired)
   * @param newName
   *          The new name of the repository directory (May be null if a rename is not desired)
   * @param renameHomeDirectories
   *          true if this is an allowed action
   * @return The ObjectId of the repository directory that was moved
   * @throws HopException
   */
  ObjectId renameRepositoryDirectory( final ObjectId dirId, final RepositoryDirectoryInterface newParent,
                                             final String newName, final boolean renameHomeDirectories ) throws HopException;


  /**
   * Delete a repository directory
   *
   * @param dir
   *          The ObjectId of the repository directory to move
   * @param deleteHomeDirectories
   *          true if this is an allowed action
   * @throws HopException
   */
  void deleteRepositoryDirectory( final RepositoryDirectoryInterface dir, final boolean deleteHomeDirectories )
          throws HopException;

  List<RepositoryObjectInterface> getChildren( String path, String filter );

  List<DatabaseMeta> getConnections( boolean cached ) throws HopException;

  List<SlaveServer> getSlaveServers( boolean cached ) throws HopException;

  List<PartitionSchema> getPartitions( boolean cached ) throws HopException;

  List<ClusterSchema> getClusters( boolean cached ) throws HopException;
}
