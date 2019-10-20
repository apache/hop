/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.repository.kdr.delegates;

import java.util.ArrayList;
import java.util.List;

import org.apache.hop.core.Const;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.repository.LongObjectId;
import org.apache.hop.repository.ObjectId;
import org.apache.hop.repository.RepositoryDirectory;
import org.apache.hop.repository.RepositoryDirectoryInterface;
import org.apache.hop.repository.kdr.HopDatabaseRepository;

public class HopDatabaseRepositoryDirectoryDelegate extends HopDatabaseRepositoryBaseDelegate {
  private static Class<?> PKG = RepositoryDirectory.class; // for i18n purposes, needed by Translator2!!

  public HopDatabaseRepositoryDirectoryDelegate( HopDatabaseRepository repository ) {
    super( repository );
  }

  public RowMetaAndData getDirectory( ObjectId id_directory ) throws HopException {
    return repository.connectionDelegate.getOneRow(
      quoteTable( HopDatabaseRepository.TABLE_R_DIRECTORY ),
      quote( HopDatabaseRepository.FIELD_DIRECTORY_ID_DIRECTORY ), id_directory );
  }

  public RepositoryDirectoryInterface loadPathToRoot( ObjectId id_directory ) throws HopException {
    List<RepositoryDirectory> path = new ArrayList<>();

    ObjectId directoryId = id_directory;

    RowMetaAndData directoryRow = getDirectory( directoryId );
    Long parentId = directoryRow.getInteger( 1 );

    // Do not load root itself, it doesn't exist.
    //
    while ( parentId != null && parentId >= 0 ) {
      RepositoryDirectory directory = new RepositoryDirectory();
      directory.setName( directoryRow.getString( 2, null ) ); // Name of the directory
      directory.setObjectId( directoryId );
      path.add( directory );

      // System.out.println( "+ dir '" + directory.getName() + "'" );

      directoryId = new LongObjectId( parentId );
      directoryRow = getDirectory( directoryId );
      parentId = directoryRow.getInteger( HopDatabaseRepository.FIELD_DIRECTORY_ID_DIRECTORY_PARENT );
    }

    RepositoryDirectory root = new RepositoryDirectory();
    root.setObjectId( new LongObjectId( 0 ) );
    path.add( root );

    // Connect the directories to each other.
    //
    for ( int i = 0; i < path.size() - 1; i++ ) {
      RepositoryDirectory item = path.get( i );
      RepositoryDirectory parent = path.get( i + 1 );
      item.setParent( parent );
      parent.addSubdirectory( item );
    }

    RepositoryDirectory repositoryDirectory = path.get( 0 );
    return repositoryDirectory;
  }

  public RepositoryDirectoryInterface loadRepositoryDirectoryTree( RepositoryDirectoryInterface root ) throws HopException {
    try {
      synchronized ( repository ) {

        root.clear();
        ObjectId[] subids = repository.getSubDirectoryIDs( root.getObjectId() );
        for ( int i = 0; i < subids.length; i++ ) {
          RepositoryDirectory subdir = new RepositoryDirectory();
          loadRepositoryDirectory( subdir, subids[i] );
          root.addSubdirectory( subdir );
        }
      }

      return root;
    } catch ( Exception e ) {
      throw new HopException( "An error occured loading the directory tree from the repository", e );
    }
  }

  public void loadRepositoryDirectory( RepositoryDirectory repositoryDirectory, ObjectId id_directory ) throws HopException {
    if ( id_directory == null ) {
      // This is the root directory, id = OL
      id_directory = new LongObjectId( 0L );
    }

    try {
      RowMetaAndData row = getDirectory( id_directory );
      if ( row != null ) {
        repositoryDirectory.setObjectId( id_directory );

        // Content?
        //
        repositoryDirectory.setName( row.getString( "DIRECTORY_NAME", null ) );

        // The sub-directories?
        //
        ObjectId[] subids = repository.getSubDirectoryIDs( repositoryDirectory.getObjectId() );
        for ( int i = 0; i < subids.length; i++ ) {
          RepositoryDirectory subdir = new RepositoryDirectory();
          loadRepositoryDirectory( subdir, subids[i] );
          repositoryDirectory.addSubdirectory( subdir );
        }
      }
    } catch ( Exception e ) {
      throw new HopException( BaseMessages.getString(
        PKG, "Repository.LoadRepositoryDirectory.ErrorLoading.Exception" ), e );
    }
  }

  /*
   * public synchronized RepositoryDirectory refreshRepositoryDirectoryTree() throws HopException { try {
   * RepositoryDirectory tree = new RepositoryDirectory(); loadRepositoryDirectory(tree, tree.getID());
   * repository.setDirectoryTree(tree); return tree; } catch (HopException e) { repository.setDirectoryTree( new
   * RepositoryDirectory() ); throw new HopException("Unable to read the directory tree from the repository!", e); }
   * }
   */

  private synchronized ObjectId insertDirectory( ObjectId id_directory_parent, RepositoryDirectoryInterface dir ) throws HopException {
    ObjectId id = repository.connectionDelegate.getNextDirectoryID();

    String tablename = HopDatabaseRepository.TABLE_R_DIRECTORY;
    RowMetaAndData table = new RowMetaAndData();
    table.addValue( new ValueMetaInteger(
      HopDatabaseRepository.FIELD_DIRECTORY_ID_DIRECTORY ), id );
    table.addValue(
      new ValueMetaInteger(
        HopDatabaseRepository.FIELD_DIRECTORY_ID_DIRECTORY_PARENT ),
      id_directory_parent );
    table.addValue( new ValueMetaString(
      HopDatabaseRepository.FIELD_DIRECTORY_DIRECTORY_NAME ), dir.getName() );

    repository.connectionDelegate.getDatabase().prepareInsert( table.getRowMeta(), tablename );
    repository.connectionDelegate.getDatabase().setValuesInsert( table );
    repository.connectionDelegate.getDatabase().insertRow();
    repository.connectionDelegate.getDatabase().closeInsert();

    return id;
  }

  public synchronized void deleteDirectory( ObjectId id_directory ) throws HopException {
    repository.connectionDelegate.performDelete( "DELETE FROM "
      + quoteTable( HopDatabaseRepository.TABLE_R_DIRECTORY ) + " WHERE "
      + quote( HopDatabaseRepository.FIELD_DIRECTORY_ID_DIRECTORY ) + " = ? ", id_directory );
  }

  public synchronized void deleteDirectory( RepositoryDirectoryInterface dir ) throws HopException {
    String[] trans = repository.getTransformationNames( dir.getObjectId(), false ); // TODO : include or exclude deleted
                                                                                    // objects?
    String[] jobs = repository.getJobNames( dir.getObjectId(), false ); // TODO : include or exclude deleted objects?
    ObjectId[] subDirectories = repository.getSubDirectoryIDs( dir.getObjectId() );
    if ( trans.length == 0 && jobs.length == 0 && subDirectories.length == 0 ) {
      repository.directoryDelegate.deleteDirectory( dir.getObjectId() );
    } else {
      deleteDirectoryRecursively( dir );
    }
  }

  private synchronized void deleteDirectoryRecursively( RepositoryDirectoryInterface dir ) throws HopException {
    String[] trans = repository.getTransformationNames( dir.getObjectId(), false ); // TODO : include or exclude deleted
                                                                                    // objects?
    String[] jobs = repository.getJobNames( dir.getObjectId(), false ); // TODO : include or exclude deleted objects?
    for ( String transformation : trans ) {
      ObjectId id = repository.getTransformationID( transformation, dir );
      repository.deleteTransformation( id );
    }
    for ( String job : jobs ) {
      ObjectId id = repository.getJobId( job, dir );
      repository.deleteJob( id );
    }
    for ( RepositoryDirectoryInterface subDir : dir.getChildren() ) {
      deleteDirectoryRecursively( subDir );
    }
    repository.directoryDelegate.deleteDirectory( dir.getObjectId() );
  }

  /**
   * Move / rename a directory in the repository
   *
   * @param id_directory
   *          Id of the directory to be moved/renamed
   * @param id_directory_parent
   *          Id of the new parent directory (null if the parent does not change)
   * @param newName
   *          New name for this directory (null if the name does not change)
   * @throws HopException
   */
  public synchronized void renameDirectory( ObjectId id_directory, ObjectId id_directory_parent, String newName ) throws HopException {
    if ( id_directory.equals( id_directory_parent ) ) {
      // Make sure the directory cannot become its own parent
      throw new HopException( "Failed to copy directory into itself" );
    } else {
      // Make sure the directory does not become a descendant of itself
      RepositoryDirectory rd = new RepositoryDirectory();
      loadRepositoryDirectory( rd, id_directory );
      if ( rd.findDirectory( id_directory_parent ) != null ) {
        // The parent directory is a child of this directory. Do not proceed
        throw new HopException( "Directory cannot become a child to itself" );
      } else {
        // Check for duplication
        RepositoryDirectory newParent = new RepositoryDirectory();
        loadRepositoryDirectory( newParent, id_directory_parent );
        RepositoryDirectory child = newParent.findChild( newName == null ? rd.getName() : newName );
        if ( child != null ) {
          throw new HopException( "Destination directory already contains a diectory with requested name" );
        }
      }
    }

    if ( id_directory_parent != null || newName != null ) {
      RowMetaAndData r = new RowMetaAndData();

      String sql = "UPDATE " + quoteTable( HopDatabaseRepository.TABLE_R_DIRECTORY ) + " SET ";
      boolean additionalParameter = false;

      if ( newName != null ) {
        additionalParameter = true;
        sql += quote( HopDatabaseRepository.FIELD_DIRECTORY_DIRECTORY_NAME ) + " = ?";
        r.addValue( new ValueMetaString(
          HopDatabaseRepository.FIELD_DIRECTORY_DIRECTORY_NAME ), newName );
      }
      if ( id_directory_parent != null ) {
        // Add a parameter separator if the first parm was added
        if ( additionalParameter ) {
          sql += ", ";
        }
        sql += quote( HopDatabaseRepository.FIELD_DIRECTORY_ID_DIRECTORY_PARENT ) + " = ?";
        r.addValue(
          new ValueMetaInteger(
            HopDatabaseRepository.FIELD_DIRECTORY_ID_DIRECTORY_PARENT ),
          id_directory_parent );
      }

      sql += " WHERE " + quote( HopDatabaseRepository.FIELD_DIRECTORY_ID_DIRECTORY ) + " = ? ";
      r.addValue( new ValueMetaInteger( "id_directory" ), Long.valueOf( id_directory
        .toString() ) );

      repository.connectionDelegate.getDatabase().execStatement( sql, r.getRowMeta(), r.getData() );
    }
  }

  public synchronized int getNrSubDirectories( ObjectId id_directory ) throws HopException {
    int retval = 0;

    RowMetaAndData dirParRow = repository.connectionDelegate.getParameterMetaData( id_directory );
    String sql =
      "SELECT COUNT(*) FROM "
        + quoteTable( HopDatabaseRepository.TABLE_R_DIRECTORY ) + " WHERE "
        + quote( HopDatabaseRepository.FIELD_DIRECTORY_ID_DIRECTORY_PARENT ) + " = ? ";
    RowMetaAndData r = repository.connectionDelegate.getOneRow( sql, dirParRow.getRowMeta(), dirParRow.getData() );
    if ( r != null ) {
      retval = (int) r.getInteger( 0, 0 );
    }

    return retval;
  }

  public synchronized ObjectId[] getSubDirectoryIDs( ObjectId id_directory ) throws HopException {
    return repository.connectionDelegate.getIDs( "SELECT "
      + quote( HopDatabaseRepository.FIELD_DIRECTORY_ID_DIRECTORY ) + " FROM "
      + quoteTable( HopDatabaseRepository.TABLE_R_DIRECTORY ) + " WHERE "
      + quote( HopDatabaseRepository.FIELD_DIRECTORY_ID_DIRECTORY_PARENT ) + " = ? ORDER BY "
      + quote( HopDatabaseRepository.FIELD_DIRECTORY_DIRECTORY_NAME ), id_directory );
  }

  public void saveRepositoryDirectory( RepositoryDirectoryInterface dir ) throws HopException {
    try {
      ObjectId id_directory_parent = null;
      if ( dir.getParent() != null ) {
        id_directory_parent = dir.getParent().getObjectId();
      }

      dir.setObjectId( insertDirectory( id_directory_parent, dir ) );

      log.logDetailed( "New id of directory = " + dir.getObjectId() );

      repository.commit();
    } catch ( Exception e ) {
      throw new HopException( "Unable to save directory [" + dir + "] in the repository", e );
    }
  }

  public void delRepositoryDirectory( RepositoryDirectoryInterface dir, boolean deleteNonEmptyFolder ) throws HopException {
    try {
      if ( !deleteNonEmptyFolder ) {
        String[] trans = repository.getTransformationNames( dir.getObjectId(), false ); // TODO : include or exclude
                                                                                        // deleted objects?
        String[] jobs = repository.getJobNames( dir.getObjectId(), false ); // TODO : include or exclude deleted
                                                                            // objects?
        ObjectId[] subDirectories = repository.getSubDirectoryIDs( dir.getObjectId() );
        if ( trans.length == 0 && jobs.length == 0 && subDirectories.length == 0 ) {
          repository.directoryDelegate.deleteDirectory( dir.getObjectId() );
          repository.commit();
        } else {
          throw new HopException( "This directory is not empty!" );
        }
      } else {
        repository.directoryDelegate.deleteDirectory( dir );
        repository.commit();
      }
    } catch ( Exception e ) {
      throw new HopException( "Unexpected error deleting repository directory:", e );
    }
  }

  /**
   * @deprecated use {@link #renameRepositoryDirectory(ObjectId, RepositoryDirectoryInterface, String)}
   *
   * @param dir
   * @return
   * @throws HopException
   */
  @Deprecated
  public ObjectId renameRepositoryDirectory( RepositoryDirectory dir ) throws HopException {
    try {
      renameDirectory( dir.getObjectId(), null, dir.getName() );
      return dir.getObjectId(); // doesn't change in this specific case.
    } catch ( Exception e ) {
      throw new HopException( "Unable to rename the specified repository directory [" + dir + "]", e );
    }
  }

  public ObjectId renameRepositoryDirectory( ObjectId id, RepositoryDirectoryInterface newParentDir, String newName ) throws HopException {
    ObjectId parentId = null;
    if ( newParentDir != null ) {
      parentId = newParentDir.getObjectId();
    }

    try {
      renameDirectory( id, parentId, newName );
      return id; // doesn't change in this specific case.
    } catch ( Exception e ) {
      throw new HopException( "Unable to rename the specified repository directory [" + id + "]", e );
    }
  }

  /**
   * Create a new directory, possibly by creating several sub-directies of / at the same time.
   *
   * @param parentDirectory
   *          the parent directory
   * @param directoryPath
   *          The path to the new Repository Directory, to be created.
   * @return The created sub-directory
   * @throws HopException
   *           In case something goes wrong
   */
  public RepositoryDirectoryInterface createRepositoryDirectory( RepositoryDirectoryInterface parentDirectory,
    String directoryPath ) throws HopException {

    // RepositoryDirectoryInterface refreshedParentDir =
    // repository.loadRepositoryDirectoryTree().findDirectory(parentDirectory.getPath());

    RepositoryDirectoryInterface refreshedParentDir = parentDirectory;
    String[] path = Const.splitPath( directoryPath, RepositoryDirectory.DIRECTORY_SEPARATOR );

    RepositoryDirectoryInterface parent = refreshedParentDir;
    for ( int level = 0; level < path.length; level++ ) {

      RepositoryDirectoryInterface rd = parent.findChild( path[level] );
      if ( rd == null ) {
        // This child directory doesn't exists, let's add it!
        //
        rd = new RepositoryDirectory( parent, path[level] );
        saveRepositoryDirectory( rd );

        // Don't forget to add this directory to the tree!
        //
        parent.addSubdirectory( rd );

        parent = rd;
      } else {
        parent = rd;
      }
    }
    return parent;
  }

}
