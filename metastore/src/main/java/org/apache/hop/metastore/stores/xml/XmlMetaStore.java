/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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

/*!
 * This program is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License, version 2.1 as published by the Free Software
 * Foundation.
 *
 * You should have received a copy of the GNU Lesser General Public License along with this
 * program; if not, you can obtain a copy at http://www.gnu.org/licenses/old-licenses/lgpl-2.1.html
 * or from the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Lesser General Public License for more details.
 *
 * Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
 */

package org.apache.hop.metastore.stores.xml;

import org.apache.hop.metastore.api.BaseMetaStore;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.api.IMetaStoreAttribute;
import org.apache.hop.metastore.api.IMetaStoreElement;
import org.apache.hop.metastore.api.IMetaStoreElementType;
import org.apache.hop.metastore.api.exceptions.MetaStoreDependenciesExistsException;
import org.apache.hop.metastore.api.exceptions.MetaStoreElementExistException;
import org.apache.hop.metastore.api.exceptions.MetaStoreElementTypeExistsException;
import org.apache.hop.metastore.api.exceptions.MetaStoreException;
import org.apache.hop.metastore.api.security.IMetaStoreElementOwner;
import org.apache.hop.metastore.api.security.MetaStoreElementOwnerType;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class XmlMetaStore extends BaseMetaStore implements IMetaStore {

  private String rootFolder;

  private File rootFile;

  private final IXmlMetaStoreCache metaStoreCache;

  public XmlMetaStore() throws MetaStoreException {
    this( new AutomaticXmlMetaStoreCache() );
  }

  public XmlMetaStore( IXmlMetaStoreCache metaStoreCacheImpl ) throws MetaStoreException {
    this( System.getProperty( "java.io.tmpdir" ) + File.separator + UUID.randomUUID(), metaStoreCacheImpl );
  }

  public XmlMetaStore( String rootFolder ) throws MetaStoreException {
    this( rootFolder, new AutomaticXmlMetaStoreCache() );
  }

  public XmlMetaStore( String rootFolder, IXmlMetaStoreCache metaStoreCacheImpl ) throws MetaStoreException {
    this.rootFolder = rootFolder + File.separator + XmlUtil.META_FOLDER_NAME;

    rootFile = new File( this.rootFolder );
    if ( !rootFile.exists() ) {
      if ( !rootFile.mkdirs() ) {
        throw new MetaStoreException( "Unable to create XML meta store root folder: " + this.rootFolder );
      }
    }

    // Give the MetaStore a default name
    //
    setName( this.rootFolder );
    metaStoreCache = metaStoreCacheImpl;
  }

  @Override
  public boolean equals( Object obj ) {
    if ( this == obj ) {
      return true;
    }
    if ( !( obj instanceof XmlMetaStore ) ) {
      return false;
    }
    return ( (XmlMetaStore) obj ).name.equalsIgnoreCase( name );
  }
  

  @Override
  public synchronized List<IMetaStoreElementType> getElementTypes() throws MetaStoreException {
    return getElementTypes( true );
  }

  protected synchronized List<IMetaStoreElementType> getElementTypes( boolean lock )
    throws MetaStoreException {
    if ( lock ) {
      lockStore();
    }
    try {
      String spaceFolder = XmlUtil.getRootFolder( rootFolder );
      File spaceFolderFile = new File( spaceFolder );
      File[] elementTypeFolders = listFolders( spaceFolderFile );
      List<IMetaStoreElementType> elementTypes = new ArrayList<IMetaStoreElementType>( elementTypeFolders.length );
      for ( File elementTypeFolder : elementTypeFolders ) {
        String elementTypeId = elementTypeFolder.getName();
        IMetaStoreElementType elementType = getElementType( elementTypeId, false );
        elementTypes.add( elementType );
      }

      return elementTypes;
    } finally {
      if ( lock ) {
        unlockStore();
      }
    }
  }

  @Override
  public synchronized List<String> getElementTypeIds() throws MetaStoreException {
    lockStore();
    try {
      String spaceFolder = XmlUtil.getRootFolder( rootFolder );
      File spaceFolderFile = new File( spaceFolder );
      File[] elementTypeFolders = listFolders( spaceFolderFile );
      List<String> ids = new ArrayList<>( elementTypeFolders.length );
      for ( File elementTypeFolder : elementTypeFolders ) {
        String elementTypeId = elementTypeFolder.getName();
        ids.add( elementTypeId );
      }

      return ids;
    } finally {
      unlockStore();
    }
  }

  protected synchronized XmlMetaStoreElementType getElementType( String elementTypeId, boolean lock )
    throws MetaStoreException {
    if ( lock ) {
      lockStore();
    }
    try {
      String elementTypeFile = XmlUtil.getElementTypeFile( rootFolder, elementTypeId );
      XmlMetaStoreElementType elementType = new XmlMetaStoreElementType( elementTypeFile );
      elementType.setMetaStoreName( getName() );
      return elementType;
    } finally {
      if ( lock ) {
        unlockStore();
      }
    }
  }

  public synchronized XmlMetaStoreElementType getElementType( String elementTypeId )
    throws MetaStoreException {
    return getElementType( elementTypeId, true );
  }

  @Override
  public synchronized XmlMetaStoreElementType getElementTypeByName( String elementTypeName )
    throws MetaStoreException {
    for ( IMetaStoreElementType elementType : getElementTypes() ) {
      if ( elementType.getName() != null && elementType.getName().equalsIgnoreCase( elementTypeName ) ) {
        return (XmlMetaStoreElementType) elementType;
      }
    }
    return null;
  }

  public IMetaStoreAttribute newAttribute( String id, Object value ) throws MetaStoreException {
    return new XmlMetaStoreAttribute( id, value );
  }

  @Override
  public synchronized void createElementType( IMetaStoreElementType elementType )
    throws MetaStoreException, MetaStoreElementTypeExistsException {
    lockStore();
    try {
      // In the case of a file, the ID is the name
      //
      if ( elementType.getId() == null ) {
        elementType.setId( elementType.getName() );
      }

      String elementTypeFolder = XmlUtil.getElementTypeFolder( rootFolder, elementType.getName() );
      File elementTypeFolderFile = new File( elementTypeFolder );
      if ( elementTypeFolderFile.exists() ) {
        throw new MetaStoreElementTypeExistsException( getElementTypes( false ),
          "The specified element type already exists with the same ID" );
      }
      if ( !elementTypeFolderFile.mkdir() ) {
        throw new MetaStoreException( "Unable to create XML meta store element type folder '" + elementTypeFolder + "'" );
      }

      String elementTypeFilename = XmlUtil.getElementTypeFile( rootFolder, elementType.getName() );

      // Copy the element type information to the XML meta store
      //
      XmlMetaStoreElementType xmlType =
        new XmlMetaStoreElementType( elementType.getId(), elementType.getName(), elementType
          .getDescription() );
      xmlType.setFilename( elementTypeFilename );
      xmlType.save();

      metaStoreCache.registerElementTypeIdForName( elementType.getName(), elementType.getId() );
      metaStoreCache.registerProcessedFile( elementTypeFolder, new File( elementTypeFolder ).lastModified() );

      xmlType.setMetaStoreName( getName() );
      elementType.setMetaStoreName( getName() );
    } finally {
      unlockStore();
    }
  }

  @Override
  public synchronized void updateElementType( IMetaStoreElementType elementType )
    throws MetaStoreException {
    lockStore();
    try {
      String elementTypeFolder = XmlUtil.getElementTypeFolder( rootFolder, elementType.getName() );
      File elementTypeFolderFile = new File( elementTypeFolder );
      if ( !elementTypeFolderFile.exists() ) {
        throw new MetaStoreException( "The specified element type with ID '" + elementType.getId()
          + "' doesn't exists so we can't update it." );
      }

      String elementTypeFilename = XmlUtil.getElementTypeFile( rootFolder, elementType.getName() );

      // Save the element type information to the XML meta store
      //
      XmlMetaStoreElementType xmlType =
        new XmlMetaStoreElementType( elementType.getId(), elementType.getName(), elementType
          .getDescription() );
      xmlType.setFilename( elementTypeFilename );
      xmlType.save();

      metaStoreCache.registerElementTypeIdForName( elementType.getName(), elementType.getId() );
      metaStoreCache.registerProcessedFile( elementTypeFolder, elementTypeFolderFile.lastModified() );
    } finally {
      unlockStore();
    }
  }

  @Override
  public synchronized void deleteElementType( IMetaStoreElementType elementType )
    throws MetaStoreException, MetaStoreDependenciesExistsException {
    lockStore();
    try {
      String elementTypeFilename = XmlUtil.getElementTypeFile( rootFolder, elementType.getName() );
      File elementTypeFile = new File( elementTypeFilename );
      if ( !elementTypeFile.exists() ) {
        return;
      }
      // Check if the element type has no remaining elements
      List<IMetaStoreElement> elements = getElements( elementType, false, true );
      if ( !elements.isEmpty() ) {
        List<String> dependencies = new ArrayList<>();
        for ( IMetaStoreElement element : elements ) {
          dependencies.add( element.getId() );
        }
        throw new MetaStoreDependenciesExistsException( dependencies, "Unable to delete element type with name '"
          + elementType.getName() + "' because there are still elements present" );
      }

      // Remove the elementType.xml file
      //
      if ( !elementTypeFile.delete() ) {
        throw new MetaStoreException( "Unable to delete element type XML file '" + elementTypeFilename + "'" );
      }

      // Remove the folder too, should be empty by now.
      //
      String elementTypeFolder = XmlUtil.getElementTypeFolder( rootFolder, elementType.getName() );
      File elementTypeFolderFile = new File( elementTypeFolder );
      if ( !elementTypeFolderFile.delete() ) {
        throw new MetaStoreException( "Unable to delete element type XML folder '" + elementTypeFolder + "'" );
      }
      metaStoreCache.unregisterElementTypeId( elementType.getId() );
      metaStoreCache.unregisterProcessedFile( elementTypeFolder );
    } finally {
      unlockStore();
    }
  }

  @Override
  public List<IMetaStoreElement> getElements( IMetaStoreElementType elementType )
    throws MetaStoreException {
    return getElements( elementType, true, true );
  }

  protected synchronized List<IMetaStoreElement> getElements( IMetaStoreElementType elementType,
                                                              boolean lock, boolean includeProcessedFiles ) throws MetaStoreException {
    if ( lock ) {
      lockStore();
    }
    try {
      String elementTypeFolder = XmlUtil.getElementTypeFolder( rootFolder, elementType.getName() );
      File elementTypeFolderFile = new File( elementTypeFolder );
      File[] elementTypeFiles = listFiles( elementTypeFolderFile, includeProcessedFiles );
      List<IMetaStoreElement> elements = new ArrayList<IMetaStoreElement>( elementTypeFiles.length );
      for ( File elementTypeFile : elementTypeFiles ) {
        String elementId = elementTypeFile.getName();
        // File .type.xml doesn't hidden in OS Windows so better to ignore it explicitly
        if ( elementId.equals( XmlUtil.ELEMENT_TYPE_FILE_NAME ) ) {
          continue;
        }
        elementId = elementId.substring( 0, elementId.length() - 4 ); // remove .xml to get the ID
        elements.add( getElement( elementType, elementId, false ) );
      }

      return elements;
    } finally {
      if ( lock ) {
        unlockStore();
      }
    }
  }

  @Override
  public synchronized List<String> getElementIds( IMetaStoreElementType elementType )
    throws MetaStoreException {
    lockStore();
    try {
      String elementTypeFolder = XmlUtil.getElementTypeFolder( rootFolder, elementType.getName() );
      File elementTypeFolderFile = new File( elementTypeFolder );
      File[] elementTypeFiles = listFiles( elementTypeFolderFile, true );
      List<String> elementIds = new ArrayList<>( elementTypeFiles.length );
      for ( File elementTypeFile : elementTypeFiles ) {
        String elementId = elementTypeFile.getName();
        // File .type.xml doesn't hidden in OS Windows so better to ignore it explicitly
        if ( elementId.equals( XmlUtil.ELEMENT_TYPE_FILE_NAME ) ) {
          continue;
        }
        elementId = elementId.substring( 0, elementId.length() - 4 ); // remove .xml to get the ID
        elementIds.add( elementId );
      }

      return elementIds;
    } finally {
      unlockStore();
    }
  }

  @Override
  public IMetaStoreElement getElement( IMetaStoreElementType elementType, String elementId )
    throws MetaStoreException {
    return getElement( elementType, elementId, true );
  }

  protected synchronized IMetaStoreElement getElement( IMetaStoreElementType elementType,
                                                       String elementId, boolean lock ) throws MetaStoreException {
    if ( lock ) {
      lockStore();
    }
    try {
      String elementFilename = XmlUtil.getElementFile( rootFolder, elementType.getName(), elementId );
      File elementFile = new File( elementFilename );
      if ( !elementFile.exists() ) {
        return null;
      }
      XmlMetaStoreElement element = new XmlMetaStoreElement( elementFilename );
      metaStoreCache.registerElementIdForName( elementType, element.getName(), elementId );
      metaStoreCache.registerProcessedFile( elementFilename, elementFile.lastModified() );
      return element;
    } finally {
      if ( lock ) {
        unlockStore();
      }
    }
  }

  @Override
  public synchronized IMetaStoreElement getElementByName( IMetaStoreElementType elementType, String name )
    throws MetaStoreException {
    lockStore();
    try {
      String chachedElementId = metaStoreCache.getElementIdByName( elementType, name );
      if ( chachedElementId != null ) {
        IMetaStoreElement element = getElement( elementType, chachedElementId, false );
        if ( element != null && element.getName().equalsIgnoreCase( name ) ) {
          return element;
        }
      }

      for ( IMetaStoreElement element : getElements( elementType, false, false ) ) {
        if ( element.getName() != null && element.getName().equalsIgnoreCase( name ) ) {
          return element;
        }
      }
      return null;
    } finally {
      unlockStore();
    }
  }

  public synchronized void
  createElement( IMetaStoreElementType elementType, IMetaStoreElement element )
    throws MetaStoreException, MetaStoreElementExistException {
    lockStore();
    try {
      // In the case of a file, the ID is the name
      //
      if ( element.getId() == null ) {
        element.setId( element.getName() );
      }

      String elementFilename = XmlUtil.getElementFile( rootFolder, elementType.getName(), element.getId() );
      File elementFile = new File( elementFilename );
      if ( elementFile.exists() ) {
        throw new MetaStoreElementExistException( getElements( elementType, false, true ),
          "The specified element already exists with the same ID: '" + element.getId() + "'" );
      }
      XmlMetaStoreElement xmlElement = new XmlMetaStoreElement( element );
      xmlElement.setFilename( elementFilename );
      xmlElement.save();

      metaStoreCache.registerElementIdForName( elementType, xmlElement.getName(), element.getId() );
      metaStoreCache.registerProcessedFile( elementFilename, new File( elementFilename ).lastModified() );
      // In the case of the XML store, the name is the same as the ID
      //
      element.setId( xmlElement.getName() );
    } finally {
      unlockStore();
    }
  }

  @Override
  public synchronized void updateElement( IMetaStoreElementType elementType, String elementId,
                                          IMetaStoreElement element ) throws MetaStoreException {

    // verify that the element type belongs to this meta store
    //
    if ( elementType.getMetaStoreName() == null || !elementType.getMetaStoreName().equals( getName() ) ) {
      throw new MetaStoreException( "The element type '" + elementType.getName()
        + "' needs to explicitly belong to the meta store in which you are updating." );
    }

    lockStore();
    try {
      String elementFilename = XmlUtil.getElementFile( rootFolder, elementType.getName(), element.getName() );
      File elementFile = new File( elementFilename );
      if ( !elementFile.exists() ) {
        throw new MetaStoreException( "The specified element to update doesn't exist with ID: '" + elementId + "'" );
      }

      XmlMetaStoreElement xmlElement = new XmlMetaStoreElement( element );
      xmlElement.setFilename( elementFilename );
      xmlElement.setIdWithFilename( elementFilename );
      xmlElement.save();

      metaStoreCache.registerElementIdForName( elementType, xmlElement.getName(), xmlElement.getId() );
      metaStoreCache.registerProcessedFile( elementFilename, elementFile.lastModified() );
    } finally {
      unlockStore();
    }
  }

  @Override
  public synchronized void deleteElement( IMetaStoreElementType elementType, String elementId )
    throws MetaStoreException {
    lockStore();
    try {
      String elementFilename = XmlUtil.getElementFile( rootFolder, elementType.getName(), elementId );
      File elementFile = new File( elementFilename );
      if ( !elementFile.exists() ) {
        return;
      }

      if ( !elementFile.delete() ) {
        throw new MetaStoreException( "Unable to delete element with ID '" + elementId + "' in filename '"
          + elementFilename + "'" );
      }

      metaStoreCache.unregisterElementId( elementType, elementId );
      metaStoreCache.unregisterProcessedFile( elementFilename );
    } finally {
      unlockStore();
    }
  }

  /**
   * @return the rootFolder
   */
  public String getRootFolder() {
    return rootFolder;
  }

  /**
   * @param rootFolder the rootFolder to set
   */
  public void setRootFolder( String rootFolder ) {
    this.rootFolder = rootFolder;
  }

  /**
   * @param folder
   * @return the non-hidden folders in the specified folder
   */
  protected File[] listFolders( File folder ) {
    File[] folders = folder.listFiles( file -> !file.isHidden() && file.isDirectory() );
    if ( folders == null ) {
      folders = new File[] {};
    }
    return folders;
  }

  /**
   * @param folder
   * @param includeProcessedFiles
   * @return the non-hidden files in the specified folder
   */
  protected File[] listFiles( File folder, final boolean includeProcessedFiles ) {
    File[] files = folder.listFiles( new FileFilter() {
      @Override
      public boolean accept( File file ) {
        if ( !includeProcessedFiles ) {
          Map<String, Long> processedFiles = metaStoreCache.getProcessedFiles();
          Long fileLastModified = processedFiles.get( file.getPath() );
          if ( fileLastModified != null && fileLastModified.equals( file.lastModified() ) ) {
            return false;
          }
        }
        return !file.isHidden() && file.isFile();
      }
    } );
    if ( files == null ) {
      files = new File[] {};
    }
    return files;
  }

  @Override
  public IMetaStoreElementType newElementType() throws MetaStoreException {
    return new XmlMetaStoreElementType( null, null, null );
  }

  @Override
  public IMetaStoreElement newElement() throws MetaStoreException {
    return new XmlMetaStoreElement();
  }

  @Override
  public IMetaStoreElement newElement( IMetaStoreElementType elementType, String id, Object value )
    throws MetaStoreException {
    return new XmlMetaStoreElement( elementType, id, value );
  }

  @Override
  public IMetaStoreElementOwner newElementOwner( String name, MetaStoreElementOwnerType ownerType )
    throws MetaStoreException {
    return new XmlMetaStoreElementOwner( name, ownerType );
  }

  /**
   * Create a .lock file in the store root folder. If it already exists, wait until it becomes available.
   *
   * @throws MetaStoreException in case we have to wait more than 10 seconds to acquire a lock
   */
  protected void lockStore() throws MetaStoreException {
    boolean waiting = true;
    long totalTime = 0L;
    while ( waiting ) {
      File lockFile = new File( rootFile, ".lock" );
      try {
        if ( lockFile.createNewFile() ) {
          return;
        }
      } catch ( IOException e ) {
        throw new MetaStoreException( "Unable to create lock file: " + lockFile.toString(), e );
      }
      try {
        Thread.sleep( 100 );
      } catch ( InterruptedException e ) {
        throw new RuntimeException( e );
      }
      totalTime += 100;
      if ( totalTime > 10000 ) {
        throw new MetaStoreException( "Maximum wait time of 10 seconds exceed while acquiring lock using file '"+lockFile.toString()+"' for metastore: "+getName() );
      }
    }
  }

  protected void unlockStore() {
    File lockFile = new File( rootFile, ".lock" );
    lockFile.delete();
  }
}
