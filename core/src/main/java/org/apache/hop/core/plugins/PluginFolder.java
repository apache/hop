/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
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

package org.apache.hop.core.plugins;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.Variables;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A folder to search plugins in.
 *
 * @author matt
 */
public class PluginFolder implements IPluginFolder {

  public static final String VAR_HOP_PLUGIN_BASE_FOLDERS = "HOP_PLUGIN_BASE_FOLDERS";
  private String folder;
  private boolean pluginXmlFolder;
  private boolean pluginAnnotationsFolder;
  private boolean searchLibDir;

  /**
   * @param folder                  The folder location
   * @param pluginXmlFolder         set to true if the folder needs to be searched for plugin.xml appearances
   * @param pluginAnnotationsFolder set to true if the folder needs to be searched for jar files with plugin annotations
   */
  public PluginFolder( String folder, boolean pluginXmlFolder, boolean pluginAnnotationsFolder ) {
    this( folder, pluginXmlFolder, pluginAnnotationsFolder, false );
  }

  /**
   * @param folder                  The folder location
   * @param pluginXmlFolder         set to true if the folder needs to be searched for plugin.xml appearances
   * @param pluginAnnotationsFolder set to true if the folder needs to be searched for jar files with plugin annotations
   * @param searchLibDir            look inside the plugins lib dir for additional plugins
   */
  public PluginFolder( String folder, boolean pluginXmlFolder, boolean pluginAnnotationsFolder,
                       boolean searchLibDir ) {
    this.folder = folder;
    this.pluginXmlFolder = pluginXmlFolder;
    this.pluginAnnotationsFolder = pluginAnnotationsFolder;
    this.searchLibDir = searchLibDir;
  }

  /**
   * Create a list of plugin folders based on the specified xml sub folder
   *
   * @param xmlSubfolder the sub-folder to consider for XML plugin files or null if it's not applicable.
   * @return The list of plugin folders found
   */
  public static List<IPluginFolder> populateFolders( String xmlSubfolder ) {
    List<IPluginFolder> pluginFolders = new ArrayList<>();

    String folderPaths = Const.NVL( Variables.getADefaultVariableSpace().getVariable( VAR_HOP_PLUGIN_BASE_FOLDERS ), EnvUtil.getSystemProperty( VAR_HOP_PLUGIN_BASE_FOLDERS ) );
    if ( folderPaths == null ) {
      folderPaths = Const.DEFAULT_PLUGIN_BASE_FOLDERS;
    }
    String[] folders = folderPaths.split( "," );
    // for each folder in the list of plugin base folders
    // add an annotation and xml path for searching
    // trim the folder - we don't need leading and trailing spaces
    for ( String folder : folders ) {
      folder = folder.trim();
      pluginFolders.add( new PluginFolder( folder, false, true ) );
      if ( !Utils.isEmpty( xmlSubfolder ) ) {
        pluginFolders.add( new PluginFolder( folder + File.separator + xmlSubfolder, true, false ) );
      }
    }
    return pluginFolders;
  }

  @Override
  public String toString() {
    return folder;
  }

  @Override
  public File[] findJarFiles() throws HopFileException {
    return findJarFiles( searchLibDir );
  }

  public File[] findJarFiles( final boolean includeLibJars ) throws HopFileException {

    try {
      // Find all the jar files in this folder...
      //
      File folderFile = new File( this.getFolder() );

      Set<File> pluginJarFiles = findFiles(folderFile);

      return pluginJarFiles.toArray(new File[0]);
    } catch ( Exception e ) {
      throw new HopFileException( "Unable to list jar files in plugin folder '" + toString() + "'", e );
    }
  }

  private Set<File> findFiles( File folder ) {
    Set<File> files = new HashSet<>();
    File[] children = folder.listFiles();
    if (children!=null) {
      for ( File child : children ) {
        if ( child.isFile() ) {
          if ( child.getName().endsWith( ".jar" ) ) {
            files.add( child );
          }
        }
        if ( child.isDirectory() ) {
          if ( !"lib".equals( child.getName() ) ) {
            files.addAll( findFiles( child ) );
          }
        }
      }
    }

    return files;
  }

  /**
   * @return the folder
   */
  @Override
  public String getFolder() {
    return folder;
  }

  /**
   * @param folder the folder to set
   */
  public void setFolder( String folder ) {
    this.folder = folder;
  }

  /**
   * @return the pluginXmlFolder
   */
  @Override
  public boolean isPluginXmlFolder() {
    return pluginXmlFolder;
  }

  /**
   * @param pluginXmlFolder the pluginXmlFolder to set
   */
  public void setPluginXmlFolder( boolean pluginXmlFolder ) {
    this.pluginXmlFolder = pluginXmlFolder;
  }

  /**
   * @return the pluginAnnotationsFolder
   */
  @Override
  public boolean isPluginAnnotationsFolder() {
    return pluginAnnotationsFolder;
  }

  /**
   * @param pluginAnnotationsFolder the pluginAnnotationsFolder to set
   */
  public void setPluginAnnotationsFolder( boolean pluginAnnotationsFolder ) {
    this.pluginAnnotationsFolder = pluginAnnotationsFolder;
  }

  @Override public int hashCode() {
    return new HashCodeBuilder()
      .append( folder )
      .append( pluginAnnotationsFolder )
      .append( pluginXmlFolder )
      .append( searchLibDir )
      .hashCode();
  }

  @Override public boolean equals( Object obj ) {
    if ( obj == null ) {
      return false;
    }
    if ( obj == this ) {
      return true;
    }
    if ( obj.getClass() != getClass() ) {
      return false;
    }

    PluginFolder other = (PluginFolder) obj;
    return new EqualsBuilder()
      .append( folder, other.getFolder() )
      .append( pluginAnnotationsFolder, other.isPluginAnnotationsFolder() )
      .append( pluginXmlFolder, other.isPluginXmlFolder() )
      .append( searchLibDir, other.searchLibDir )
      .isEquals();
  }
}
