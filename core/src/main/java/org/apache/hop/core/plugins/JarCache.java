/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.core.plugins;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarFile;
import java.util.zip.ZipEntry;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.variables.Variables;
import org.jboss.jandex.Index;
import org.jboss.jandex.IndexReader;

public class JarCache {
  
  public static final String ANNOTATION_INDEX_LOCATION = "META-INF/jandex.idx";

  private static JarCache instance;

  private final Map<File, Index> indexCache;
  private final Map<File, Set<File>> jarFiles;
  private final Set<File> nativeFiles;
  private final Set<File> pluginFiles;
  
  private JarCache() {
    nativeFiles = new HashSet<>();
    pluginFiles = new HashSet<>();
    jarFiles = new HashMap<>();
    indexCache = new HashMap<>();
  }

  public static JarCache getInstance() {
    if (instance == null) {
      instance = new JarCache();
    }
    return instance;
  }
    
  /**
   * Create a list of plugin folders based on the default or variable HOP_PLUGIN_BASE_FOLDERS
   *
   * @return The list of plugin folders found
   */
  public  List<String> getPluginFolders() {
    List<String> pluginFolders = new ArrayList<>();

    String folderPaths = Const.NVL( Variables.getADefaultVariableSpace().getVariable( Const.HOP_PLUGIN_BASE_FOLDERS ), EnvUtil.getSystemProperty( Const.HOP_PLUGIN_BASE_FOLDERS ) );
    if ( folderPaths == null ) {
      folderPaths = Const.DEFAULT_PLUGIN_BASE_FOLDERS;
    }

    // for each folder in the list of plugin base folders
    String[] folders = folderPaths.split( "," );
    for ( String folder : folders ) {
     // trim the folder - we don't need leading and trailing spaces
      pluginFolders.add( folder.trim() );
    }
    return pluginFolders;
  }

  
  public Set<File> getNativeJars() throws HopFileException {

      // Scan native jars only once
      //
      if (nativeFiles.isEmpty()) {
        try {
          Enumeration<URL> indexFiles  = getClass().getClassLoader().getResources(JarCache.ANNOTATION_INDEX_LOCATION);
          while (indexFiles.hasMoreElements()) {
            URL url = indexFiles.nextElement();          
            
            // Relative path
            String path = url.getFile();            

            File file = new File(StringUtils.substringBefore(path,  "!/"));            
            
            nativeFiles.add(file);
            
            // Read annotation index from resource
            //
            try (InputStream stream = url.openStream()) {
                IndexReader reader = new IndexReader(stream);
                Index index = reader.read();                
                indexCache.put(file, index);
              } catch (IOException e) {
                throw new HopFileException(
                    MessageFormat.format("Error reading annotation index from url ''{0}''", url), e);
              }
            
          }
        } catch (Exception e) {
          throw new HopFileException("Error finding native plugin jar", e);
        }
      }

      return nativeFiles;
    }

  /**
   * Get all the jar files with annotation index
   * 
   * @return list of jar files
   * @throws HopFileException
   */  
  public Set<File> getPluginJars() throws HopFileException {
    // Scan plugin jars only once
    //
    if (pluginFiles.isEmpty()) {
            
      for (String pluginFolder : getPluginFolders()) {
        
        // System.out.println("Search plugin in folder: " + pluginFolder );
        
        for (File file : this.findJarFiles(new File(pluginFolder))) {
          Index index = this.getIndex(file);
          if (index != null) {
            pluginFiles.add(file);
          }
        }
      }
    }
    return pluginFiles;
  }

  public Index getIndex(File jarFile) throws HopFileException {
    
    // Search annotation index from cache
    //
    Index index = indexCache.get(jarFile);
    
    if (index == null) {

      try (JarFile jar = new JarFile(jarFile)) {
        ZipEntry entry = jar.getEntry(ANNOTATION_INDEX_LOCATION);
        if (entry != null) {
          // System.out.println("- Plugin jar indexed " + jarFile);
          try (InputStream stream = jar.getInputStream(entry)) {
            IndexReader reader = new IndexReader(stream);
            index = reader.read();
            indexCache.put(jarFile, index);
          }
        }
      } catch (IOException e) {
        throw new HopFileException(
            MessageFormat.format("Error reading annotation index from file ''{0}''", jarFile), e);
      }

      // Cache annotation index of jars
      //
      indexCache.put(jarFile, index);
    }

    return index;
  }

  public void clear() {
    nativeFiles.clear();
    pluginFiles.clear();
    indexCache.clear();
    jarFiles.clear();
  }

  public Set<File> findJarFiles(final File folder) throws HopFileException {

    Set<File> files = jarFiles.get(folder);

    if (files == null) {

      try {
        // Find all the jar files in this folder...
        //
        files = findFiles(folder);

        // Cache list of jars in the folder
        //
        jarFiles.put(folder, files);

      } catch (Exception e) {
        throw new HopFileException("Unable to list jar files in plugin folder '" + folder + "'", e);
      }
    }

    return files;
  }

  /**
   * Find all the jar files in the folder and sub-folders exception for lib folder.
   * 
   * @param folder
   * 
   * @return the list of jar files
   */
  private  static Set<File> findFiles(final File folder ) {
    Set<File> files = new HashSet<>();
    File[] children = folder.listFiles();
    if (children!=null) {
      for ( File child : children ) {
        if ( child.isFile() ) {
          if ( child.getName().endsWith( ".jar" ) ) {
            files.add( child );
          }
        }
        else if ( child.isDirectory() ) {
          if ( !"lib".equals( child.getName() ) ) {
            files.addAll( findFiles( child ) );
          }
        }
      }
    }

    return files;
  }

}
