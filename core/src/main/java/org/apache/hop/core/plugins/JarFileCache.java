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
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.JarFile;
import java.util.zip.ZipEntry;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopFileException;
import org.jboss.jandex.Index;
import org.jboss.jandex.IndexReader;

public class JarFileCache {

  public static final String ANNOTATION_INDEX_LOCATION = "META-INF/jandex.idx";

  private static JarFileCache instance;

  private final Map<File, Index> indexCache;
  private final Map<IPluginFolder, List<File>> folderFiles;
  private final List<File> nativeFiles;

  private JarFileCache() {
    nativeFiles = new ArrayList<>();
    folderFiles = new HashMap<>();
    indexCache = new HashMap<>();
  }

  public static JarFileCache getInstance() {
    if (instance == null) {
      instance = new JarFileCache();
    }
    return instance;
  }
  
  public List<File> getNativeJars() throws HopFileException {

      // Scan native jars only once
      //
      if (nativeFiles.isEmpty()) {
        try {
          Enumeration<URL> indexFiles  = getClass().getClassLoader().getResources(JarFileCache.ANNOTATION_INDEX_LOCATION);
          while (indexFiles .hasMoreElements()) {
            URL url = indexFiles .nextElement();          
            
            // Relative path
            String path = url.getFile();            

            File file = new File(StringUtils.substringBefore(path,  "!/"));            
            
            //System.out.println("- Native jar indexed " + file);
            nativeFiles.add(file);
            
            // Read index from resource
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


  public List<File> getJars(IPluginFolder pluginFolder) throws HopFileException {
    List<File> jarFiles = folderFiles.get(pluginFolder);

    if (jarFiles == null) {
      jarFiles = Arrays.asList(pluginFolder.findJarFiles());
      folderFiles.put(pluginFolder, jarFiles);
    }
    return jarFiles;
  }

  public Index getIndex(File jarFile) throws HopFileException {
    Index index = indexCache.get(jarFile);
    
    if (index == null) {


      try (JarFile jar = new JarFile(jarFile)) {
        ZipEntry entry = jar.getEntry(ANNOTATION_INDEX_LOCATION);
        if (entry != null) {
          //System.out.println("- Plugin jar indexed " + jarFile);
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

      indexCache.put(jarFile, index);
    }

    return index;
  }

  public void clear() {
    nativeFiles.clear();
    indexCache.clear();
    folderFiles.clear();
  }
}
