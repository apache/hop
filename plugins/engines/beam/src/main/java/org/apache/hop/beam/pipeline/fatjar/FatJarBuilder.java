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

package org.apache.hop.beam.pipeline.fatjar;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.beam.pipeline.HopPipelineMetaToBeamPipelineConverter;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.JarCache;
import org.apache.hop.core.variables.IVariables;
import org.jboss.jandex.IndexWriter;
import org.jboss.jandex.Indexer;

public class FatJarBuilder {

  private IVariables variables;
  private String targetJarFile;
  private List<String> jarFiles;
  private String extraTransformPluginClasses;
  private String extraXpPluginClasses;

  private transient Map<String, String> fileContentMap;

  public FatJarBuilder() {
    jarFiles = new ArrayList<>();
    extraTransformPluginClasses = null;
    extraXpPluginClasses = null;
  }

  public FatJarBuilder( IVariables variables, String targetJarFile, List<String> jarFiles ) {
    this();
    this.variables = variables;
    this.targetJarFile = targetJarFile;
    this.jarFiles = jarFiles;
  }

  public void buildTargetJar() throws HopException {

    fileContentMap = new HashMap<>();

    // The real target file to write to...
    //
    String realTargetJarFile = variables.resolve(targetJarFile);

    JarCache cache = JarCache.getInstance();
    Indexer indexer = new Indexer();
    
    try {
      byte[] buffer = new byte[ 1024 ];
      ZipOutputStream zipOutputStream = new ZipOutputStream( new FileOutputStream( realTargetJarFile ) );

      for ( String jarFile : jarFiles ) {
	
	// Index only already indexed jar
	boolean jarIndexed = false;	
 	if ( cache.getIndex(new File(jarFile))!=null ) { 	   
 	    jarIndexed = true;
 	}
	
        ZipInputStream zipInputStream = new ZipInputStream( new FileInputStream( jarFile ) );        
        ZipEntry zipEntry = zipInputStream.getNextEntry();
        while ( zipEntry != null ) {

          boolean skip = false;
          boolean merge = false;
          boolean index = false;

          String entryName = zipEntry.getName();

          if ( zipEntry.isDirectory() ) {
              skip = true;
          }
          if ( entryName.contains( "META-INF/INDEX.LIST" ) ) {
            skip = true;
          }
          else if ( entryName.contains( "META-INF/MANIFEST.MF" ) ) {
            skip = true;
          }
          else if ( entryName.startsWith( "META-INF" ) && entryName.endsWith( ".SF" ) ) {
            skip = true;
          }
          else if ( entryName.startsWith( "META-INF" ) && entryName.endsWith( ".DSA" ) ) {
            skip = true;
          }
          else if ( entryName.startsWith( "META-INF" ) && entryName.endsWith( ".RSA" ) ) {
            skip = true;
          }
          else if ( entryName.startsWith( "META-INF/services/" ) ) {
            merge = true;
            skip = true;
          }          
          // Skip because we rebuild a new one from cache
          else if ( entryName.endsWith( "META-INF/jandex.idx" ) ) {
            skip = true;
          }        
          else if (jarIndexed && entryName.endsWith(".class")) {
            index = true;
          }
          
          if ( !skip ) {
            try {
              zipOutputStream.putNextEntry( new ZipEntry( zipEntry.getName() ) );
            } catch ( ZipException ze ) {
              // Duplicate entry!
              //
              skip = true;
            }
          }

          if ( merge ) {
            String fileContent = IOUtils.toString( zipInputStream, "UTF-8" );
            String previousContent = fileContentMap.get( entryName );
            if ( previousContent == null ) {
              fileContentMap.put( entryName, fileContent );
            } else {
              fileContentMap.put( entryName, previousContent + Const.CR + fileContent );
            }
          } 
          else if ( !skip ) {
            int len;
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            while ( ( len = zipInputStream.read( buffer ) ) > 0 ) {
              zipOutputStream.write( buffer, 0, len );
              if (index) {
        	  baos.write(buffer, 0, len);
              }
            }

            if (index) {
                indexer.index(new ByteArrayInputStream(baos.toByteArray()));
            }
          }

          zipInputStream.closeEntry();

          if ( !skip ) {
            zipOutputStream.closeEntry();
          }

          zipEntry = zipInputStream.getNextEntry();

        }
                
        zipInputStream.close();
      }

      // Add the META-INF/services files...
      //
      for ( String entryName : fileContentMap.keySet() ) {
        // System.out.println( "Entry merged: " + entryName );
        String fileContent = fileContentMap.get( entryName );
        zipOutputStream.putNextEntry( new ZipEntry( entryName ) );
        zipOutputStream.write( fileContent.getBytes( "UTF-8" ) );
        zipOutputStream.closeEntry();
      }

      // Add META-INF/jandex.idx file
      //
      zipOutputStream.putNextEntry( new ZipEntry( JarCache.ANNOTATION_INDEX_LOCATION ) );
      IndexWriter indexWriter = new IndexWriter(zipOutputStream); 
      indexWriter.write(indexer.complete());      
      zipOutputStream.closeEntry();
  
      // Close FatJar
      zipOutputStream.close();
    } catch ( Exception e ) {
      throw new HopException( "Unable to build far jar file '" + realTargetJarFile + "'", e );
    } finally {
      fileContentMap.clear();
    }

  }

  public static String findPluginClasses( String pluginClassName, String pluginsToInclude ) throws HopException {
    String plugins = pluginsToInclude;

    if ( StringUtils.isEmpty( plugins ) ) {
      plugins = "hop-beam";
    } else {
      plugins += ",hop-beam";
    }

    Set<String> classes = new HashSet<>();
    String[] pluginFolders = plugins.split( "," );
    for ( String pluginFolder : pluginFolders ) {
      try {
        List<String> transformClasses = HopPipelineMetaToBeamPipelineConverter.findAnnotatedClasses( pluginFolder, pluginClassName );
        for ( String transformClass : transformClasses ) {
          classes.add( transformClass );
        }
      } catch ( Exception e ) {
        throw new HopException( "Error find plugin classes of annotation type '" + pluginClassName + "' in folder '" + pluginFolder, e );
      }
    }

    // OK, we now have all the classes...
    // Let's sort by name and add them in the dialog comma separated...
    //
    List<String> classesList = new ArrayList<>();
    classesList.addAll( classes );
    Collections.sort( classesList );

    StringBuffer all = new StringBuffer();
    for ( String pluginClass : classesList ) {
      if ( all.length() > 0 ) {
        all.append( "," );
      }
      all.append( pluginClass );
    }

    return all.toString();
  }


  /**
   * Gets targetJarFile
   *
   * @return value of targetJarFile
   */
  public String getTargetJarFile() {
    return targetJarFile;
  }

  /**
   * @param targetJarFile The targetJarFile to set
   */
  public void setTargetJarFile( String targetJarFile ) {
    this.targetJarFile = targetJarFile;
  }

  /**
   * Gets jarFiles
   *
   * @return value of jarFiles
   */
  public List<String> getJarFiles() {
    return jarFiles;
  }

  /**
   * @param jarFiles The jarFiles to set
   */
  public void setJarFiles( List<String> jarFiles ) {
    this.jarFiles = jarFiles;
  }

  /**
   * Gets extraTransformPluginClasses
   *
   * @return value of extraTransformPluginClasses
   */
  public String getExtraTransformPluginClasses() {
    return extraTransformPluginClasses;
  }

  /**
   * @param extraTransformPluginClasses The extraTransformPluginClasses to set
   */
  public void setExtraTransformPluginClasses( String extraTransformPluginClasses ) {
    this.extraTransformPluginClasses = extraTransformPluginClasses;
  }

  /**
   * Gets extraXpPluginClasses
   *
   * @return value of extraXpPluginClasses
   */
  public String getExtraXpPluginClasses() {
    return extraXpPluginClasses;
  }

  /**
   * @param extraXpPluginClasses The extraXpPluginClasses to set
   */
  public void setExtraXpPluginClasses( String extraXpPluginClasses ) {
    this.extraXpPluginClasses = extraXpPluginClasses;
  }

  /**
   * Gets fileContentMap
   *
   * @return value of fileContentMap
   */
  public Map<String, String> getFileContentMap() {
    return fileContentMap;
  }

  /**
   * @param fileContentMap The fileContentMap to set
   */
  public void setFileContentMap( Map<String, String> fileContentMap ) {
    this.fileContentMap = fileContentMap;
  }
}
