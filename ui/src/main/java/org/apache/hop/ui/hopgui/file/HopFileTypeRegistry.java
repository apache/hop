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

package org.apache.hop.ui.hopgui.file;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This class contains all the available Hop File types
 */
public class HopFileTypeRegistry {
  private static HopFileTypeRegistry hopFileTypeRegistry;

  private List<IHopFileType> hopFileTypes;

  private HopFileTypeRegistry() {
    hopFileTypes = new ArrayList<>();
  }

  public static final HopFileTypeRegistry getInstance() {
    if ( hopFileTypeRegistry == null ) {
      hopFileTypeRegistry = new HopFileTypeRegistry();
    }
    return hopFileTypeRegistry;
  }

  public List<IHopFileType> getFileTypes() {
    return hopFileTypes;
  }

  public void registerHopFile( IHopFileType hopFileTypeInterface ) {
    if ( !hopFileTypes.contains( hopFileTypeInterface ) ) {
      hopFileTypes.add( hopFileTypeInterface );
    }
  }

  /**
   * This method first tries to find a HopFile by looking at the extension.
   * If none can be found the content is looked at by each IHopFileType
   *
   * @param filename The filename to search with
   * @return The IHopFileType we can use to open the file itself.
   * @throws HopException
   */
  public IHopFileType findHopFileType( String filename ) throws HopException {
    for ( IHopFileType hopFile : hopFileTypes ) {
      if ( hopFile.isHandledBy( filename, false ) ) {
        return hopFile;
      }
    }
    for ( IHopFileType hopFile : hopFileTypes ) {
      if ( hopFile.isHandledBy( filename, true ) ) {
        return hopFile;
      }
    }
    return null;
  }

  /**
   * Get All the filter extensions of all the HopFile plugins
   *
   * @return all the file extensions
   */
  public String[] getFilterExtensions() {
    List<String> filterExtensions = new ArrayList<>();
    for ( IHopFileType hopFile : hopFileTypes ) {
      filterExtensions.addAll( Arrays.asList( hopFile.getFilterExtensions() ) );
    }
    if ( filterExtensions.size() > 1 ) {
      String all = "";
      for ( String filterExtension : filterExtensions ) {
        if ( all.length() > 0 ) {
          all += ";";
        }
        all += filterExtension;
      }
      filterExtensions.add( 0, all );
    }
    return filterExtensions.toArray( new String[ 0 ] );
  }

  /**
   * Get All the filter names of all the HopFile plugins
   *
   * @return all the file names
   */
  public String[] getFilterNames() {
    List<String> filterNames = new ArrayList<>();
    for ( IHopFileType hopFile : hopFileTypes ) {
      filterNames.addAll( Arrays.asList( hopFile.getFilterNames() ) );
    }
    if ( filterNames.size() > 1 ) {
      // Add an entry for all the types
      //
      //TODO: Add Translation
      filterNames.add( 0, "All Hop file types" );
    }
    return filterNames.toArray( new String[ 0 ] );
  }

  public List<String> getFileTypeNames() {
    List<String> names = new ArrayList<>();
    for ( IHopFileType fileType : hopFileTypes ) {
      names.add( fileType.getName() );
    }
    return names;
  }

  public IHopFileType getFileTypeByName( String name ) {
    if ( StringUtils.isEmpty( name ) ) {
      return null;
    }
    for ( IHopFileType fileType : hopFileTypes ) {
      if ( fileType.getName().equalsIgnoreCase( name ) ) {
        return fileType;
      }
    }
    return null;
  }
}
