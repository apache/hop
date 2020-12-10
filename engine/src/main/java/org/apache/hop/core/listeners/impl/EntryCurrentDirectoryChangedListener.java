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

package org.apache.hop.core.listeners.impl;

import com.google.common.base.Objects;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.listeners.ICurrentDirectoryChangedListener;
import org.apache.hop.core.variables.Variables;

import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Updates directory references referencing {@link Const#INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER}
 */
public class EntryCurrentDirectoryChangedListener implements ICurrentDirectoryChangedListener {

  public interface IPathReference {
    String getPath();

    void setPath( String path );
  }

  private IPathReference[] references;

  public EntryCurrentDirectoryChangedListener( IPathReference... refs ) {
    references = refs;
  }

  public EntryCurrentDirectoryChangedListener(
    Supplier<String> pathGetter,
    Consumer<String> pathSetter ) {
    this( new IPathReference() {

      @Override
      public String getPath() {
        return pathGetter.get();
      }

      @Override
      public void setPath( String path ) {
        pathSetter.accept( path );
      }
    } );
  }

  @Override
  public void directoryChanged( Object origin, String oldCurrentDir, String newCurrentDir ) {
    for ( IPathReference ref : references ) {
      String path = ref.getPath();
      if ( StringUtils.contains( path, Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER )
        && !Objects.equal( oldCurrentDir, newCurrentDir ) ) {
        path = reapplyCurrentDir( oldCurrentDir, newCurrentDir, path );
        ref.setPath( path );
      }
    }
  }

  private String reapplyCurrentDir( String oldCurrentDir, String newCurrentDir, String path ) {
    Variables vars = new Variables();
    vars.setVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER, oldCurrentDir );
    String newPath = vars.resolve( path );
    return getPath( newCurrentDir, newPath );
  }

  private static String getPath( String parentPath, String path ) {
    if ( !parentPath.equals( "/" ) && path.startsWith( parentPath ) ) {
      path = path.replace( parentPath, "${" + Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER + "}" );
    }
    return path;
  }

}
