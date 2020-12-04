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

package org.apache.hop.ui.core.metadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;

public class MetadataFileType implements IHopFileType {
  @Override public String getName() {
    return "meta";
  }
 
  @Override
  public boolean equals( Object obj ) {
    if ( obj == this ) {
      return true;
    }
    if ( obj.getClass().equals( this.getClass() ) ) {
      return true; // same class is enough
    }
    return false;
  }
  
  @Override public String getDefaultFileExtension() {
	return null;
  }

  @Override public String[] getFilterExtensions() {
    return new String[ 0 ];
  }

  @Override public String[] getFilterNames() {
    return new String[ 0 ];
  }
  
  @Override public Properties getCapabilities() {
	    Properties capabilities = new Properties();
	    capabilities.setProperty( IHopFileType.CAPABILITY_NEW, "true" );
	    capabilities.setProperty( IHopFileType.CAPABILITY_CLOSE, "true" );
	    capabilities.setProperty( IHopFileType.CAPABILITY_SAVE, "true" );

	    return capabilities;
  }

  @Override public boolean hasCapability( String capability ) {
	    if ( getCapabilities() == null ) {
	      return false;
	    }
	    Object available = getCapabilities().get( capability );
	    if (available==null) {
	      return false;
	    }
	    return "true".equalsIgnoreCase( available.toString() );
  }

  @Override public IHopFileTypeHandler openFile( HopGui hopGui, String filename, IVariables parentVariableSpace ) throws HopException {
    return new MetadataFileTypeHandler();
  }

  @Override public IHopFileTypeHandler newFile( HopGui hopGui, IVariables parentVariableSpace ) throws HopException {
    return new MetadataFileTypeHandler();
  }

  @Override public boolean isHandledBy( String filename, boolean checkContent ) throws HopException {
    return false;
  }

  @Override public boolean supportsFile( IHasFilename metaObject ) {
    return false;
  }

  @Override public List<IGuiContextHandler> getContextHandlers() {
    List<IGuiContextHandler> handlers = new ArrayList<>();
    return handlers;
  }
}
