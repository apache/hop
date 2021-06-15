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

package org.apache.hop.ui.hopgui.perspective;

import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.empty.EmptyFileType;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This class helps the perspective plugins to keep track of their visualisation.
 * The main principle is that a perspective has it's own composite and draws on it.  It's shown or not depending on what is selected.
 */
public class HopPerspectiveManager {

  private HopGui hopGui;

  private Map<Class<? extends IHopPerspective>, IHopPerspective> perspectivesMap;

  private final ConcurrentLinkedQueue<IHopPerspectiveListener> listeners;
  
  public HopPerspectiveManager( HopGui hopGui ) {
    this.hopGui = hopGui;   
    this.perspectivesMap = new HashMap<>();
    this.listeners =  new ConcurrentLinkedQueue<>();
  }

  public void addPerspective( IHopPerspective perspective ) {
    perspectivesMap.put( perspective.getClass(), perspective );
  }

  public IHopPerspective getComposite( Class<? extends IHopPerspective> perspectiveClass ) {
    return perspectivesMap.get( perspectiveClass );
  }

  public void showPerspective( Class<? extends IHopPerspective> perspectiveClass ) {
    // Hide all perspectives but one...
    //
    for ( IHopPerspective perspective : perspectivesMap.values() ) {
      if ( perspective.getClass().equals( perspectiveClass ) ) {
        hopGui.setActivePerspective( perspective );
      }
    }
  }

  public IHopPerspective findPerspective( Class<? extends IHopPerspective> perspectiveClass ) {
    for ( IHopPerspective perspective : perspectivesMap.values() ) {        	
      if ( perspective.getClass().equals( perspectiveClass ) ) {
        return perspective;
      }
    }
    return null;
  }

  /**
   * Loop over all perspectives and see if any one of them recognises the object
   *
   * @param fileMetadata
   * @return
   */
  public IHopFileType findFileTypeHandler( IHasFilename fileMetadata ) {
    for ( IHopPerspective perspective : getPerspectives() ) {
      for ( IHopFileType fileType : perspective.getSupportedHopFileTypes() ) {
        if ( fileType.supportsFile( fileMetadata ) ) {
          return fileType;
        }
      }
    }
    return new EmptyFileType();
  }

  /**
   * Get a copy of all the handled/registered perspectives
   *
   * @return All perspectives copied over in a new list
   */
  public List<IHopPerspective> getPerspectives() {
    return new ArrayList<>( perspectivesMap.values() );
  }

  public void addPerspectiveListener(IHopPerspectiveListener listener) {	
	  if ( listener!=null ) {
		  listeners.add(listener);
	  }
  }
  
  public void removePerspectiveListener(IHopPerspectiveListener listener) {
	  if ( listener!=null ) {
		  listeners.remove(listener);
	  }
  }

  public void notifyPerspectiveActiviated(IHopPerspective perspective) {	  
	  for (IHopPerspectiveListener listener: this.listeners) {
		  listener.perspectiveActivated(perspective);
	  }
  }

  private List<Class<? extends IHopPerspective>> getSortedClasses() {

    List<Class<? extends IHopPerspective>> list = new ArrayList<>(perspectivesMap.keySet());
    Collections.sort( list, Comparator.comparing( c -> c.getAnnotation( HopPerspectivePlugin.class ).id() ) );
    return list;
  }

  public void showPreviousPerspective( IHopPerspective currentPerspective ) {
    if (currentPerspective==null) {
      return;
    }
    List<Class<? extends IHopPerspective>> list = getSortedClasses();
    int index = list.indexOf( currentPerspective.getClass() );
    if (index>0) {
      Class<? extends IHopPerspective> previousClass = list.get(index-1);
      IHopPerspective previousPerspective = perspectivesMap.get(previousClass);
      if (previousPerspective != null) {
        previousPerspective.activate();
      }
    }
  }

  public void showNextPerspective( IHopPerspective currentPerspective ) {
    if (currentPerspective==null) {
      return;
    }
    List<Class<? extends IHopPerspective>> list = getSortedClasses();
    int index = list.indexOf( currentPerspective.getClass() );
    if (index<list.size()-1) {
      Class<? extends IHopPerspective> nextClass = list.get(index+1);
      IHopPerspective nextPerspective = perspectivesMap.get(nextClass);
      if (nextPerspective != null) {
        nextPerspective.activate();
      }
    }
  }
}
