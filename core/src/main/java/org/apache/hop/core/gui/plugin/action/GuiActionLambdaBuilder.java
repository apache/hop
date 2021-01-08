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

package org.apache.hop.core.gui.plugin.action;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.IGuiActionLambda;
import org.apache.hop.core.gui.plugin.IGuiRefresher;

import java.lang.reflect.Method;

public class GuiActionLambdaBuilder<T> {

  public GuiActionLambdaBuilder() {
  }


  /**
   * Create a copy of the given action and create an action lambda for it.
   *
   * @param guiAction
   * @param methodParameter
   * @return The action with the appropriate lambda capable of executing the provided method in the given parent object
   */
  public GuiAction createLambda( GuiAction guiAction, T methodParameter, IGuiRefresher refresher ) {
    if ( guiAction.getGuiPluginMethodName() == null ) {
      throw new RuntimeException( "We need a method to execute this action" );
    }
    // Create a copy to make sure we're not doing anything stupid
    //
    GuiAction action = new GuiAction( guiAction );

    try {
      ClassLoader classLoader;
      if (guiAction.getClassLoader()==null) {
        classLoader = getClass().getClassLoader();
      } else {
        classLoader = guiAction.getClassLoader();
      }

      // We use the GUI Class defined in the GUI Action...
      //
      Class<?> guiPluginClass;

      try {
        guiPluginClass = classLoader.loadClass( guiAction.getGuiPluginClassName() );
      } catch(Exception e) {
        throw new HopException( "Unable to find class '"+guiAction.getGuiPluginClassName()+"'", e );
      }

      Object guiPlugin;

      try {
        Method getInstanceMethod = guiPluginClass.getDeclaredMethod( "getInstance" );
        guiPlugin = getInstanceMethod.invoke( null, null );
      } catch(Exception nsme) {
        // On the rebound we'll try to simply construct a new instance...
        // This makes the plugins even simpler.
        //
        try {
          guiPlugin = guiPluginClass.newInstance();
        } catch(Exception e) {
          throw nsme;
        }
      }

      Method method = guiPluginClass.getMethod( action.getGuiPluginMethodName(), methodParameter.getClass() );
      if ( method == null ) {
        throw new RuntimeException( "Unable to find method " + action.getGuiPluginMethodName() + " with parameter " + methodParameter.getClass().getName() + " in class " + guiAction.getGuiPluginClassName() );
      }
      final Object finalGuiPlugin = guiPlugin;
      IGuiActionLambda<T> actionLambda = ( shiftClicked, controlClicked, objects ) -> {
        try {
          method.invoke( finalGuiPlugin, methodParameter );
          if ( refresher != null ) {
            refresher.updateGui();
          }
        } catch ( Exception e ) {
          throw new RuntimeException( "Error executing method : " + action.getGuiPluginMethodName() + " in class " + guiAction.getGuiPluginClassName(), e );
        }
      };
      action.setActionLambda( actionLambda );
      return action;
    } catch ( Exception e ) {
      throw new RuntimeException( "Error creating action function for action : " + toString()+". Probably you need to provide a static getInstance() method in class "+guiAction.getGuiPluginClassName()+" to allow the GuiPlugin to be found and then method "+guiAction.getGuiPluginMethodName()+" can be called.", e );
    }
  }
}
