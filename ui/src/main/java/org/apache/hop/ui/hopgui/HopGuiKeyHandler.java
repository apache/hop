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

package org.apache.hop.ui.hopgui;

import org.apache.hop.core.Const;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.key.KeyboardShortcut;
import org.apache.hop.core.logging.LogChannel;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class HopGuiKeyHandler extends KeyAdapter {

  private static HopGuiKeyHandler singleton;

  public Set<Object> parentObjects;

  private HopGuiKeyHandler() {
    this.parentObjects = new HashSet<>();
  }

  public static HopGuiKeyHandler getInstance() {
    if ( singleton == null ) {
      singleton = new HopGuiKeyHandler();
    }
    return singleton;
  }

  public void addParentObjectToHandle( Object parentObject ) {
    parentObjects.add( parentObject );
  }

  public void removeParentObjectToHandle( Object parentObject ) {
    parentObjects.remove( parentObject );
  }

  @Override public void keyPressed( KeyEvent e ) {
    // TODO: allow for keyboard shortcut priorities for certain objects.
    //
    for ( Object parentObject : parentObjects ) {
      List<KeyboardShortcut> shortcuts = GuiRegistry.getInstance().getKeyboardShortcuts( parentObject.getClass().getName() );
      if (shortcuts!=null) {
        for ( KeyboardShortcut shortcut : shortcuts ) {
          if ( handleKey( parentObject, e, shortcut ) ) {
            return; // This key is handled.
          }
        }
      }
    }
  }

  private boolean handleKey( Object parentObject, KeyEvent event, KeyboardShortcut shortcut ) {
    int keyCode = ( event.keyCode & SWT.KEY_MASK );

    boolean alt = ( event.stateMask & SWT.ALT ) != 0;
    boolean shift = ( event.stateMask & SWT.SHIFT ) != 0;
    boolean control = ( event.stateMask & SWT.CONTROL ) != 0;
    boolean command = ( event.stateMask & SWT.COMMAND ) != 0;

    boolean matchOS = Const.isOSX() == shortcut.isOsx();

    if ( keyCode == SWT.KEYPAD_ADD ) keyCode = '+';
    else if ( keyCode == SWT.KEYPAD_SUBTRACT ) keyCode = '-';
    else if ( keyCode == SWT.KEYPAD_MULTIPLY ) keyCode = '*';
    else if ( keyCode == SWT.KEYPAD_DIVIDE ) keyCode = '/';
    else if ( keyCode == SWT.KEYPAD_EQUAL ) keyCode = '=';

    boolean keyMatch = keyCode == shortcut.getKeyCode();
    boolean altMatch = shortcut.isAlt() == alt;
    boolean shiftMatch = shortcut.isShift() == shift;
    boolean controlMatch = shortcut.isControl() == control;
    boolean commandMatch = shortcut.isCommand() == command;

    if ( matchOS && keyMatch && altMatch && shiftMatch && controlMatch && commandMatch ) {
      // This is the key: call the method to which the original key shortcut annotation belongs
      //
      try {
        Class<?> parentClass = parentObject.getClass();
        Method method = parentClass.getMethod( shortcut.getParentMethodName() );
        if ( method != null ) {
          method.invoke( parentObject );
          return true; // Stop looking after 1 execution
        }
      } catch ( Exception ex ) {
        LogChannel.UI.logError( "Error calling keyboard shortcut method on parent object " + parentObject.toString(), ex );
      }
    }
    return false;
  }
}
