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

package org.apache.hop.core.gui.plugin.key;

import java.lang.reflect.Method;
import java.util.Objects;

public class KeyboardShortcut {
  private boolean osx;
  private boolean alt;
  private boolean control;
  private boolean shift;
  private boolean command;
  private int keyCode;

  private String parentMethodName;

  public KeyboardShortcut() {
    keyCode = 0;
  }

  public KeyboardShortcut( GuiKeyboardShortcut shortcut, Method parentMethod ) {
    this.osx = false;
    this.alt = shortcut.alt();
    this.control = shortcut.control();
    this.shift = shortcut.shift();
    this.command = shortcut.command();
    this.keyCode = shortcut.key();
    this.parentMethodName = parentMethod.getName();
  }

  public KeyboardShortcut( GuiOsxKeyboardShortcut shortcut, Method parentMethod ) {
    this.osx = true;
    this.alt = shortcut.alt();
    this.control = shortcut.control();
    this.shift = shortcut.shift();
    this.command = shortcut.command();
    this.keyCode = shortcut.key();
    this.parentMethodName = parentMethod.getName();
  }

  @Override public String toString() {
    if ( keyCode == 0 ) {
      return parentMethodName.toString();
    }
    StringBuilder str = new StringBuilder();
    if ( control ) {
      str.append( "Ctrl+" );
    }
    if ( alt ) {
      str.append( "Alt+" );
    }
    if ( shift ) {
      str.append( "Shift+" );
    }
    if ( command ) {
      str.append( "Cmd+" );
    }
        
    // Character upper
    if ( keyCode >= 65 && keyCode <= 90 ) {
      str.append( (char) keyCode );
    }
    // Character lower
    else if ( keyCode >= 97 && keyCode <= 122 ) {
      str.append( Character.toUpperCase( (char) keyCode ) );
    }
    // Delete key
    else if ( keyCode == 127 ) {
      str.append( "Delete" );
    } 
    // Digit
    else if ( keyCode >= 48 && keyCode <= 57 ) {
      str.append( ( (char) keyCode ) );
    }
    return str.toString();
  }

  @Override public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    KeyboardShortcut that = (KeyboardShortcut) o;
    return alt == that.alt &&
      control == that.control &&
      shift == that.shift &&
      command == that.command &&
      keyCode == that.keyCode &&
      parentMethodName.equals( that.parentMethodName );
  }

  @Override public int hashCode() {
    return Objects.hash( alt, control, shift, command, keyCode, parentMethodName );
  }

  /**
   * Gets osx
   *
   * @return value of osx
   */
  public boolean isOsx() {
    return osx;
  }

  /**
   * @param osx The osx to set
   */
  public void setOsx( boolean osx ) {
    this.osx = osx;
  }

  /**
   * Gets alt
   *
   * @return value of alt
   */
  public boolean isAlt() {
    return alt;
  }

  /**
   * @param alt The alt to set
   */
  public void setAlt( boolean alt ) {
    this.alt = alt;
  }

  /**
   * Gets control
   *
   * @return value of control
   */
  public boolean isControl() {
    return control;
  }

  /**
   * @param control The control to set
   */
  public void setControl( boolean control ) {
    this.control = control;
  }

  /**
   * Gets shift
   *
   * @return value of shift
   */
  public boolean isShift() {
    return shift;
  }

  /**
   * @param shift The shift to set
   */
  public void setShift( boolean shift ) {
    this.shift = shift;
  }

  /**
   * Gets command
   *
   * @return value of command
   */
  public boolean isCommand() {
    return command;
  }

  /**
   * @param command The command to set
   */
  public void setCommand( boolean command ) {
    this.command = command;
  }

  /**
   * Gets keyCode
   *
   * @return value of keyCode
   */
  public int getKeyCode() {
    return keyCode;
  }

  /**
   * @param keyCode The keyCode to set
   */
  public void setKeyCode( int keyCode ) {
    this.keyCode = keyCode;
  }

  /**
   * Gets parentMethodName
   *
   * @return value of parentMethodName
   */
  public String getParentMethodName() {
    return parentMethodName;
  }

  /**
   * @param parentMethodName The parentMethodName to set
   */
  public void setParentMethodName( String parentMethodName ) {
    this.parentMethodName = parentMethodName;
  }

}
