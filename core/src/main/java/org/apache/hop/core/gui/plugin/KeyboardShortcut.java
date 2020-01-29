package org.apache.hop.core.gui.plugin;

import java.lang.reflect.Method;
import java.util.Objects;

public class KeyboardShortcut {
  private boolean alt;
  private boolean control;
  private boolean shift;
  private int keyCode;

  private String parentMethodName;

  public KeyboardShortcut() {
  }

  public KeyboardShortcut( GuiKeyboardShortcut shortcut, Method parentMethod ) {
    this.alt = shortcut.alt();
    this.control = shortcut.control();
    this.shift = shortcut.shift();
    this.keyCode = shortcut.key();
    this.parentMethodName = parentMethod.getName();
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
      keyCode == that.keyCode;
  }

  @Override public int hashCode() {
    return Objects.hash( alt, control, shift, keyCode );
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
