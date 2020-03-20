package org.apache.hop.core.action;

public enum GuiActions {
  NEW, EDIT, DELETE,
  ;

  public String id() {
    return name();
  }
}
