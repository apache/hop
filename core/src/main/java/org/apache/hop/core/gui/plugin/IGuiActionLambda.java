package org.apache.hop.core.gui.plugin;

public interface IGuiActionLambda<T> {

  public void executeAction( boolean shiftAction, boolean controlAction, T... t );
}
