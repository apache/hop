package org.apache.hop.ui.core.gui;

import org.apache.hop.core.util.Utils;

/**
 * This keeps track of the currently active namespace for all the current.
 * It makes it easy to see which namespace is active.
 * A namespace is used for plugins like Environment to set the active environment.
 * The standard for HopGUI is hop-gui and for Translator is is hop-translator
 */
public class HopNamespace {

  private static HopNamespace instance;

  private String namespace;

  private HopNamespace() {
  }

  public static final HopNamespace getInstance() {
    if (instance==null) {
      instance = new HopNamespace();
    }
    return instance;
  }

  /**
   * Gets namespace
   *
   * @return value of namespace
   */
  public static final String getNamespace() {
    String namespace = getInstance().namespace;
    if ( Utils.isEmpty( namespace ) ) {
      throw new RuntimeException("Please set a namespace before using one");
    }
    return getInstance().namespace;
  }

  /**
   * @param namespace The namespace to set
   */
  public static final void setNamespace( String namespace ) {
    getInstance().namespace = namespace;
  }
}
