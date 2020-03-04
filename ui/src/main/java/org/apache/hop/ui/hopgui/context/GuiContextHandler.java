package org.apache.hop.ui.hopgui.context;

import org.apache.hop.core.gui.plugin.IGuiAction;
import org.eclipse.swt.widgets.Shell;

import java.util.ArrayList;
import java.util.List;

/**
 * This class handles actions for a certain context.
 * For example, the main HopGui dialog registers a bunch of context handlers for MetaStore objects, asks the various perspectives, ...
 */
public abstract class GuiContextHandler implements IGuiContextHandler {
  private String id;
  private String name;
  private List<Object> objects;

  public GuiContextHandler( String id, String name ) {
    this.id = id;
    this.name = name;
    this.objects = new ArrayList<>();
  }

  /**
   * Gets id
   *
   * @return value of id
   */
  public String getId() {
    return id;
  }

  /**
   * @param id The id to set
   */
  public void setId( String id ) {
    this.id = id;
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name The name to set
   */
  public void setName( String name ) {
    this.name = name;
  }

  /**
   * Gets objects
   *
   * @return value of objects
   */
  public List<Object> getObjects() {
    return objects;
  }

  /**
   * @param objects The objects to set
   */
  public void setObjects( List<Object> objects ) {
    this.objects = objects;
  }
}
