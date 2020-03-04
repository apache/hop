package org.apache.hop.core.gui.plugin;

import java.util.Objects;

public class GuiAction implements IGuiAction {
  private String id;
  private GuiActionType type;
  private String name;
  private String tooltip;
  private String image;
  private IGuiActionLambda actionLambda;

  public GuiAction( String id, GuiActionType type, String name, String tooltip, String image, IGuiActionLambda actionLambda ) {
    this.id = id;
    this.type = type;
    this.name = name;
    this.tooltip = tooltip;
    this.image = image;
    this.actionLambda = actionLambda;
  }

  @Override public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    GuiAction guiAction = (GuiAction) o;
    return id.equals( guiAction.id );
  }

  @Override public int hashCode() {
    return Objects.hash( id );
  }

  /**
   * Gets id
   *
   * @return value of id
   */
  @Override public String getId() {
    return id;
  }

  /**
   * @param id The id to set
   */
  public void setId( String id ) {
    this.id = id;
  }

  /**
   * Gets type
   *
   * @return value of type
   */
  @Override public GuiActionType getType() {
    return type;
  }

  /**
   * @param type The type to set
   */
  public void setType( GuiActionType type ) {
    this.type = type;
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  @Override public String getName() {
    return name;
  }

  /**
   * @param name The name to set
   */
  public void setName( String name ) {
    this.name = name;
  }

  /**
   * Gets tooltip
   *
   * @return value of tooltip
   */
  @Override public String getTooltip() {
    return tooltip;
  }

  /**
   * @param tooltip The tooltip to set
   */
  public void setTooltip( String tooltip ) {
    this.tooltip = tooltip;
  }

  /**
   * Gets image
   *
   * @return value of image
   */
  @Override public String getImage() {
    return image;
  }

  /**
   * @param image The image to set
   */
  public void setImage( String image ) {
    this.image = image;
  }

  /**
   * Gets actionLambda
   *
   * @return value of actionLambda
   */
  public IGuiActionLambda getActionLambda() {
    return actionLambda;
  }

  /**
   * @param actionLambda The actionLambda to set
   */
  public void setActionLambda( IGuiActionLambda actionLambda ) {
    this.actionLambda = actionLambda;
  }
}
