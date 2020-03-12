package org.apache.hop.core.gui.plugin;

import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class GuiAction {
  private String id;
  private GuiActionType type;
  private String name;
  private String tooltip;
  private String image;
  private IGuiActionLambda actionLambda;
  private String methodName;
  private ClassLoader classLoader;
  private List<String> keywords;

  public GuiAction() {
    this.keywords = new ArrayList<>();
  }

  /**
   * It's a direct action using a simple lambda
   *
   * @param id
   * @param type
   * @param name
   * @param tooltip
   * @param image
   * @param actionLambda
   */
  public GuiAction( String id, GuiActionType type, String name, String tooltip, String image, IGuiActionLambda actionLambda ) {
    this();
    this.id = id;
    this.type = type;
    this.name = name;
    this.tooltip = tooltip;
    this.image = image;
    this.actionLambda = actionLambda;
  }

  /**
   * We don't know the action to execute, just the method so we defer looking up the action until we have the parent object in hand.
   * We're just storing the method name in this case.
   * You can use method createLambda() to build it when you have the parent object and parameter to pass.
   *
   * @param id
   * @param type
   * @param name
   * @param tooltip
   * @param image
   * @param methodName
   */
  public GuiAction( String id, GuiActionType type, String name, String tooltip, String image, String methodName ) {
    this();
    this.id = id;
    this.type = type;
    this.name = name;
    this.tooltip = tooltip;
    this.image = image;
    this.methodName = methodName;
  }

  public GuiAction( GuiAction guiAction ) {
    this();
    this.id = guiAction.id;
    this.type = guiAction.type;
    this.name = guiAction.name;
    this.tooltip = guiAction.tooltip;
    this.image = guiAction.image;
    this.methodName = guiAction.methodName;
    this.actionLambda = guiAction.actionLambda;
    for ( String keyword : guiAction.getKeywords() ) {
      keywords.add( keyword );
    }
  }

  public boolean containsFilterStrings( String[] filters ) {
    for ( String filter : filters ) {
      if ( !containsFilterString( filter ) ) {
        return false;
      }
    }
    return true;
  }

  public boolean containsFilterString( String filter ) {
    if ( filter == null ) {
      return false;
    }
    if ( matchesString(name, filter) ) {
      return true;
    }
    if ( matchesString(tooltip, filter) ) {
      return true;
    }
    if ( type!=null && matchesString( type.name(), filter)) {
      return true;
    }
    for ( String keyword : keywords ) {
      if ( matchesString( keyword, filter )) {
        return true;
      }
    }
    return false;
  }

  private boolean matchesString(String string, String filter) {
    if ( StringUtils.isEmpty(string)) {
      return false;
    }
    // TODO: consider some fuzzy matching algorithm
    // TODO: Do a Levenshtein distance on the filter string across all valid string indexes 0..
    return string.toUpperCase().contains( filter.toUpperCase() );
  }

  @Override public String toString() {
    return "GuiAction{" +
      "id='" + id + '\'' +
      ", type=" + type +
      ", name='" + name + '\'' +
      ", tooltip='" + tooltip + '\'' +
      '}';
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
   * Gets type
   *
   * @return value of type
   */
  public GuiActionType getType() {
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
   * Gets tooltip
   *
   * @return value of tooltip
   */
  public String getTooltip() {
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
  public String getImage() {
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

  /**
   * Gets methodName
   *
   * @return value of methodName
   */
  public String getMethodName() {
    return methodName;
  }

  /**
   * @param methodName The methodName to set
   */
  public void setMethodName( String methodName ) {
    this.methodName = methodName;
  }

  /**
   * Gets classLoader
   *
   * @return value of classLoader
   */
  public ClassLoader getClassLoader() {
    return classLoader;
  }

  /**
   * @param classLoader The classLoader to set
   */
  public void setClassLoader( ClassLoader classLoader ) {
    this.classLoader = classLoader;
  }

  /**
   * Gets keywords
   *
   * @return value of keywords
   */
  public List<String> getKeywords() {
    return keywords;
  }

  /**
   * @param keywords The keywords to set
   */
  public void setKeywords( List<String> keywords ) {
    this.keywords = keywords;
  }
}
