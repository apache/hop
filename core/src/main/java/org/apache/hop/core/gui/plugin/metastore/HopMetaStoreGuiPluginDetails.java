package org.apache.hop.core.gui.plugin.metastore;

import java.util.Objects;

public class HopMetaStoreGuiPluginDetails {
  private String name;
  private String description;
  private String iconImage;
  private ClassLoader classLoader;

  public HopMetaStoreGuiPluginDetails() {
  }

  public HopMetaStoreGuiPluginDetails( String name, String description, String iconImage, ClassLoader classLoader ) {
    this.name = name;
    this.description = description;
    this.iconImage = iconImage;
    this.classLoader = classLoader;
  }

  @Override public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    HopMetaStoreGuiPluginDetails that = (HopMetaStoreGuiPluginDetails) o;
    return Objects.equals( name, that.name );
  }

  @Override public int hashCode() {
    return Objects.hash( name );
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
   * Gets description
   *
   * @return value of description
   */
  public String getDescription() {
    return description;
  }

  /**
   * @param description The description to set
   */
  public void setDescription( String description ) {
    this.description = description;
  }

  /**
   * Gets iconImage
   *
   * @return value of iconImage
   */
  public String getIconImage() {
    return iconImage;
  }

  /**
   * @param iconImage The iconImage to set
   */
  public void setIconImage( String iconImage ) {
    this.iconImage = iconImage;
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
}
