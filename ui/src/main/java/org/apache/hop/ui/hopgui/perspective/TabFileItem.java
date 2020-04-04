package org.apache.hop.ui.hopgui.perspective;

import org.apache.hop.core.util.UUIDUtil;
import org.eclipse.swt.custom.CTabItem;

import java.util.Objects;

public class TabFileItem {
  private String uuid;
  private String filename;
  private CTabItem tabItem;

  public TabFileItem() {
    uuid = UUIDUtil.getUUIDAsString();
  }

  public TabFileItem( String filename, CTabItem tabItem ) {
    this();
    this.filename = filename;
    this.tabItem = tabItem;
  }

  @Override public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    TabFileItem that = (TabFileItem) o;
    return uuid.equals( that.uuid );
  }

  @Override public int hashCode() {
    return Objects.hash( uuid );
  }

  /**
   * Gets uuid
   *
   * @return value of uuid
   */
  public String getUuid() {
    return uuid;
  }

  /**
   * @param uuid The uuid to set
   */
  public void setUuid( String uuid ) {
    this.uuid = uuid;
  }

  /**
   * Gets filename
   *
   * @return value of filename
   */
  public String getFilename() {
    return filename;
  }

  /**
   * @param filename The filename to set
   */
  public void setFilename( String filename ) {
    this.filename = filename;
  }

  /**
   * Gets tabItem
   *
   * @return value of tabItem
   */
  public CTabItem getTabItem() {
    return tabItem;
  }

  /**
   * @param tabItem The tabItem to set
   */
  public void setTabItem( CTabItem tabItem ) {
    this.tabItem = tabItem;
  }
}
