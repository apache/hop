package org.apache.hop.ui.hopgui.perspective.dataorch;

import org.eclipse.swt.custom.CTabFolder;

import java.util.List;

public class TabFileManager {
  private CTabFolder tabFolder;
  private List<TabFileItem> tabFileItems;

  public TabFileManager( CTabFolder tabFolder ) {
    this.tabFolder = tabFolder;
  }

  public TabFileItem findTabWithFilename(String filename) {
    if (filename==null) {
      return null;
    }
    for (TabFileItem tabFileItem : tabFileItems) {
      if (tabFileItem.getFilename()!=null && tabFileItem.getFilename().equals( filename )) {
        return tabFileItem;
      }
    }
    return null;
  }

  public TabFileItem findTabWithUUID(String uuid) {
    if (uuid==null) {
      return null;
    }
    for (TabFileItem tabFileItem : tabFileItems) {
      if (tabFileItem.getUuid().equals( uuid )) {
        return tabFileItem;
      }
    }
    return null;
  }

  public void addTab(String filename) {
    //
  }

  /**
   * Gets tabFolder
   *
   * @return value of tabFolder
   */
  public CTabFolder getTabFolder() {
    return tabFolder;
  }

  /**
   * @param tabFolder The tabFolder to set
   */
  public void setTabFolder( CTabFolder tabFolder ) {
    this.tabFolder = tabFolder;
  }

  /**
   * Gets tabFileItems
   *
   * @return value of tabFileItems
   */
  public List<TabFileItem> getTabFileItems() {
    return tabFileItems;
  }

  /**
   * @param tabFileItems The tabFileItems to set
   */
  public void setTabFileItems( List<TabFileItem> tabFileItems ) {
    this.tabFileItems = tabFileItems;
  }
}
