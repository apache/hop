package org.apache.hop.ui.hopgui.perspective.dataorch;

import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.eclipse.swt.custom.CTabItem;

import java.util.Objects;

public class TabItemHandler {
  private CTabItem tabItem;
  private IHopFileTypeHandler typeHandler;

  public TabItemHandler() {
  }

  public TabItemHandler( CTabItem tabItem, IHopFileTypeHandler typeHandler ) {
    this.tabItem = tabItem;
    this.typeHandler = typeHandler;
  }

  @Override public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    TabItemHandler that = (TabItemHandler) o;
    return tabItem.equals( that.tabItem );
  }

  @Override public int hashCode() {
    return Objects.hash( tabItem );
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

  /**
   * Gets typeHandler
   *
   * @return value of typeHandler
   */
  public IHopFileTypeHandler getTypeHandler() {
    return typeHandler;
  }

  /**
   * @param typeHandler The typeHandler to set
   */
  public void setTypeHandler( IHopFileTypeHandler typeHandler ) {
    this.typeHandler = typeHandler;
  }
}
