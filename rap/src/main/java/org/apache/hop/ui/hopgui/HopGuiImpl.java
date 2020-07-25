package org.apache.hop.ui.hopgui;

import org.eclipse.rap.rwt.SingletonUtil;

public class HopGuiImpl implements ISingletonProvider {
  public Object getInstanceInternal() {
    return SingletonUtil.getSessionInstance( HopGui.class );
  }
}
