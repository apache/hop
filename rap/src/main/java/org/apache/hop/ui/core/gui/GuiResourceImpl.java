package org.apache.hop.ui.core.gui;

import org.apache.hop.ui.hopgui.ISingletonProvider;
import org.eclipse.rap.rwt.SingletonUtil;

public class GuiResourceImpl implements ISingletonProvider {
  public Object getInstanceInternal() {
    return SingletonUtil.getSessionInstance( GuiResource.class );
  }
}
