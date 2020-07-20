package org.apache.hop.ui.hopgui;

import org.apache.hop.base.AbstractMeta;
import org.eclipse.swt.widgets.Canvas;

public abstract class CanvasFacade {
  private final static CanvasFacade IMPL;
  static {
    IMPL = (CanvasFacade) ImplementationLoader.newInstance( CanvasFacade.class );
  }
  public static void setData( Canvas canvas, float magnification, AbstractMeta meta, Class type ) {
    IMPL.setDataInternal( canvas, magnification, meta, type );
  }
  abstract void setDataInternal( Canvas canvas, float magnification, AbstractMeta meta, Class type );
}
