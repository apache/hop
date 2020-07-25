package org.apache.hop.ui.hopgui;

import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.Point;

public abstract class TextSizeUtilFacade {
  private final static TextSizeUtilFacade IMPL;
  static {
    IMPL = (TextSizeUtilFacade) ImplementationLoader.newInstance( TextSizeUtilFacade.class );
  }

  public static Point textExtent( Font font, String text, int wrapWidth ) {
    return IMPL.textExtentInternal( font, text, wrapWidth );
  }
  abstract Point textExtentInternal( Font font, String text, int wrapWidth );
}
