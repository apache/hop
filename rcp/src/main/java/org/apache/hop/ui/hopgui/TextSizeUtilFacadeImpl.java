package org.apache.hop.ui.hopgui;

import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.widgets.Display;

public class TextSizeUtilFacadeImpl extends TextSizeUtilFacade {

  @Override
  Point textExtentInternal( Font font, String text, int wrapWidth ) {
    Image dummyImage = new Image( Display.getCurrent(), 50, 10 );
    GC dummyGC = new GC( dummyImage );
    Point point = dummyGC.textExtent( text, SWT.DRAW_TAB | SWT.DRAW_DELIMITER );
    dummyImage.dispose();
    dummyGC.dispose();
    return point;
  }

}
