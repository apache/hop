package org.apache.hop.ui.hopgui;

import org.eclipse.rap.rwt.internal.textsize.TextSizeUtil;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.Point;

public class TextSizeUtilFacadeImpl extends TextSizeUtilFacade {

  @Override
  Point textExtentInternal(Font font, String text, int wrapWidth) {
    return TextSizeUtil.textExtent( font, text, wrapWidth );
  }

}
