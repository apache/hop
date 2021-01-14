/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.core;

import org.apache.hop.ui.core.dialog.EnterPrintDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.PaletteData;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.RGB;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.printing.PrintDialog;
import org.eclipse.swt.printing.Printer;
import org.eclipse.swt.printing.PrinterData;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;

/**
 * This class handles printing for Hop.
 *
 * @author Matt
 * @since 28-03-2004
 */
public class PrintSpool {
  private PrinterData printerdata;
  private Printer printer;
  private PaletteData palette;

  public PrintSpool() {
    printerdata = Printer.getDefaultPrinterData();
    if ( printerdata != null ) {
      // Fail silently instead of crashing.
      printer = new Printer( printerdata );
    }
  }

  public PrinterData getPrinterData() {
    return printerdata;
  }

  // Ask which printer to use...

  public Printer getPrinter( Shell sh ) {
    PrintDialog pd = new PrintDialog( sh );
    printerdata = pd.open();
    if ( printerdata != null ) {
      if ( printer != null ) {
        printer.dispose();
      }
      printer = new Printer( printerdata );
    }

    return printer;
  }

  public void dispose() {
    if ( printer != null ) {
      printer.dispose();
    }
  }

  public int getDepth() {
    return printer.getDepth();
  }

  public PaletteData getPaletteData() {
    switch ( getDepth() ) {
      case 1:
        palette = new PaletteData( new RGB[] { new RGB( 0, 0, 0 ), new RGB( 255, 255, 255 ) } );
        break;
      default:
        palette = new PaletteData( 0, 0, 0 );
        palette.isDirect = true;
        break;
    }

    return palette;
  }

  public void printImage( Shell sh, Image img ) {
    if ( printerdata != null ) {
      Rectangle imgbounds = img.getBounds();
      Point max = new Point( imgbounds.width, imgbounds.height );

      // What's the printers DPI?
      Point dpi_printer = printer.getDPI();

      // What's the screens DPI?
      Point dpi_screen = Display.getCurrent().getDPI();

      // Resize on printer: calculate factor:
      double factorx = (double) dpi_printer.x / (double) dpi_screen.x;
      double factory = (double) dpi_printer.y / (double) dpi_screen.y;

      // Get size of 1 page?
      Rectangle page = printer.getBounds();

      double margin_left = 0.40; // 0,40 inch about 1cm
      double margin_right = 0.40;
      double marginTop = 0.40;
      double margin_bottom = 0.40;

      EnterPrintDialog epd =
        new EnterPrintDialog(
          sh, 1, 1, 100, factorx, factory, page, margin_left, margin_right, marginTop, margin_bottom, img );
      if ( epd.open() == SWT.OK ) {
        double page_left = epd.leftMargin * dpi_printer.x;
        double page_right = epd.rightMargin * dpi_printer.x;
        double pageTop = epd.topMargin * dpi_printer.y;
        double page_bottom = epd.bottomMargin * dpi_printer.y;
        double page_sizex = page.width - page_left - page_right;
        double page_sizey = page.height - pageTop - page_bottom;

        double size_on_paperx = max.x * factorx;
        double size_on_papery = max.y * factory;
        double actual_sizex = size_on_paperx * epd.scale / 100;
        double actual_sizey = size_on_papery * epd.scale / 100;

        // Create new print workflow.
        printer.startJob( "HopGui : print workflow" );

        // How much of the image do we print on each page: all or just a page worth of pixels?

        for ( int c = 0; c < epd.nrcols; c++ ) {
          double leftToPrintX = actual_sizex - page_sizex * c;
          double printx =
            ( leftToPrintX > page_sizex ) ? page_sizex : ( leftToPrintX >= 0 ? leftToPrintX : 0 );

          for ( int r = 0; r < epd.nrrows; r++ ) {
            double leftToPrintY = actual_sizey - page_sizey * r;
            double printy =
              ( leftToPrintY > page_sizey ) ? page_sizey : ( leftToPrintY >= 0 ? leftToPrintY : 0 );

            int startx = (int) ( actual_sizex - leftToPrintX );
            int starty = (int) ( actual_sizey - leftToPrintY );

            int fromx = (int) ( startx / ( factorx * epd.scale / 100 ) );
            int fromy = (int) ( starty / ( factory * epd.scale / 100 ) );
            int imx = (int) ( max.x * printx / actual_sizex ) - 1;
            int imy = (int) ( max.y * printy / actual_sizey ) - 1;

            printer.startPage();
            GC gc_printer = new GC( printer );

            gc_printer.drawImage(
              img, fromx, fromy, imx, imy, (int) page_left, (int) pageTop, (int) printx, (int) printy );

            // ShowImageDialog sid = new ShowImageDialog(sh, props, img);
            // sid.open();

            System.out.println( "img dept = " + img.getImageData().depth );
            System.out.println( "prn dept = " + printer.getDepth() );
            System.out.println( "img size = ("
              + img.getBounds().x + "," + img.getBounds().y + ") : (" + img.getBounds().width + ","
              + img.getBounds().height + ")" );
            System.out.println( "fromx="
              + fromx + ", fromy=" + fromy + ", imx=" + imx + ", imy=" + imy + ", page_left=" + (int) page_left
              + ", page_top=" + (int) pageTop + ", printx=" + (int) printx + ", printy=" + (int) printy );

            printer.endPage();
            gc_printer.dispose();
          }
        }

        printer.endJob();
        printer.dispose();
      }
      img.dispose();
    }
  }

}
