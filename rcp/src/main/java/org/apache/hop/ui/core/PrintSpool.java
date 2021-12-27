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
import org.eclipse.swt.graphics.*;
import org.eclipse.swt.printing.PrintDialog;
import org.eclipse.swt.printing.Printer;
import org.eclipse.swt.printing.PrinterData;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;

/** This class handles printing for Hop. */
public class PrintSpool {
  private PrinterData printerdata;
  private Printer printer;

  public PrintSpool() {
    printerdata = Printer.getDefaultPrinterData();
    if (printerdata != null) {
      // Fail silently instead of crashing.
      printer = new Printer(printerdata);
    }
  }

  public PrinterData getPrinterData() {
    return printerdata;
  }

  // Ask which printer to use...

  public Printer getPrinter(Shell sh) {
    PrintDialog pd = new PrintDialog(sh);
    printerdata = pd.open();
    if (printerdata != null) {
      if (printer != null) {
        printer.dispose();
      }
      printer = new Printer(printerdata);
    }

    return printer;
  }

  public void dispose() {
    if (printer != null) {
      printer.dispose();
    }
  }

  public int getDepth() {
    return printer.getDepth();
  }

  public PaletteData getPaletteData() {
    PaletteData palette;
    switch (getDepth()) {
      case 1:
        palette = new PaletteData(new RGB[] {new RGB(0, 0, 0), new RGB(255, 255, 255)});
        break;
      default:
        palette = new PaletteData(0, 0, 0);
        palette.isDirect = true;
        break;
    }

    return palette;
  }

  public void printImage(Shell sh, Image img) {
    if (printerdata != null) {
      Rectangle imgbounds = img.getBounds();
      Point max = new Point(imgbounds.width, imgbounds.height);

      // What's the printers DPI?
      Point dpiPrinter = printer.getDPI();

      // What's the screens DPI?
      Point dpiScreen = Display.getCurrent().getDPI();

      // Resize on printer: calculate factor:
      double factorx = (double) dpiPrinter.x / (double) dpiScreen.x;
      double factory = (double) dpiPrinter.y / (double) dpiScreen.y;

      // Get size of 1 page?
      Rectangle page = printer.getBounds();

      double marginLeft = 0.40; // 0,40 inch about 1cm
      double marginRight = 0.40;
      double marginTop = 0.40;
      double marginBottom = 0.40;

      EnterPrintDialog epd =
          new EnterPrintDialog(
              sh,
              1,
              1,
              100,
              factorx,
              factory,
              page,
              marginLeft,
              marginRight,
              marginTop,
              marginBottom,
              img);
      if (epd.open() == SWT.OK) {
        double pageLeft = epd.leftMargin * dpiPrinter.x;
        double pageRight = epd.rightMargin * dpiPrinter.x;
        double pageTop = epd.topMargin * dpiPrinter.y;
        double pageBottom = epd.bottomMargin * dpiPrinter.y;
        double pageSizex = page.width - pageLeft - pageRight;
        double pageSizey = page.height - pageTop - pageBottom;

        double sizeOnPaperx = max.x * factorx;
        double sizeOnPapery = max.y * factory;
        double actualSizex = sizeOnPaperx * epd.scale / 100;
        double actualSizey = sizeOnPapery * epd.scale / 100;

        // Create new print workflow.
        printer.startJob("HopGui : print workflow");

        // How much of the image do we print on each page: all or just a page worth of pixels?

        for (int c = 0; c < epd.nrcols; c++) {
          double leftToPrintX = actualSizex - pageSizex * c;
          double printx =
              (leftToPrintX > pageSizex) ? pageSizex : (leftToPrintX >= 0 ? leftToPrintX : 0);

          for (int r = 0; r < epd.nrrows; r++) {
            double leftToPrintY = actualSizey - pageSizey * r;
            double printy =
                (leftToPrintY > pageSizey) ? pageSizey : (leftToPrintY >= 0 ? leftToPrintY : 0);

            int startx = (int) (actualSizex - leftToPrintX);
            int starty = (int) (actualSizey - leftToPrintY);

            int fromx = (int) (startx / (factorx * epd.scale / 100));
            int fromy = (int) (starty / (factory * epd.scale / 100));
            int imx = (int) (max.x * printx / actualSizex) - 1;
            int imy = (int) (max.y * printy / actualSizey) - 1;

            printer.startPage();
            GC gcPrinter = new GC(printer);

            gcPrinter.drawImage(
                img,
                fromx,
                fromy,
                imx,
                imy,
                (int) pageLeft,
                (int) pageTop,
                (int) printx,
                (int) printy);

            System.out.println("img dept = " + img.getImageData().depth);
            System.out.println("prn dept = " + printer.getDepth());
            System.out.println(
                "img size = ("
                    + img.getBounds().x
                    + ","
                    + img.getBounds().y
                    + ") : ("
                    + img.getBounds().width
                    + ","
                    + img.getBounds().height
                    + ")");
            System.out.println(
                "fromx="
                    + fromx
                    + ", fromy="
                    + fromy
                    + ", imx="
                    + imx
                    + ", imy="
                    + imy
                    + ", pageLeft="
                    + (int) pageLeft
                    + ", page_top="
                    + (int) pageTop
                    + ", printx="
                    + (int) printx
                    + ", printy="
                    + (int) printy);

            printer.endPage();
            gcPrinter.dispose();
          }
        }

        printer.endJob();
        printer.dispose();
      }
      img.dispose();
    }
  }
}
