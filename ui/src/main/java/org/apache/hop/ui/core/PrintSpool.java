/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.ui.core;

import org.apache.hop.ui.core.dialog.EnterPrintDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.PaletteData;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.RGB;
import org.eclipse.swt.graphics.Rectangle;
//import org.eclipse.swt.printing.PrintDialog;
//import org.eclipse.swt.printing.Printer;
//import org.eclipse.swt.printing.PrinterData;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;

/**
 * This class handles printing for Hop.
 *
 * @author Matt
 * @since 28-03-2004
 */
public class PrintSpool {
//  private PrinterData printerdata;
//  private Printer printer;
  private PaletteData palette;

  public PrintSpool() {
//    printerdata = Printer.getDefaultPrinterData();
//    if ( printerdata != null ) {
//      // Fail silently instead of crashing.
//      printer = new Printer( printerdata );
//    }
  }

//  public PrinterData getPrinterData() {
//    return printerdata;
//  }

  // Ask which printer to use...

//  public Printer getPrinter( Shell sh ) {
//    PrintDialog pd = new PrintDialog( sh );
//    printerdata = pd.open();
//    if ( printerdata != null ) {
//      if ( printer != null ) {
//        printer.dispose();
//      }
//      printer = new Printer( printerdata );
//    }
//
//    return printer;
//  }

  public void dispose() {
//    if ( printer != null ) {
//      printer.dispose();
//    }
  }

  public int getDepth() {
//    return printer.getDepth();
    return 1;
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
  }

}
