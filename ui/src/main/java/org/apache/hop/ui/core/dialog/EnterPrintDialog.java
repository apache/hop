/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.core.dialog;

import org.apache.hop.core.Const;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

/**
 * A dialog that sets the printer settings for a Hop printout.
 *
 * @author Matt
 * @since 19-06-2003
 */
public class EnterPrintDialog extends Dialog {
  private static final Class<?> PKG = EnterPrintDialog.class; // For Translator

  private int retval;
  private Image image;

  private Canvas wCanvas;

  private Slider wCols;

  private Slider wRows;

  private Slider wScale;

  private Text wLeft;

  private Text wRight;

  private Text wTop;

  private Text wBottom;

  private Shell shell;
  private PropsUi props;

  public int nrcols, nrrows, scale;
  public Point page;
  public double factorx, factory;
  public double leftMargin, rightMargin, topMargin, bottomMargin;

  public EnterPrintDialog(
      Shell parent,
      int nrcols,
      int nrrows,
      int scale,
      double factorX,
      double factorY,
      Rectangle m,
      double marginLeft,
      double marginRigth,
      double marginTop,
      double marginBottom,
      Image image) {
    super(parent, SWT.NONE);
    props = PropsUi.getInstance();
    this.nrcols = nrcols;
    this.nrrows = nrrows;
    this.scale = scale;
    this.image = image;
    this.factorx = factorX;
    this.factory = factorY;
    this.leftMargin = marginLeft;
    this.rightMargin = marginRigth;
    this.topMargin = marginTop;
    this.bottomMargin = marginBottom;

    page = new Point(m.width, m.height);
  }

  public int open() {
    Shell parent = getParent();

    retval = SWT.OK;

    shell =
        new Shell(
            parent,
            SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.SHEET | SWT.RESIZE | SWT.MAX | SWT.MIN);
    props.setLook(shell);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "EnterPrintDialog.Title"));

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Canvas
    Label wlCanvas = new Label(shell, SWT.NONE);
    wlCanvas.setText(BaseMessages.getString(PKG, "EnterPrintDialog.PrintArea.Label"));
    props.setLook(wlCanvas);
    FormData fdlCanvas = new FormData();
    fdlCanvas.left = new FormAttachment(0, 0);
    fdlCanvas.top = new FormAttachment(0, margin);
    wlCanvas.setLayoutData(fdlCanvas);
    wCanvas = new Canvas(shell, SWT.BORDER);
    props.setLook(wCanvas);
    wCanvas.addPaintListener(pe -> repaint(pe.gc, pe.width, pe.height));
    FormData fdCanvas = new FormData();
    fdCanvas.left = new FormAttachment(0, 0);
    fdCanvas.top = new FormAttachment(wlCanvas, margin);
    fdCanvas.right = new FormAttachment(100, 0);
    fdCanvas.bottom = new FormAttachment(100, -220);
    wCanvas.setLayoutData(fdCanvas);

    // Rows
    Label wlRows = new Label(shell, SWT.NONE);
    wlRows.setText(BaseMessages.getString(PKG, "EnterPrintDialog.Rows.Label"));
    props.setLook(wlRows);
    FormData fdlRows = new FormData();
    fdlRows.left = new FormAttachment(0, 0);
    fdlRows.right = new FormAttachment(middle, -margin);
    fdlRows.top = new FormAttachment(wCanvas, margin);
    wlRows.setLayoutData(fdlRows);
    wRows = new Slider(shell, SWT.HORIZONTAL);
    wRows.setIncrement(1);
    wRows.setMinimum(1);
    wRows.setMaximum(11);
    wRows.setThumb(1);
    wRows.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent se) {
            Slider sl = (Slider) se.widget;
            nrrows = sl.getSelection();
            wCanvas.redraw();
          }
        });
    props.setLook(wRows);
    FormData fdRows = new FormData();
    fdRows.left = new FormAttachment(middle, 0);
    fdRows.top = new FormAttachment(wCanvas, margin);
    fdRows.right = new FormAttachment(100, 0);
    wRows.setLayoutData(fdRows);

    // Cols
    Label wlCols = new Label(shell, SWT.NONE);
    wlCols.setText(BaseMessages.getString(PKG, "EnterPrintDialog.Cols.Label"));
    props.setLook(wlCols);
    FormData fdlCols = new FormData();
    fdlCols.left = new FormAttachment(0, 0);
    fdlCols.right = new FormAttachment(middle, -margin);
    fdlCols.top = new FormAttachment(wRows, margin);
    wlCols.setLayoutData(fdlCols);
    wCols = new Slider(shell, SWT.HORIZONTAL);
    wCols.setIncrement(1);
    wCols.setMinimum(1);
    wCols.setMaximum(11);
    wCols.setThumb(1);
    wCols.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent se) {
            Slider sl = (Slider) se.widget;
            nrcols = sl.getSelection();
            wCanvas.redraw();
          }
        });
    props.setLook(wCols);
    FormData fdCols = new FormData();
    fdCols.left = new FormAttachment(middle, 0);
    fdCols.top = new FormAttachment(wRows, margin);
    fdCols.right = new FormAttachment(100, 0);
    wCols.setLayoutData(fdCols);

    // Scale
    Label wlScale = new Label(shell, SWT.NONE);
    wlScale.setText(BaseMessages.getString(PKG, "EnterPrintDialog.Scaling.Label"));
    props.setLook(wlScale);
    FormData fdlScale = new FormData();
    fdlScale.left = new FormAttachment(0, 0);
    fdlScale.right = new FormAttachment(middle, -margin);
    fdlScale.top = new FormAttachment(wCols, margin);
    wlScale.setLayoutData(fdlScale);
    wScale = new Slider(shell, SWT.HORIZONTAL);
    wScale.setIncrement(10);
    wScale.setMinimum(10);
    wScale.setMaximum(500);
    wScale.setThumb(10);
    wScale.setPageIncrement(25);
    wScale.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent se) {
            Slider sl = (Slider) se.widget;
            scale = sl.getSelection();
            wCanvas.redraw();
          }
        });
    props.setLook(wScale);
    FormData fdScale = new FormData();
    fdScale.left = new FormAttachment(middle, 0);
    fdScale.top = new FormAttachment(wCols, margin);
    fdScale.right = new FormAttachment(100, 0);
    wScale.setLayoutData(fdScale);

    // Left
    Label wlLeft = new Label(shell, SWT.NONE);
    wlLeft.setText(BaseMessages.getString(PKG, "EnterPrintDialog.LeftMargin.Label"));
    props.setLook(wlLeft);
    FormData fdlLeft = new FormData();
    fdlLeft.left = new FormAttachment(0, 0);
    fdlLeft.right = new FormAttachment(middle, -margin);
    fdlLeft.top = new FormAttachment(wScale, margin);
    wlLeft.setLayoutData(fdlLeft);
    wLeft = new Text(shell, SWT.BORDER);
    wLeft.addModifyListener(
        e -> {
          Text w = (Text) e.widget;
          leftMargin = Const.toDouble(w.getText(), 0.00);
        });
    props.setLook(wLeft);
    FormData fdLeft = new FormData();
    fdLeft.left = new FormAttachment(middle, 0);
    fdLeft.top = new FormAttachment(wScale, margin);
    fdLeft.right = new FormAttachment(100, 0);
    wLeft.setLayoutData(fdLeft);

    // Right
    Label wlRight = new Label(shell, SWT.NONE);
    wlRight.setText(BaseMessages.getString(PKG, "EnterPrintDialog.RightMargin.Label"));
    props.setLook(wlRight);
    FormData fdlRight = new FormData();
    fdlRight.left = new FormAttachment(0, 0);
    fdlRight.right = new FormAttachment(middle, -margin);
    fdlRight.top = new FormAttachment(wLeft, margin);
    wlRight.setLayoutData(fdlRight);
    wRight = new Text(shell, SWT.BORDER);
    wRight.addModifyListener(
        e -> {
          Text w = (Text) e.widget;
          rightMargin = Const.toDouble(w.getText(), 0.00);
        });
    props.setLook(wRight);
    FormData fdRight = new FormData();
    fdRight.left = new FormAttachment(middle, 0);
    fdRight.top = new FormAttachment(wLeft, margin);
    fdRight.right = new FormAttachment(100, 0);
    wRight.setLayoutData(fdRight);

    // Top
    Label wlTop = new Label(shell, SWT.NONE);
    wlTop.setText(BaseMessages.getString(PKG, "EnterPrintDialog.TopMargin.Label"));
    props.setLook(wlTop);
    FormData fdlTop = new FormData();
    fdlTop.left = new FormAttachment(0, 0);
    fdlTop.right = new FormAttachment(middle, -margin);
    fdlTop.top = new FormAttachment(wRight, margin);
    wlTop.setLayoutData(fdlTop);
    wTop = new Text(shell, SWT.BORDER);
    wTop.addModifyListener(
        e -> {
          Text w = (Text) e.widget;
          topMargin = Const.toDouble(w.getText(), 0.00);
        });
    props.setLook(wTop);
    FormData fdTop = new FormData();
    fdTop.left = new FormAttachment(middle, 0);
    fdTop.top = new FormAttachment(wRight, margin);
    fdTop.right = new FormAttachment(100, 0);
    wTop.setLayoutData(fdTop);

    // Bottom
    Label wlBottom = new Label(shell, SWT.NONE);
    wlBottom.setText(BaseMessages.getString(PKG, "EnterPrintDialog.BottomMargin.Label"));
    props.setLook(wlBottom);
    FormData fdlBottom = new FormData();
    fdlBottom.left = new FormAttachment(0, 0);
    fdlBottom.right = new FormAttachment(middle, -margin);
    fdlBottom.top = new FormAttachment(wTop, margin);
    wlBottom.setLayoutData(fdlBottom);
    wBottom = new Text(shell, SWT.BORDER);
    wBottom.addModifyListener(
        e -> {
          Text w = (Text) e.widget;
          bottomMargin = Const.toDouble(w.getText(), 0.00);
        });
    props.setLook(wBottom);
    FormData fdBottom = new FormData();
    fdBottom.left = new FormAttachment(middle, 0);
    fdBottom.top = new FormAttachment(wTop, margin);
    fdBottom.right = new FormAttachment(100, 0);
    wBottom.setLayoutData(fdBottom);

    // Some buttons
    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    FormData fdOk = new FormData();
    fdOk.left = new FormAttachment(33, 0);
    fdOk.bottom = new FormAttachment(100, 0);
    wOk.setLayoutData(fdOk);
    FormData fdCancel = new FormData();
    fdCancel.left = new FormAttachment(66, 0);
    fdCancel.bottom = new FormAttachment(100, 0);
    wCancel.setLayoutData(fdCancel);

    // Add listeners
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel.addListener(SWT.Selection, e -> cancel());

    getData();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return retval;
  }

  public void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }

  public void getData() {
    wCols.setSelection(nrcols);
    wRows.setSelection(nrrows);
    wScale.setSelection(scale);
    wLeft.setText(Double.toString(leftMargin));
    wRight.setText(Double.toString(rightMargin));
    wTop.setText(Double.toString(topMargin));
    wBottom.setText(Double.toString(bottomMargin));
  }

  private void cancel() {
    retval = SWT.CANCEL;
    dispose();
  }

  private void ok() {
    nrcols = wCols.getSelection();
    nrrows = wRows.getSelection();
    scale = wScale.getSelection();
    dispose();
  }

  private void repaint(GC gc, int width, int height) {
    ImageData imd = image.getImageData();

    double sizeOnPaperX = imd.width * factorx;
    double sizeOnPaperY = imd.height * factory;
    double actualSizeX = sizeOnPaperX * scale / 100;
    double actualSizeY = sizeOnPaperY * scale / 100;

    // What % of the screen is filled?
    // The canvas is nrcols * nrrows nr of pages large.
    double percentScreenX = actualSizeX / (page.x * nrcols);
    double percentScreenY = actualSizeY / (page.y * nrrows);

    gc.drawImage(
        image,
        0,
        0,
        imd.width,
        imd.height,
        0,
        0,
        (int) (width * percentScreenX),
        (int) (height * percentScreenY));

    StringBuilder text = new StringBuilder();
    text.append(nrcols).append("x").append(nrrows).append(" @ ").append(scale).append("%");
    gc.drawText(text.toString(), 0, 0);
    for (int c = 1; c < nrcols; c++) {
      gc.drawLine(c * (width / nrcols), 0, c * (width / nrcols), height);
    }

    for (int r = 1; r < nrrows; r++) {
      gc.drawLine(0, r * (height / nrrows), width, r * (height / nrrows));
    }
  }
}
