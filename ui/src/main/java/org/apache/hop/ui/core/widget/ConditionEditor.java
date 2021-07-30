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

package org.apache.hop.ui.core.widget;

import org.apache.hop.core.Condition;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.ValueMetaAndData;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.EnterValueDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.*;
import org.eclipse.swt.widgets.*;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.util.ArrayList;

/** Widget that allows you to edit a Condition in a graphical way. */
public class ConditionEditor extends Canvas implements MouseMoveListener {
  private static final Class<?> PKG = ConditionEditor.class; // For Translator

  private static final int X_PADDING = 18;
  private static final String STRING_NOT = BaseMessages.getString(PKG, "ConditionEditor.StringNot");
  private static final String STRING_UP = BaseMessages.getString(PKG, "ConditionEditor.StringUp");

  private static final int AREA_NONE = 0;
  private static final int AREA_BACKGROUND = 1;
  private static final int AREA_NOT = 2;
  private static final int AREA_CONDITION = 3;
  private static final int AREA_SUBCONDITION = 4;
  private static final int AREA_OPERATOR = 5;
  private static final int AREA_UP = 6;
  private static final int AREA_LEFT = 7;
  private static final int AREA_FUNCTION = 8;
  private static final int AREA_RIGHT_VALUE = 9;
  private static final int AREA_RIGHT_EXACT = 10;
  private static final int AREA_ICON_ADD = 11;

  protected Canvas widget;
  private Shell shell;
  private Display display;
  private Condition activeCondition;
  private Color bg;
  private Color white;
  private Color black;
  private Color red;
  private Color green;
  private Color blue;
  private Color gray;
  private Font fixed;

  private Image imageAdd;

  private Rectangle sizeNot;
  private Rectangle sizeWidget;
  private Rectangle sizeAndNot;
  private Rectangle sizeUp;
  private Rectangle sizeLeft;
  private Rectangle sizeFn;
  private Rectangle sizeRightval;
  private Rectangle sizeRightex;
  private Rectangle[] sizeCond;
  private Rectangle[] sizeOper;
  private Rectangle sizeAdd;
  private Rectangle maxdrawn;

  private int hoverCondition;
  private int hoverOperator;
  private boolean hoverNot;
  private boolean hoverUp;
  private boolean hoverLeft;
  private boolean hoverFn;
  private boolean hoverRightval;
  private boolean hoverRightex;

  private int previousArea;
  private int previousAreaNr;

  private ArrayList<Condition> parents;
  private IRowMeta fields;

  private int maxFieldLength;

  private ScrollBar sbVertical;
  private ScrollBar sbHorizontal;
  private int offsetx;
  private int offsety;

  private ArrayList<ModifyListener> modListeners;

  private String messageString;
  private Menu mPop;

  public ConditionEditor(Composite composite, int arg1, Condition co, IRowMeta inputFields) {
    super(composite, arg1 | SWT.NO_BACKGROUND | SWT.V_SCROLL | SWT.H_SCROLL);

    widget = this;

    this.activeCondition = co;
    this.fields = inputFields;

    imageAdd = GuiResource.getInstance().getImage("ui/images/add.svg");

    modListeners = new ArrayList<>();

    sbVertical = getVerticalBar();
    sbHorizontal = getHorizontalBar();
    offsetx = 0;
    offsety = 0;
    maxdrawn = null;

    sizeNot = null;
    sizeWidget = null;
    sizeCond = null;

    previousArea = -1;
    previousAreaNr = -1;

    parents = new ArrayList<>(); // Remember parent in drill-down...

    hoverCondition = -1;
    hoverOperator = -1;
    hoverNot = false;
    hoverUp = false;
    hoverLeft = false;
    hoverFn = false;
    hoverRightval = false;
    hoverRightex = false;

    /*
     * Determine the maximum field length...
     */
    getMaxFieldLength();

    shell = composite.getShell();
    display = shell.getDisplay();

    bg = GuiResource.getInstance().getColorBackground();
    fixed = GuiResource.getInstance().getFontFixed();

    white = GuiResource.getInstance().getColorWhite();
    black = GuiResource.getInstance().getColorBlack();
    red = GuiResource.getInstance().getColorRed();
    green = GuiResource.getInstance().getColorGreen();
    blue = GuiResource.getInstance().getColorBlue();
    gray = GuiResource.getInstance().getColorDarkGray();

    widget.addPaintListener(
        pe -> {
          Rectangle r = widget.getBounds();
          if (r.width > 0 && r.height > 0) {
            repaint(pe.gc, r.width, r.height);
          }
        });

    if (!EnvironmentUtils.getInstance().isWeb()) {
      widget.addMouseMoveListener(this);
    }

    widget.addMouseListener(
        new MouseAdapter() {
          @Override
          public void mouseDown(MouseEvent e) {
            Point screen = new Point(e.x, e.y);
            int area = getAreaCode(screen);

            if (e.button == 1) { // Left click on widget...

              switch (area) {
                case AREA_NOT:
                  activeCondition.negate();
                  setModified();
                  widget.redraw();
                  break;
                case AREA_OPERATOR:
                  int operator = getNrOperator(screen);
                  EnterSelectionDialog esd =
                      new EnterSelectionDialog(
                          shell,
                          Condition.getRealOperators(),
                          BaseMessages.getString(PKG, "ConditionEditor.Operator.Label"),
                          BaseMessages.getString(PKG, "ConditionEditor.SelectOperator.Label"));
                  esd.setAvoidQuickSearch();
                  Condition selcond = activeCondition.getCondition(operator);
                  String def = selcond.getOperatorDesc();
                  int defnr = esd.getSelectionNr(Const.trim(def));
                  String selection = esd.open(defnr);
                  if (selection != null) {
                    int opnr = Condition.getOperator(selection);
                    activeCondition.getCondition(operator).setOperator(opnr);
                    setModified();
                  }
                  widget.redraw();
                  break;
                case AREA_SUBCONDITION:
                  int nr = getNrSubcondition(screen);
                  editCondition(nr);
                  setMessageString(
                      BaseMessages.getString(
                          PKG, "ConditionEditor.GoUpOneLevel.Label", "" + getLevel()));
                  redraw();
                  break;
                case AREA_UP:
                  // Go to the parent condition...
                  goUp();
                  break;
                case AREA_FUNCTION:
                  if (activeCondition.isAtomic()) {
                    esd =
                        new EnterSelectionDialog(
                            shell,
                            Condition.functions,
                            BaseMessages.getString(PKG, "ConditionEditor.Functions.Label"),
                            BaseMessages.getString(PKG, "ConditionEditor.SelectFunction.Label"));
                    esd.setAvoidQuickSearch();
                    def = activeCondition.getFunctionDesc();
                    defnr = esd.getSelectionNr(def);
                    selection = esd.open(defnr);
                    if (selection != null) {
                      int fnnr = Condition.getFunction(selection);
                      activeCondition.setFunction(fnnr);

                      if (activeCondition.getFunction() == Condition.FUNC_NOT_NULL
                          || activeCondition.getFunction() == Condition.FUNC_NULL) {
                        activeCondition.setRightValuename(null);
                        activeCondition.setRightExact(null);
                      }

                      setModified();
                    }
                    widget.redraw();
                  }
                  break;
                case AREA_LEFT:
                  if (activeCondition.isAtomic() && fields != null) {
                    esd =
                        new EnterSelectionDialog(
                            shell,
                            fields.getFieldNamesAndTypes(maxFieldLength),
                            BaseMessages.getString(PKG, "ConditionEditor.Fields"),
                            BaseMessages.getString(PKG, "ConditionEditor.SelectAField"));
                    esd.setAvoidQuickSearch();
                    def = activeCondition.getLeftValuename();
                    defnr = esd.getSelectionNr(def);
                    selection = esd.open(defnr);
                    if (selection != null) {
                      IValueMeta v = fields.getValueMeta(esd.getSelectionNr());
                      activeCondition.setLeftValuename(v.getName());
                      setModified();
                    }
                    widget.redraw();
                  }
                  break;
                case AREA_RIGHT_VALUE:
                  if (activeCondition.isAtomic() && fields != null) {
                    esd =
                        new EnterSelectionDialog(
                            shell,
                            fields.getFieldNamesAndTypes(maxFieldLength),
                            BaseMessages.getString(PKG, "ConditionEditor.Fields"),
                            BaseMessages.getString(PKG, "ConditionEditor.SelectAField"));
                    esd.setAvoidQuickSearch();
                    def = activeCondition.getLeftValuename();
                    defnr = esd.getSelectionNr(def);
                    selection = esd.open(defnr);
                    if (selection != null) {
                      IValueMeta v = fields.getValueMeta(esd.getSelectionNr());
                      activeCondition.setRightValuename(v.getName());
                      activeCondition.setRightExact(null);
                      setModified();
                    }
                    widget.redraw();
                  }
                  break;
                case AREA_RIGHT_EXACT:
                  if (activeCondition.isAtomic()) {
                    ValueMetaAndData v = activeCondition.getRightExact();
                    if (v == null) {
                      IValueMeta leftval =
                          fields != null
                              ? fields.searchValueMeta(activeCondition.getLeftValuename())
                              : null;
                      if (leftval != null) {
                        try {
                          v =
                              new ValueMetaAndData(
                                  ValueMetaFactory.createValueMeta("constant", leftval.getType()),
                                  null);
                        } catch (Exception exception) {
                          new ErrorDialog(
                              shell, "Error", "Error creating value meta object", exception);
                        }
                      } else {
                        v = new ValueMetaAndData(new ValueMetaString("constant"), null);
                      }
                    }
                    EnterValueDialog evd =
                        new EnterValueDialog(shell, SWT.NONE, v.getValueMeta(), v.getValueData());
                    evd.setModalDialog(
                        true); // To keep the condition editor from being closed with a value dialog
                    // still
                    // open.
                    ValueMetaAndData newval = evd.open();
                    if (newval != null) {
                      activeCondition.setRightValuename(null);
                      activeCondition.setRightExact(newval);
                      setModified();
                    }
                    widget.redraw();
                  }
                  break;
                case AREA_ICON_ADD:
                  addCondition();
                  break;

                default:
                  break;
              }
            }
          }

          @Override
          public void mouseUp(MouseEvent e) {
            // Disable mouseUp event
          }
        });

    //
    // set the pop-up menu
    //
    widget.addMenuDetectListener(
        e -> {
          Point screen = new Point(e.x, e.y);
          Point widgetScreen = widget.toDisplay(1, 1);
          Point wRel = new Point(screen.x - widgetScreen.x, screen.y - widgetScreen.y);
          int area = getAreaCode(wRel);
          setMenu(area, wRel);
        });

    sbVertical.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            offsety = -sbVertical.getSelection();
            widget.redraw();
          }
        });

    sbHorizontal.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            offsetx = -sbHorizontal.getSelection();
            widget.redraw();
          }
        });

    widget.addControlListener(
        new ControlAdapter() {
          @Override
          public void controlResized(ControlEvent arg0) {
            sizeWidget = widget.getBounds();
            setBars();
          }
        });
  }

  private void getMaxFieldLength() {
    maxFieldLength = 5;
    if (fields != null) {
      for (int i = 0; i < fields.size(); i++) {
        IValueMeta value = fields.getValueMeta(i);
        if (value != null && value.getName() != null) {
          int len = fields.getValueMeta(i).getName().length();
          if (len > maxFieldLength) {
            maxFieldLength = len;
          }
        }
      }
    }
  }

  public int getLevel() {
    return parents.size();
  }

  public void goUp() {
    if (parents.size() > 0) {
      int last = parents.size() - 1;
      activeCondition = parents.get(last);
      parents.remove(last);

      redraw();
    }
    if (getLevel() > 0) {
      setMessageString(
          BaseMessages.getString(PKG, "ConditionEditor.GoUpOneLevel.Label", "" + getLevel()));
    } else {
      setMessageString(BaseMessages.getString(PKG, "ConditionEditor.EditSubCondition"));
    }
  }

  private void setMenu(int area, Point screen) {
    final int cond_nr = getNrSubcondition(screen);
    if (mPop != null && !mPop.isDisposed()) {
      mPop.dispose();
    }

    switch (area) {
      case AREA_NOT:
        mPop = new Menu(widget);
        MenuItem miNegate = new MenuItem(mPop, SWT.PUSH);
        miNegate.setText(BaseMessages.getString(PKG, "ConditionEditor.NegateCondition"));
        miNegate.addSelectionListener(
            new SelectionAdapter() {
              @Override
              public void widgetSelected(SelectionEvent e) {
                activeCondition.negate();
                widget.redraw();
                setModified();
              }
            });
        setMenu(mPop);
        break;
      case AREA_BACKGROUND:
      case AREA_ICON_ADD:
        mPop = new Menu(widget);
        MenuItem miAdd = new MenuItem(mPop, SWT.PUSH);
        miAdd.setText(BaseMessages.getString(PKG, "ConditionEditor.AddCondition.Label"));
        miAdd.addSelectionListener(
            new SelectionAdapter() {
              @Override
              public void widgetSelected(SelectionEvent e) {
                addCondition();
              }
            });
        setMenu(mPop);
        break;
      case AREA_SUBCONDITION:
        mPop = new Menu(widget);
        MenuItem miEdit = new MenuItem(mPop, SWT.PUSH);
        miEdit.setText(BaseMessages.getString(PKG, "ConditionEditor.EditCondition.Label"));
        miEdit.addSelectionListener(
            new SelectionAdapter() {
              @Override
              public void widgetSelected(SelectionEvent e) {
                editCondition(cond_nr);
                setModified();
                widget.redraw();
              }
            });
        MenuItem miDel = new MenuItem(mPop, SWT.PUSH);
        miDel.setText(BaseMessages.getString(PKG, "ConditionEditor.DeleteCondition.Label"));
        miDel.addSelectionListener(
            new SelectionAdapter() {
              @Override
              public void widgetSelected(SelectionEvent e) {
                removeCondition(cond_nr);
                setModified();
                widget.redraw();
              }
            });
        // Add a sub-condition in the subcondition... (move down)
        final Condition sub = activeCondition.getCondition(cond_nr);
        if (sub.getLeftValuename() != null) {
          miAdd = new MenuItem(mPop, SWT.PUSH);
          miAdd.setText(BaseMessages.getString(PKG, "ConditionEditor.AddSubCondition.Label"));
          miAdd.addSelectionListener(
              new SelectionAdapter() {
                @Override
                public void widgetSelected(SelectionEvent e) {
                  Condition c = new Condition();
                  c.setOperator(Condition.OPERATOR_AND);
                  sub.addCondition(c);
                  setModified();
                  widget.redraw();
                }
              });
        }
        // --------------------------------------------------
        new MenuItem(mPop, SWT.SEPARATOR);

        MenuItem miCopy = new MenuItem(mPop, SWT.PUSH);
        miCopy.setText(BaseMessages.getString(PKG, "ConditionEditor.CopyToClipboard"));
        miCopy.addSelectionListener(
            new SelectionAdapter() {
              @Override
              public void widgetSelected(SelectionEvent e) {
                Condition c = activeCondition.getCondition(cond_nr);
                try {
                  String xml = c.getXml();
                  GuiResource.getInstance().toClipboard(xml);
                  widget.redraw();
                } catch (Exception ex) {
                  new ErrorDialog(shell, "Error", "Error encoding to XML", ex);
                }
              }
            });
        MenuItem miPasteBef = new MenuItem(mPop, SWT.PUSH);
        miPasteBef.setText(
            BaseMessages.getString(PKG, "ConditionEditor.PasteFromClipboardBeforeCondition"));
        miPasteBef.addSelectionListener(
            new SelectionAdapter() {
              @Override
              public void widgetSelected(SelectionEvent e) {
                String xml = GuiResource.getInstance().fromClipboard();
                try {
                  Document d = XmlHandler.loadXmlString(xml);
                  Node condNode = XmlHandler.getSubNode(d, "condition");
                  if (condNode != null) {
                    Condition c = new Condition(condNode);
                    activeCondition.addCondition(cond_nr, c);
                    widget.redraw();
                  } else {
                    new ErrorDialog(
                        shell,
                        BaseMessages.getString(PKG, "ConditionEditor.Error"),
                        BaseMessages.getString(PKG, "ConditionEditor.NoConditionFoundXML"),
                        new HopXmlException(
                            BaseMessages.getString(
                                PKG,
                                "ConditionEditor.NoConditionFoundXML.Exception",
                                Const.CR + Const.CR + xml)));
                  }
                } catch (HopXmlException ex) {
                  new ErrorDialog(
                      shell,
                      BaseMessages.getString(PKG, "ConditionEditor.Error"),
                      BaseMessages.getString(PKG, "ConditionEditor.ErrorParsingCondition"),
                      ex);
                }
              }
            });
        // --------------------------------------------------
        new MenuItem(mPop, SWT.SEPARATOR);

        MenuItem miPasteAft = new MenuItem(mPop, SWT.PUSH);
        miPasteAft.setText(
            BaseMessages.getString(PKG, "ConditionEditor.PasteFromClipboardAfterCondition"));
        miPasteAft.addSelectionListener(
            new SelectionAdapter() {
              @Override
              public void widgetSelected(SelectionEvent e) {
                String xml = GuiResource.getInstance().fromClipboard();
                try {
                  Document d = XmlHandler.loadXmlString(xml);
                  Node condNode = XmlHandler.getSubNode(d, "condition");
                  if (condNode != null) {
                    Condition c = new Condition(condNode);
                    activeCondition.addCondition(cond_nr + 1, c);
                    widget.redraw();
                  } else {
                    new ErrorDialog(
                        shell,
                        BaseMessages.getString(PKG, "ConditionEditor.Error"),
                        BaseMessages.getString(PKG, "ConditionEditor.NoConditionFoundXML"),
                        new HopXmlException(
                            BaseMessages.getString(
                                PKG,
                                "ConditionEditor.NoConditionFoundXML.Exception",
                                Const.CR + Const.CR + xml)));
                  }
                } catch (HopXmlException ex) {
                  new ErrorDialog(
                      shell,
                      BaseMessages.getString(PKG, "ConditionEditor.Error"),
                      BaseMessages.getString(PKG, "ConditionEditor.ErrorParsingCondition"),
                      ex);
                }
              }
            });
        // --------------------------------------------------
        new MenuItem(mPop, SWT.SEPARATOR);
        MenuItem miMoveSub = new MenuItem(mPop, SWT.PUSH);
        miMoveSub.setText(
            BaseMessages.getString(PKG, "ConditionEditor.MoveConditionToSubCondition"));
        miMoveSub.addSelectionListener(
            new SelectionAdapter() {
              @Override
              public void widgetSelected(SelectionEvent e) {
                // Move the condition lower: this means create a subcondition and put the condition
                // there in the list.
                //
                Condition down = activeCondition.getCondition(cond_nr);
                Condition c = new Condition();
                c.setOperator(down.getOperator());
                down.setOperator(Condition.OPERATOR_NONE);
                activeCondition.setCondition(cond_nr, c);
                c.addCondition(down);

                widget.redraw();
              }
            });
        MenuItem miMoveParent = new MenuItem(mPop, SWT.PUSH);
        miMoveParent.setText(
            BaseMessages.getString(PKG, "ConditionEditor.MoveConditionToParentCondition"));
        if (getLevel() == 0) {
          miMoveParent.setEnabled(false);
        }
        miMoveParent.addSelectionListener(
            new SelectionAdapter() {
              @Override
              public void widgetSelected(SelectionEvent e) {
                // Move the condition lower: this means delete the condition from the
                // active_condition.
                // After that, move it to the parent.
                Condition up = activeCondition.getCondition(cond_nr);
                activeCondition.removeCondition(cond_nr);
                Condition parent = parents.get(getLevel() - 1);

                parent.addCondition(up);

                // Take a look upward...
                goUp();

                widget.redraw();
              }
            });
        // --------------------------------------------------
        new MenuItem(mPop, SWT.SEPARATOR);
        MenuItem miMoveDown = new MenuItem(mPop, SWT.PUSH);
        miMoveDown.setText(BaseMessages.getString(PKG, "ConditionEditor.MoveConditionDown"));
        if (cond_nr >= activeCondition.nrConditions() - 1) {
          miMoveDown.setEnabled(false);
        }
        miMoveDown.addSelectionListener(
            new SelectionAdapter() {
              @Override
              public void widgetSelected(SelectionEvent e) {
                Condition down = activeCondition.getCondition(cond_nr);
                activeCondition.removeCondition(cond_nr);
                activeCondition.addCondition(cond_nr + 1, down);

                widget.redraw();
              }
            });
        MenuItem miMoveUp = new MenuItem(mPop, SWT.PUSH);
        miMoveUp.setText(BaseMessages.getString(PKG, "ConditionEditor.MoveConditionUp"));
        if (cond_nr == 0) {
          miMoveUp.setEnabled(false);
        }
        miMoveUp.addSelectionListener(
            new SelectionAdapter() {
              @Override
              public void widgetSelected(SelectionEvent e) {
                Condition up = activeCondition.getCondition(cond_nr);
                activeCondition.removeCondition(cond_nr);
                activeCondition.addCondition(cond_nr - 1, up);

                widget.redraw();
              }
            });

        setMenu(mPop);

        break;
      case AREA_OPERATOR:
        Menu mPop = new Menu(widget);
        MenuItem miDown = new MenuItem(mPop, SWT.PUSH);
        miDown.setText(BaseMessages.getString(PKG, "ConditionEditor.MoveDown"));
        miDown.addSelectionListener(
            new SelectionAdapter() {
              @Override
              public void widgetSelected(SelectionEvent e) {
                // Move a condition down!
                // oper_nr = 1 : means move down
                setModified();
                widget.redraw();
              }
            });
        setMenu(mPop);
        break;

      default:
        setMenu(null);
        break;
    }
  }

  public void repaint(GC gc, int width, int height) {
    // Initialize some information
    sizeNot = getNotSize(gc);
    sizeWidget = getWidgetSize(gc);
    sizeAndNot = getAndNotSize(gc);
    sizeUp = getUpSize(gc);
    sizeAdd = getAddSize(gc);
    sizeLeft = null;
    sizeFn = null;
    sizeRightval = null;
    sizeRightex = null;

    // Clear the background...
    gc.setBackground(white);
    gc.setForeground(black);
    gc.fillRectangle(0, 0, width, height);

    // Set the fixed font:
    gc.setFont(fixed);

    // Atomic condition?
    if (activeCondition.isAtomic()) {
      sizeCond = null;
      drawNegated(gc, 0, 0, activeCondition);

      drawAtomic(gc, 0, 0, activeCondition);

    } else {
      drawNegated(gc, 0, 0, activeCondition);

      sizeCond = new Rectangle[activeCondition.nrConditions()];
      sizeOper = new Rectangle[activeCondition.nrConditions()];

      int basex = 10;
      int basey = sizeNot.y + 5;

      for (int i = 0; i < activeCondition.nrConditions(); i++) {
        Point to = drawCondition(gc, basex, basey, i, activeCondition.getCondition(i));
        basey += sizeAndNot.height + to.y + 15;
      }
    }

    gc.drawImage(imageAdd, sizeAdd.x, sizeAdd.y);

    /*
     * Draw the up-symbol if needed...
     */
    if (parents.size() > 0) {
      drawUp(gc);
    }

    if (messageString != null) {
      drawMessage(gc);
    }

    /*
     * Determine the maximum size of the displayed items... Normally, they are all size up already.
     */
    getMaxSize();

    /*
     * Set the scroll bars: show/don't show and set the size
     */
    setBars();
  }

  private Rectangle getNotSize(GC gc) {
    Point p = gc.textExtent(STRING_NOT);
    return new Rectangle(0, 0, p.x + 10, p.y + 4);
  }

  private Rectangle getWidgetSize(GC gc) {
    return widget.getBounds();
  }

  private Rectangle getAndNotSize(GC gc) {
    Point p = gc.textExtent(Condition.operators[Condition.OPERATOR_AND_NOT]);
    return new Rectangle(0, 0, p.x, p.y);
  }

  private Rectangle getUpSize(GC gc) {
    Point p = gc.textExtent(STRING_UP);
    return new Rectangle(sizeNot.x + sizeNot.width + 40, sizeNot.y, p.x + 20, sizeNot.height);
  }

  private Rectangle getAddSize(GC gc) {
    Rectangle is = imageAdd.getBounds(); // image size
    Rectangle cs = getBounds(); // Canvas size

    return new Rectangle(cs.width - is.width - 5 - X_PADDING, 5, is.width, is.height);
  }

  private void drawNegated(GC gc, int x, int y, Condition condition) {
    Color color = gc.getForeground();

    if (hoverNot) {
      gc.setBackground(gray);
    }
    gc.fillRectangle(real2Screen(sizeNot));
    gc.drawRectangle(real2Screen(sizeNot));

    if (condition.isNegated()) {
      if (hoverNot) {
        gc.setForeground(green);
      }
      gc.drawText(
          STRING_NOT, sizeNot.x + 5 + offsetx, sizeNot.y + 2 + offsety, SWT.DRAW_TRANSPARENT);
      gc.drawText(
          STRING_NOT, sizeNot.x + 6 + offsetx, sizeNot.y + 2 + offsety, SWT.DRAW_TRANSPARENT);
      if (hoverNot) {
        gc.setForeground(color);
      }
    } else {
      if (hoverNot) {
        gc.setForeground(red);
        gc.drawText(
            STRING_NOT, sizeNot.x + 5 + offsetx, sizeNot.y + 2 + offsety, SWT.DRAW_TRANSPARENT);
        gc.drawText(
            STRING_NOT, sizeNot.x + 6 + offsetx, sizeNot.y + 2 + offsety, SWT.DRAW_TRANSPARENT);
        gc.setForeground(color);
      }
    }

    if (hoverNot) {
      gc.setBackground(bg);
    }
  }

  private void drawAtomic(GC gc, int x, int y, Condition condition) {

    // First the text sizes...
    String left = Const.rightPad(condition.getLeftValuename(), maxFieldLength);
    Point extLeft = gc.textExtent(left);
    if (condition.getLeftValuename() == null) {
      extLeft = gc.textExtent("<field>");
    }

    String fnMax = Condition.functions[Condition.FUNC_NOT_NULL];
    String fn = condition.getFunctionDesc();
    Point extFn = gc.textExtent(fnMax);

    String rightval = Const.rightPad(condition.getRightValuename(), maxFieldLength);
    Point extRval = gc.textExtent(rightval);
    if (condition.getLeftValuename() == null) {
      extRval = gc.textExtent("<field>");
    }

    String rightex = condition.getRightExactString();

    String rightexMax = rightex;
    if (rightex == null) {
      rightexMax = Const.rightPad(" ", 10);
    } else {
      if (rightex.length() < 10) {
        rightexMax = Const.rightPad(rightex, 10);
      }
    }

    Point extRex = gc.textExtent(rightexMax);

    sizeLeft = new Rectangle(x + 5, y + sizeNot.height + 5, extLeft.x + 5, extLeft.y + 5);

    sizeFn =
        new Rectangle(
            sizeLeft.x + sizeLeft.width + 15, y + sizeNot.height + 5, extFn.x + 5, extFn.y + 5);

    sizeRightval =
        new Rectangle(
            sizeFn.x + sizeFn.width + 15, y + sizeNot.height + 5, extRval.x + 5, extRval.y + 5);

    sizeRightex =
        new Rectangle(
            sizeFn.x + sizeFn.width + 15,
            y + sizeNot.height + 5 + sizeRightval.height + 5,
            extRex.x + 5,
            extRex.y + 5);

    if (hoverLeft) {
      gc.setBackground(gray);
    }
    gc.fillRectangle(real2Screen(sizeLeft));
    gc.drawRectangle(real2Screen(sizeLeft));
    gc.setBackground(bg);

    if (hoverFn) {
      gc.setBackground(gray);
    }
    gc.fillRectangle(real2Screen(sizeFn));
    gc.drawRectangle(real2Screen(sizeFn));
    gc.setBackground(bg);

    if (hoverRightval) {
      gc.setBackground(gray);
    }
    gc.fillRectangle(real2Screen(sizeRightval));
    gc.drawRectangle(real2Screen(sizeRightval));
    gc.setBackground(bg);

    if (hoverRightex) {
      gc.setBackground(gray);
    }
    gc.fillRectangle(real2Screen(sizeRightex));
    gc.drawRectangle(real2Screen(sizeRightex));
    gc.setBackground(bg);

    if (condition.getLeftValuename() != null) {
      gc.drawText(left, sizeLeft.x + 1 + offsetx, sizeLeft.y + 1 + offsety, SWT.DRAW_TRANSPARENT);
    } else {
      gc.setForeground(gray);
      gc.drawText(
          "<field>", sizeLeft.x + 1 + offsetx, sizeLeft.y + 1 + offsety, SWT.DRAW_TRANSPARENT);
      gc.setForeground(black);
    }

    gc.drawText(fn, sizeFn.x + 1 + offsetx, sizeFn.y + 1 + offsety, SWT.DRAW_TRANSPARENT);

    if (condition.getFunction() != Condition.FUNC_NOT_NULL
        && condition.getFunction() != Condition.FUNC_NULL) {
      String re = rightex == null ? "" : rightex;
      String stype = "";
      ValueMetaAndData v = condition.getRightExact();
      if (v != null) {
        stype = " (" + v.getValueMeta().getTypeDesc() + ")";
      }

      if (condition.getRightValuename() != null) {
        gc.drawText(
            rightval,
            sizeRightval.x + 1 + offsetx,
            sizeRightval.y + 1 + offsety,
            SWT.DRAW_TRANSPARENT);
      } else {
        String nothing = rightex == null ? "<field>" : "";
        gc.setForeground(gray);
        gc.drawText(
            nothing,
            sizeRightval.x + 1 + offsetx,
            sizeRightval.y + 1 + offsety,
            SWT.DRAW_TRANSPARENT);
        if (condition.getRightValuename() == null) {
          gc.setForeground(black);
        }
      }

      if (rightex != null) {
        gc.drawText(
            re, sizeRightex.x + 1 + offsetx, sizeRightex.y + 1 + offsety, SWT.DRAW_TRANSPARENT);
      } else {
        String nothing = condition.getRightValuename() == null ? "<value>" : "";
        gc.setForeground(gray);
        gc.drawText(
            nothing,
            sizeRightex.x + 1 + offsetx,
            sizeRightex.y + 1 + offsety,
            SWT.DRAW_TRANSPARENT);
        gc.setForeground(black);
      }

      gc.drawText(
          stype,
          sizeRightex.x + 1 + sizeRightex.width + 10 + offsetx,
          sizeRightex.y + 1 + offsety,
          SWT.DRAW_TRANSPARENT);
    } else {
      gc.drawText(
          "-", sizeRightval.x + 1 + offsetx, sizeRightval.y + 1 + offsety, SWT.DRAW_TRANSPARENT);
      gc.drawText(
          "-", sizeRightex.x + 1 + offsetx, sizeRightex.y + 1 + offsety, SWT.DRAW_TRANSPARENT);
    }
  }

  private Point drawCondition(GC gc, int x, int y, int nr, Condition condition) {
    int opx;
    int opy;
    int opw;
    int oph;
    int cx;
    int cy;
    int cw;
    int ch;

    opx = x;
    opy = y;
    opw = sizeAndNot.width + 6;
    oph = sizeAndNot.height + 2;

    /*
     * First draw the operator ...
     */
    if (nr > 0) {
      String operator = condition.getOperatorDesc();
      // Remember the size of the rectangle!
      sizeOper[nr] = new Rectangle(opx, opy, opw, oph);
      if (nr == hoverOperator) {
        gc.setBackground(gray);
        gc.fillRectangle(real2Screen(sizeOper[nr]));
        gc.drawRectangle(real2Screen(sizeOper[nr]));
        gc.setBackground(bg);
      }
      gc.drawText(
          operator,
          sizeOper[nr].x + 2 + offsetx,
          sizeOper[nr].y + 2 + offsety,
          SWT.DRAW_TRANSPARENT);
    }

    /*
     * Then draw the condition below, possibly negated!
     */
    String str = condition.toString(0, true, false); // don't show the operator!
    Point p = gc.textExtent(str);

    cx = opx + 23;
    cy = opy + oph + 10;
    cw = p.x + 5;
    ch = p.y + 5;

    // Remember the size of the rectangle!
    sizeCond[nr] = new Rectangle(cx, cy, cw, ch);

    if (nr == hoverCondition) {
      gc.setBackground(gray);
      gc.fillRectangle(real2Screen(sizeCond[nr]));
      gc.drawRectangle(real2Screen(sizeCond[nr]));
      gc.setBackground(bg);
    }
    gc.drawText(
        str,
        sizeCond[nr].x + 2 + offsetx,
        sizeCond[nr].y + 5 + offsety,
        SWT.DRAW_DELIMITER | SWT.DRAW_TRANSPARENT | SWT.DRAW_TAB | SWT.DRAW_MNEMONIC);

    p.x += 0;
    p.y += 5;

    return p;
  }

  public void drawUp(GC gc) {
    if (hoverUp) {
      gc.setBackground(gray);
      gc.fillRectangle(sizeUp);
    }
    gc.drawRectangle(sizeUp);
    gc.drawText(STRING_UP, sizeUp.x + 1 + offsetx, sizeUp.y + 1 + offsety, SWT.DRAW_TRANSPARENT);
  }

  public void drawMessage(GC gc) {
    gc.setForeground(blue);
    gc.drawText(
        getMessageString(),
        sizeUp.x + sizeUp.width + offsetx + 40,
        sizeUp.y + 1 + offsety,
        SWT.DRAW_TRANSPARENT);
  }

  private boolean isInNot(Point screen) {
    if (sizeNot == null) {
      return false;
    }
    return real2Screen(sizeNot).contains(screen);
  }

  private boolean isInUp(Point screen) {
    if (sizeUp == null || parents.isEmpty()) {
      return false; // not displayed!
    }

    return real2Screen(sizeUp).contains(screen);
  }

  private boolean isInAdd(Point screen) {
    if (sizeAdd == null || screen == null) {
      return false;
    }
    return sizeAdd.contains(screen);
  }

  private boolean isInWidget(Point screen) {
    if (sizeWidget == null) {
      return false;
    }

    return real2Screen(sizeWidget).contains(screen);
  }

  private int getNrSubcondition(Point screen) {
    if (sizeCond == null) {
      return -1;
    }

    for (int i = 0; i < sizeCond.length; i++) {
      if (sizeCond[i] != null && screen2Real(sizeCond[i]).contains(screen)) {
        return i;
      }
    }
    return -1;
  }

  private boolean isInSubcondition(Point screen) {
    return getNrSubcondition(screen) >= 0;
  }

  private int getNrOperator(Point screen) {
    if (sizeOper == null) {
      return -1;
    }

    for (int i = 0; i < sizeOper.length; i++) {
      if (sizeOper[i] != null && screen2Real(sizeOper[i]).contains(screen)) {
        return i;
      }
    }
    return -1;
  }

  private boolean isInOperator(Point screen) {
    return getNrOperator(screen) >= 0;
  }

  private boolean isInLeft(Point screen) {
    if (sizeLeft == null) {
      return false;
    }
    return real2Screen(sizeLeft).contains(screen);
  }

  private boolean isInFunction(Point screen) {
    if (sizeFn == null) {
      return false;
    }
    return real2Screen(sizeFn).contains(screen);
  }

  private boolean isInRightValue(Point screen) {
    if (sizeRightval == null) {
      return false;
    }
    return real2Screen(sizeRightval).contains(screen);
  }

  private boolean isInRightExact(Point screen) {
    if (sizeRightex == null) {
      return false;
    }
    return real2Screen(sizeRightex).contains(screen);
  }

  private int getAreaCode(Point screen) {
    if (isInNot(screen)) {
      return AREA_NOT;
    }
    if (isInUp(screen)) {
      return AREA_UP;
    }
    if (isInAdd(screen)) {
      return AREA_ICON_ADD;
    }

    if (activeCondition.isAtomic()) {
      if (isInLeft(screen)) {
        return AREA_LEFT;
      }
      if (isInFunction(screen)) {
        return AREA_FUNCTION;
      }
      if (isInRightExact(screen)) {
        return AREA_RIGHT_EXACT;
      }
      if (isInRightValue(screen)) {
        return AREA_RIGHT_VALUE;
      }
    } else {
      if (isInSubcondition(screen)) {
        return AREA_SUBCONDITION;
      }
      if (isInOperator(screen)) {
        return AREA_OPERATOR;
      }
    }

    if (isInWidget(screen)) {
      return AREA_BACKGROUND;
    }

    return AREA_NONE;
  }

  /**
   * Edit the condition in a separate dialog box...
   *
   * @param nr The condition nr to be edited
   */
  private void editCondition(int nr) {
    if (activeCondition.isComposite()) {
      parents.add(activeCondition);
      activeCondition = activeCondition.getCondition(nr);
    }
  }

  private void addCondition() {
    Condition c = new Condition();
    c.setOperator(Condition.OPERATOR_AND);

    addCondition(c);
    setModified();

    widget.redraw();
  }

  /**
   * Add a sub-condition to the active condition...
   *
   * @param condition The condition to which we want to add one more.
   */
  private void addCondition(Condition condition) {
    activeCondition.addCondition(condition);
  }

  /**
   * Remove a sub-condition from the active condition...
   *
   * @param nr The condition nr to be removed.
   */
  private void removeCondition(int nr) {
    activeCondition.removeCondition(nr);
  }

  /** @param messageString The messageString to set. */
  public void setMessageString(String messageString) {
    this.messageString = messageString;
  }

  /** @return Returns the messageString. */
  public String getMessageString() {
    return messageString;
  }

  private Rectangle real2Screen(Rectangle r) {
    return new Rectangle(r.x + offsetx, r.y + offsety, r.width, r.height);
  }

  private Rectangle screen2Real(Rectangle r) {
    return new Rectangle(r.x - offsetx, r.y - offsety, r.width, r.height);
  }

  /** Determine the maximum rectangle of used canvas variables... */
  private void getMaxSize() {
    // Top line...
    maxdrawn = sizeNot.union(sizeUp);

    // Atomic
    if (activeCondition.isAtomic()) {
      maxdrawn = maxdrawn.union(sizeLeft);
      maxdrawn = maxdrawn.union(sizeFn);
      maxdrawn = maxdrawn.union(sizeRightval);
      maxdrawn = maxdrawn.union(sizeRightex);
      maxdrawn.width += 100;
    } else {
      if (sizeCond != null) {
        for (int i = 0; i < sizeCond.length; i++) {
          if (sizeCond[i] != null) {
            maxdrawn = maxdrawn.union(sizeCond[i]);
          }
        }
      }
      if (sizeOper != null) {
        for (int i = 0; i < sizeOper.length; i++) {
          if (sizeOper[i] != null) {
            maxdrawn = maxdrawn.union(sizeOper[i]);
          }
        }
      }
    }

    maxdrawn.width += 10;
    maxdrawn.height += 10;
  }

  private void setBars() {
    if (sizeWidget == null || maxdrawn == null) {
      return;
    }

    // Horizontal scrollbar behavior
    //
    if (sizeWidget.width > maxdrawn.width) {
      offsetx = 0;
      sbHorizontal.setSelection(0);
      sbHorizontal.setVisible(false);
    } else {
      offsetx = -sbHorizontal.getSelection();
      sbHorizontal.setVisible(true);
      // Set the bar's parameters...
      sbHorizontal.setMaximum(maxdrawn.width);
      sbHorizontal.setMinimum(0);
      if (!EnvironmentUtils.getInstance().isWeb()) {
        sbHorizontal.setPageIncrement(sizeWidget.width);
        sbHorizontal.setIncrement(10);
      }
    }

    // Vertical scrollbar behavior
    //
    if (sizeWidget.height > maxdrawn.height) {
      offsety = 0;
      sbVertical.setSelection(0);
      sbVertical.setVisible(false);
    } else {
      offsety = sbVertical.getSelection();
      sbVertical.setVisible(true);
      // Set the bar's parameters...
      sbVertical.setMaximum(maxdrawn.height);
      sbVertical.setMinimum(0);
      if (!EnvironmentUtils.getInstance().isWeb()) {
        sbVertical.setPageIncrement(sizeWidget.height);
        sbVertical.setIncrement(10);
      }
    }
  }

  public void addModifyListener(ModifyListener lsMod) {
    modListeners.add(lsMod);
  }

  public void setModified() {
    for (int i = 0; i < modListeners.size(); i++) {
      ModifyListener lsMod = modListeners.get(i);
      if (lsMod != null) {
        Event e = new Event();
        e.widget = this;
        lsMod.modifyText(new ModifyEvent(e));
      }
    }
  }

  @Override
  public void mouseMove(MouseEvent e) {

    Point screen = new Point(e.x, e.y);
    int area = getAreaCode(screen);

    int nr = 0;
    boolean needRedraw = false;

    hoverCondition = -1;
    hoverOperator = -1;
    hoverNot = false;
    hoverUp = false;
    hoverLeft = false;
    hoverFn = false;
    hoverRightval = false;
    hoverRightex = false;

    if (area != AREA_ICON_ADD) {
      setToolTipText(null);
    } else {
      setToolTipText(BaseMessages.getString(PKG, "ConditionEditor.AddCondition.Label"));
    }

    switch (area) {
      case AREA_NOT:
        hoverNot = true;
        nr = 1;
        break;
      case AREA_UP:
        hoverUp = getLevel() > 0;
        nr = 1;
        break;
      case AREA_BACKGROUND:
        break;
      case AREA_SUBCONDITION:
        hoverCondition = getNrSubcondition(screen);
        nr = hoverCondition;
        break;
      case AREA_OPERATOR:
        hoverOperator = getNrOperator(screen);
        nr = hoverOperator;
        break;
      case AREA_LEFT:
        hoverLeft = true;
        nr = 1;
        break;
      case AREA_FUNCTION:
        hoverFn = true;
        nr = 1;
        break;
      case AREA_RIGHT_VALUE:
        hoverRightval = true;
        nr = 1;
        break;
      case AREA_RIGHT_EXACT:
        hoverRightex = true;
        nr = 1;
        break;
      case AREA_CONDITION:
        break;
      case AREA_NONE:
        break;
      default:
        break;
    }

    if (area != previousArea || nr != previousAreaNr) {
      needRedraw = true;
    }

    if (needRedraw) {
      offsetx = -sbHorizontal.getSelection();
      offsety = -sbVertical.getSelection();
      widget.redraw();
    }

    previousArea = area;
    previousAreaNr = nr;
  }
}
