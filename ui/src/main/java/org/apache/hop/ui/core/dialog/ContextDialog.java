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

package org.apache.hop.ui.core.dialog;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.SwtUniversalImage;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.plugin.GuiAction;
import org.apache.hop.core.gui.plugin.GuiActionType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.util.SwtSvgImageUtil;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.PaintEvent;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.ScrollBar;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class ContextDialog extends Dialog {

	private Point location;
	private List<GuiAction> actions;
	private PropsUi props;
	private Shell shell;
	private Text wSearch;
	private Label wlTooltip;
	private Canvas wCanvas;

	private int iconSize;
	private int maxNameWidth;
	private int maxNameHeight;

	private int cellWidth;
	private int cellHeight;
	private int margin;

	private boolean shiftClicked;
	private boolean ctrlClicked;
	private boolean focusLost;

	/**
	 * All context items.
	 */
	private final List<Item> items = new ArrayList<>();

	/**
	 * List of filtered items.
	 */
	private final List<Item> filteredItems = new ArrayList<>();

	private Item selectedItem;

	private GuiAction selectedAction;

	private static class Item {
		private GuiAction action;
		private Image image;

		public Item(GuiAction action, Image image) {
			this.action = action;
			this.image = image;
		}

		public GuiAction getAction() {
			return action;
		}

		public String getText() {
			return action.getShortName();
		}

		public Image getImage() {
			return image;
		}

		public void dispose() {
			if (image != null) {
				image.dispose();
			}
		}
	}

	public ContextDialog(Shell parent, String title, Point location, List<GuiAction> actions) {
		super(parent);
		
		this.setText(title);
		this.location = location;
		this.actions = actions;
		props = PropsUi.getInstance();

		shiftClicked = false;
		ctrlClicked = false;

		// Make the icons a bit smaller to fit more
		//
		iconSize = (int) Math.round(props.getZoomFactor() * props.getIconSize() * 0.75);
	}

	public GuiAction open() {
		
		shell = new Shell(getParent(), SWT.DIALOG_TRIM | SWT.RESIZE );
		shell.setText(getText());
		shell.setMinimumSize(new org.eclipse.swt.graphics.Point(200,180));
		shell.setImage(GuiResource.getInstance().getImageHop());
		shell.setLayout(new FormLayout());
		//props.setLook(shell);
		
		// Load all the images...
		// Filter all actions by default
		//
		maxNameWidth = iconSize;
		maxNameHeight = 0;
		Display display = shell.getDisplay();
		Image img = new Image(display, 100, 100);
		GC gc = new GC(img);

		items.clear();
		for (GuiAction action : actions) {
			ClassLoader classLoader = action.getClassLoader();
			if (classLoader == null) {
				classLoader = ClassLoader.getSystemClassLoader();
			}
			SwtUniversalImage universalImage = SwtSvgImageUtil.getUniversalImage(display, classLoader,
					action.getImage());
			Image image = universalImage.getAsBitmapForSize(display, iconSize, iconSize);

			Item item = new Item(action, image);

			if (item.getText() != null) {
				org.eclipse.swt.graphics.Point extent = gc.textExtent(action.getShortName());
				if (extent.x > maxNameWidth) {
					maxNameWidth = extent.x;
				}
				if (extent.y > maxNameHeight) {
					maxNameHeight = extent.y;
				}
			}
			
			items.add(item);
		}
		gc.dispose();
		img.dispose();

		// Calculate the cell width height
		//
		margin = props.getMargin();
		cellWidth = maxNameWidth + margin;
		cellHeight = iconSize + margin + maxNameHeight + margin;
		
		// Add a search bar at the top...
		//
		Composite toolBar = new Composite(shell, SWT.NONE);		
		toolBar.setLayout(new GridLayout(2, false));
		props.setLook(toolBar);
		FormData fdlToolBar = new FormData();
		fdlToolBar.top = new FormAttachment(0, 0);
		fdlToolBar.left = new FormAttachment(0, 0);
		fdlToolBar.right = new FormAttachment(100, 0);
		toolBar.setLayoutData(fdlToolBar);
		
		Label wlSearch = new Label(toolBar, SWT.LEFT);
		wlSearch.setText("Search ");
		props.setLook(wlSearch);			
		
		wSearch = new Text(toolBar, SWT.LEFT | SWT.BORDER | SWT.SINGLE | SWT.SEARCH);
		wSearch.setLayoutData(new GridData(GridData.FILL_BOTH));		 
				
		// Add a description label at the bottom...
		//
		wlTooltip = new Label(shell, SWT.LEFT );
		FormData fdlTooltip = new FormData();
		fdlTooltip.height=this.cellHeight;				
		fdlTooltip.left = new FormAttachment(0, Const.FORM_MARGIN);
		fdlTooltip.right = new FormAttachment(100, -Const.FORM_MARGIN);
		fdlTooltip.bottom = new FormAttachment(100, -Const.FORM_MARGIN);
		wlTooltip.setLayoutData(fdlTooltip);

		// The rest of the dialog is used to draw the actions...
		//
		wCanvas = new Canvas(shell, SWT.NO_BACKGROUND | SWT.V_SCROLL);
		FormData fdCanvas = new FormData();
		fdCanvas.left = new FormAttachment(0, 0);
		fdCanvas.right = new FormAttachment(100, 0);
		fdCanvas.top = new FormAttachment(toolBar, 0);
		fdCanvas.bottom = new FormAttachment(wlTooltip, 0);
		wCanvas.setLayoutData(fdCanvas);

		// TODO: Calcualte a more dynamic size based on number of actions, screen size
		// and so on
		//
		int width = (int) Math.round(650 * props.getZoomFactor());
		int height = (int) Math.round(500 * props.getZoomFactor());
		shell.setSize(width, height);
		
		// Position the dialog where there was a click to be more intuitive
		//
		if (location != null) {
			shell.setLocation(location.x, location.y);
		} else {
			Rectangle parentBounds = HopGui.getInstance().getShell().getBounds();
			shell.setLocation(Math.max((parentBounds.width - width) / 2, 0),
					Math.max((parentBounds.height - height) / 2, 0));
		}

		// Add all the listeners
		//		
		shell.addListener(SWT.Resize, event -> updateVerticalBar());
		shell.addListener(SWT.Deactivate, event -> onFocusLost());

		wSearch.addModifyListener(event -> onModifySearch());
		wSearch.addKeyListener(new KeyAdapter() {
			@Override
			public void keyPressed(KeyEvent event) {
				onKeyPressed(event);
			}			
		});
		wSearch.addSelectionListener(new SelectionAdapter() {
			@Override
			public void widgetDefaultSelected(SelectionEvent e) {
				// Pressed enter
				//
				if ( selectedItem!=null) {
					selectedAction = selectedItem.getAction();
				}
				dispose();
			}
		});
		
		wCanvas.addPaintListener(event -> onPaint(event));
		wCanvas.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseDown(MouseEvent event) {
				// See where the click was...
				//									
				Item item = findItem(event.x, event.y);
				if (item != null) {
					selectedAction = item.getAction();
					
					shiftClicked = (event.stateMask & SWT.SHIFT) != 0;
					ctrlClicked = (event.stateMask & SWT.CONTROL) != 0 || (Const.isOSX() && (event.stateMask & SWT.COMMAND) != 0);
					
					dispose();
				}
			}
		});
		wCanvas.addMouseMoveListener((MouseEvent event) -> {
			// Do we mouse over an action?
			//
			Item item = findItem(event.x, event.y);
			if (item != null) {
				selectItem(item);
			}
		});
		wCanvas.getVerticalBar().addSelectionListener(new SelectionAdapter() {
			@Override
			public void widgetSelected(SelectionEvent e) {
				wCanvas.redraw();
			}
		});

		// Filter all actions by default
		//
		this.filter(null);
				
		// Show the dialog now
		//
		shell.layout();
		shell.open();
		
		// Wait until the dialog is closed
		//
		while (!shell.isDisposed()) {
			if (!display.readAndDispatch()) {
				display.sleep();
			}
		}

		return selectedAction;
	}	

	private void dispose() {
		
		// Close the dialog window
		shell.close();
		
		// Clean up the images...
		//
		for (Item item : items) {
			item.dispose();
		}
	}

	/**
	 * This is where all the actions are drawn
	 *
	 * @param e
	 */	
	private void onPaint(PaintEvent event) {
		GC gc = event.gc;
		
		// Fill everything with white...
		//
		gc.setForeground(GuiResource.getInstance().getColorBlack());
		gc.setBackground(GuiResource.getInstance().getColorBackground());
		gc.fillRectangle(0, 0, event.width, event.height);
		
		// For text and lines...
		//
		gc.setForeground(GuiResource.getInstance().getColorBlack());
		gc.setLineWidth(4);

		// Draw all actions
		//
		int x = 0;
		int y = 0;

		// How many rows and columns do we have?
		// The canvas width, height and the number of selected actions gives us a clue:
		//
		int nrColumns = calculateNrColumns();
		int nrRows = calculateNrRows();
		if (nrColumns == 0 || nrRows == 0) {
			return;
		}
		
		// So at which row do we start rendering?
		//
		ScrollBar bar = wCanvas.getVerticalBar();		
		int startRow = bar.getSelection();
		int startItem = startRow * nrColumns;

		for (int i = startItem; i < filteredItems.size();i++)  {
			
			if (x + maxNameWidth < event.width) {
				Item item = filteredItems.get(i);
			
			
				org.eclipse.swt.graphics.Point extent = gc.textExtent(item.getText());

				boolean selected = item.equals(selectedItem);
				if (selected) {
					Rectangle selectionBox = new Rectangle(x, y, maxNameWidth, iconSize + margin + maxNameHeight);
					
					gc.setBackground(GuiResource.getInstance().getColorLightBlue());
					gc.fillRectangle(selectionBox);
					gc.drawFocus(selectionBox.x, selectionBox.y, selectionBox.width, selectionBox.height);
				} else {
					gc.setBackground(GuiResource.getInstance().getColorBackground());
				}
								
				gc.drawImage(item.getImage(), x + (maxNameWidth - iconSize) / 2, y);
				gc.drawText(item.getText(), x + (maxNameWidth - extent.x) / 2, y + iconSize + margin-1);
			}
			
			x += cellWidth;

			if (( i+1) % nrColumns==0 ) {
				x = 0;
				y += cellHeight;				
			}
					
			if (y > event.height + 2 * cellHeight) {
				break;
			}
		}
	}

	private int calculateNrColumns() {	
		//System.out.println("Client="+wCanvas.getClientArea() + " bounds="+wCanvas.getBounds());
		return Math.floorDiv(wCanvas.getClientArea().width,cellWidth);		
	}

	private int calculateNrRows() {
		int nrColumns = calculateNrColumns();
		if (nrColumns == 0) {
			return 0;
		}
		
		return (int) Math.ceil( (double) filteredItems.size() / (double) nrColumns );
	}

	private void selectItem(Item item) {
	
		if ( this.selectedItem==item ) return;
		
		this.selectedItem = item;
		
		int nrColumns = calculateNrColumns();
		int index = filteredItems.indexOf(item);		
		
		if (item == null) {
			wlTooltip.setText("");
		} else {
			wlTooltip.setText(Const.NVL(item.getAction().getTooltip(), ""));

			ScrollBar bar = wCanvas.getVerticalBar();	
												
			int row = Math.floorDiv(index,nrColumns);
			
			
	        if ( row >= bar.getSelection()+bar.getPageIncrement() ) {
	          // We scrolled down and need to scroll the scrollbar
	          //
	          bar.setSelection( Math.min(row, bar.getMaximum() ) );
	        }
	        if ( row < bar.getSelection() ) {
	          // We scrolled up and need to scroll the scrollbar up
	          //
	          bar.setSelection( Math.max( row, bar.getMinimum() ) );
	        }			
		}
		
		wCanvas.redraw();		
	}
	
	private void filter(String text) {

		if ( text==null ) text="";
		
		String[] filters = text.split(",");
		for (int i = 0; i < filters.length; i++) {
			filters[i] = Const.trim(filters[i]);
		}

		filteredItems.clear();
		for (Item item : items) {
			GuiAction action = item.getAction();
			
			if (StringUtils.isEmpty(text) || action.containsFilterStrings(filters)) {
				filteredItems.add(item);
			}
		}

		// if selected item is exclude, change to a new default selection: first in the list
		//
		if ( !filteredItems.contains(selectedItem)) {			
			Item item = ( filteredItems.isEmpty() ) ? null:filteredItems.get(0);		
			selectItem(item);
		}
				
		// Update vertical bar
		this.updateVerticalBar();
		
		wCanvas.redraw();
	}

	private void onFocusLost() {
		focusLost = true;
				
		dispose();
	}

	private void onModifySearch() {
		String text = wSearch.getText();
		this.filter(text);				
	}

	private void onKeyPressed(KeyEvent event) {
		
		if (filteredItems.isEmpty()) {
			return;
		}
				
		Rectangle area = wCanvas.getClientArea();
		int pageRows = Math.floorDiv(area.height,cellHeight);
		int nrColumns = calculateNrColumns();
		int nrRows = calculateNrRows();	
		int index = filteredItems.indexOf(selectedItem);
		
		switch( event.keyCode ) {
			case SWT.ARROW_DOWN:
				if ( index+nrColumns<filteredItems.size() ) {
					index += nrColumns;
				}			
				break;
			case SWT.ARROW_UP:
				if ( index-nrColumns>=0 ) {
					index -= nrColumns;
				}
				break;
			case SWT.PAGE_UP:
				if ( index-(pageRows*nrColumns)>0 ) {
					index -= pageRows*nrColumns;
				}			
				else {
					index = Math.floorMod(index,nrColumns);
				}
				break;
			case SWT.PAGE_DOWN:
				if ( index+(pageRows*nrColumns)<filteredItems.size()-1 ) {
					index += pageRows*nrColumns;
				}	
				else {				
					index = (nrRows-1)*nrColumns+Math.floorMod(index,nrColumns);
					if ( index>filteredItems.size()-1 ) {
					index = (nrRows-2)*nrColumns+Math.floorMod(index,nrColumns);
					}
				}
				break;
			case SWT.ARROW_LEFT:
				if ( index>0 ) index--;
				break;
			case SWT.ARROW_RIGHT:
				if ( index<filteredItems.size()-1 ) index++;
				break;
			case SWT.HOME:
				// Position on the first row and column of the screen
				index = 0;
				break;
			case SWT.END:
				// Position on the last row and column of the screen
				index = filteredItems.size()-1;
				break;
		}
		
		selectItem(filteredItems.get(index));
	}
	
	private void updateVerticalBar() {
		ScrollBar verticalBar = wCanvas.getVerticalBar();
		
		int pageRows = Math.floorDiv(wCanvas.getClientArea().height,cellHeight);
		
		verticalBar.setMinimum(0);
		verticalBar.setIncrement(1);
		verticalBar.setPageIncrement(pageRows);
		verticalBar.setMaximum(calculateNrRows());
		verticalBar.setThumb(pageRows);	
	}

	private Item findItem(int x, int y) {
		ScrollBar verticalBar = wCanvas.getVerticalBar();
		
		int startRow = verticalBar.getSelection();	
		int nrColumns = calculateNrColumns();
		int nrRows = calculateNrRows();
		
		int canvasRow = Math.min(Math.floorDiv(y, cellHeight), nrRows) ;
		int canvasColumn = Math.min(Math.floorDiv(x, cellWidth), nrColumns) ;

		int index = startRow * calculateNrColumns() + canvasRow * nrColumns + canvasColumn;			
		Item item = ( index>=0 && index < filteredItems.size() ) ? item = filteredItems.get(index):null;
		
		return item;
	}
	
	public static void main(String[] args) throws Exception {
		Display display = new Display();
		Shell shell = new Shell(display, SWT.MIN | SWT.MAX | SWT.RESIZE);
		// shell.setSize( 500, 500 );
		// shell.open();

		HopClientEnvironment.init();
		PropsUi.init(display);
		HopEnvironment.init();

		List<GuiAction> actions = new ArrayList<>();
		List<IPlugin> transformPlugins = PluginRegistry.getInstance().getPlugins(TransformPluginType.class);
		for (IPlugin transformPlugin : transformPlugins) {
			GuiAction createTransformAction = new GuiAction(
					"pipeline-graph-create-transform-" + transformPlugin.getIds()[0], GuiActionType.Create,
					transformPlugin.getName(), transformPlugin.getDescription(), transformPlugin.getImageFile(),
					(shiftClicked, controlClicked, t) -> System.out.println("Create transform action : "
							+ transformPlugin.getName() + ", shift=" + shiftClicked + ", control=" + controlClicked));
			createTransformAction.getKeywords().add(transformPlugin.getCategory());
			// if (actions.size()<2) {
			actions.add(createTransformAction);
			// }
		}
		ContextDialog dialog = new ContextDialog(shell, "Action test", new Point(50, 50), actions);
		GuiAction action = dialog.open();
		if (action == null) {
			System.out.println("There was no selection in dialog");
		} else {
			System.out.println("Selected action : " + action);
		}

		// Cleanup
		//
		display.dispose();
	}

	/**
	 * Gets shiftClicked
	 *
	 * @return value of shiftClicked
	 */
	public boolean isShiftClicked() {
		return shiftClicked;
	}

	/**
	 * @param shiftClicked The shiftClicked to set
	 */
	public void setShiftClicked(boolean shiftClicked) {
		this.shiftClicked = shiftClicked;
	}

	/**
	 * Gets ctrlClicked
	 *
	 * @return value of ctrlClicked
	 */
	public boolean isCtrlClicked() {
		return ctrlClicked;
	}

	/**
	 * @param ctrlClicked The ctrlClicked to set
	 */
	public void setCtrlClicked(boolean ctrlClicked) {
		this.ctrlClicked = ctrlClicked;
	}

	/**
	 * Gets focusLost
	 *
	 * @return value of focusLost
	 */
	public boolean isFocusLost() {
		return focusLost;
	}

	/**
	 * @param focusLost The focusLost to set
	 */
	public void setFocusLost(boolean focusLost) {
		this.focusLost = focusLost;
	}

}
