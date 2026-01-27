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

//# sourceURL=canvas.js
"use strict";

let x1, x2, y1, y2;
let mouseScreenX = 0, mouseScreenY = 0;  // Track raw screen coordinates for hop drawing
let clicked = null;
let hopStartNode = null;  // Track the start node for hop creation separately
let iconOffsetX = 0, iconOffsetY = 0;  // Track where in the icon the user clicked (like desktop client)
let offsetX, offsetY, magnification, gridSize, showGrid;
let fgColor, bgColor, selectedNodeColor, nodeColor;
let canvasWidth = 0, canvasHeight = 0;  // Store canvas dimensions for mouse wheel zoom calculations
let panStartMouseX = 0, panStartMouseY = 0;  // Mouse position when pan started
let panStartOffsetX = 0, panStartOffsetY = 0;  // Canvas offset when pan started
let panCurrentOffsetX = 0, panCurrentOffsetY = 0;  // Current calculated offset during pan
let panBounds = null;  // Cached pan boundaries
let isPanning = false;  // Track if we're actively panning
let panInitialized = false;  // Track if pan has processed at least one mouse move
let resizeStartMouseX = 0, resizeStartMouseY = 0;  // Mouse position when resize started
let resizedNote = null;  // The note being resized with updated dimensions

// These are the colors for the theme. They are picked up by the next event handler function.
//
function getThemeColors(theme) {
    if (theme === "dark") {
        // Dark colors
        //
        fgColor = 'rgb(210, 210, 210)';
        bgColor = 'rgb(50, 50, 50)';
        selectedNodeColor = 'rgb(0, 93, 166)';
        nodeColor = 'rgb(61, 99, 128)';
    } else {
        // Light theme is the default
        // Use the same background as application: colorBackground RGB(240, 240, 240)
        fgColor = 'rgb(50, 50, 50)';
        bgColor = 'rgb(240, 240, 240)';  // Matches GuiResource.colorBackground
        selectedNodeColor = 'rgb(0, 93, 166)';
        nodeColor = 'rgb(61, 99, 128)';
    }
}


//
const handleEvent = function (event) {
    const mode = event.widget.getData("mode");
    const nodes = event.widget.getData("nodes");
    const hops = event.widget.getData("hops");
    const notes = event.widget.getData("notes");
    const props = event.widget.getData("props");
    const startHopNodeName = event.widget.getData("startHopNode");
    const resizeDirection = event.widget.getData("resizeDirection");
    
    // Get pan data from props (it's sent via JSON)
    const panStartOffset = props ? props.panStartOffset : null;
    const panBoundaries = props ? props.panBoundaries : null;

    // Global vars to make the coordinate calculation function simpler.
    //
    magnification = props.magnification;
    gridSize = props.gridSize;  // Always set for snapping
    showGrid = props.showGrid;  // Control visibility only

    const iconSize = Math.round(props.iconSize * magnification);
    
    // Round offsets to avoid sub-pixel grid misalignment
    // In pan mode, use the calculated offset from MouseMove (only if properly initialized and valid)
    if (mode === "pan" && isPanning && 
        typeof panCurrentOffsetX === 'number' && !isNaN(panCurrentOffsetX) && isFinite(panCurrentOffsetX) &&
        typeof panCurrentOffsetY === 'number' && !isNaN(panCurrentOffsetY) && isFinite(panCurrentOffsetY)) {
        offsetX = panCurrentOffsetX;
        offsetY = panCurrentOffsetY;
    } else {
        // Use server-provided offset (safe fallback)
        offsetX = Math.round(props.offsetX || 0);
        offsetY = Math.round(props.offsetY || 0);
    }
    
    // Synchronize hopStartNode with server data
    if (mode === "hop" && startHopNodeName && nodes[startHopNodeName]) {
        // Always update hopStartNode when we have server data
        hopStartNode = nodes[startHopNodeName];
    } else if (mode !== "hop") {
        // Clear hopStartNode when not in hop mode
        hopStartNode = null;
    }

    switch (event.type) {
        case SWT.MouseWheel:
        case SWT.MouseVerticalWheel:
            // Mouse wheel zoom is handled by CanvasZoomHandler (canvas-zoom.js)
            // Don't process the event here to avoid interference
            break;
        case SWT.MouseDown:
            // Convert screen coordinates to logical coordinates (props might not be set yet, use previous magnification or 1.0)
            const magForDown = props ? props.magnification : (magnification || 1.0);
            x1 = event.x / magForDown;
            y1 = event.y / magForDown;
            
            // Set x2/y2 to current position (needed for hop drawing after mode change)
            x2 = x1;
            y2 = y1;

            // Determine which node is clicked if any
            const iconLogicalSize = (props ? Math.round(props.iconSize * magForDown) : 32) / magForDown;
            for (let key in nodes) {
                const node = nodes[key];
                if (node.x <= x1 && x1 < node.x + iconLogicalSize
                    && node.y <= y1 && y1 < node.y + iconLogicalSize) {
                    clicked = node;
                    
                    // Calculate iconOffset: where within the icon did user click?
                    // This matches desktop client: iconOffset = new Point(real.x - p.x, real.y - p.y)
                    iconOffsetX = x1 - node.x;
                    iconOffsetY = y1 - node.y;
                    break;
                }
            }
            
            // Don't initialize resize in MouseDown - there's a timing issue
            // The mode data arrives after MouseDown, so we'll initialize in MouseMove instead
            break;
        case SWT.MouseUp:
            // Only reset clicked for non-hop modes
            // In hop mode, keep the start node until mode changes
            if (mode !== "hop") {
                clicked = null;
                iconOffsetX = 0;
                iconOffsetY = 0;
            }
            
            // Clear pan state
            if (isPanning) {
                // Uncomment for debugging: console.log("Pan ended - final offset:", panCurrentOffsetX, panCurrentOffsetY);
                isPanning = false;
                panInitialized = false;
                panBounds = null;
                panStartMouseX = 0;
                panStartMouseY = 0;
                panStartOffsetX = 0;
                panStartOffsetY = 0;
                panCurrentOffsetX = 0;
                panCurrentOffsetY = 0;
            }
            
            // Clear resize state
            if (resizedNote) {
                resizedNote = null;
                resizeStartMouseX = 0;
                resizeStartMouseY = 0;
            }
            break;
        case SWT.MouseMove:
            // Store raw screen coordinates for hop drawing
            mouseScreenX = event.x;
            mouseScreenY = event.y;
            
            // Initialize resize state if we're in resize mode and haven't initialized yet
            // (Handle timing issue where mode data arrives after MouseDown)
            if (mode === "resize" && !resizedNote && resizeDirection && notes) {
                resizeStartMouseX = event.x;
                resizeStartMouseY = event.y;
                // Find the selected note
                notes.forEach(function(note) {
                    if (note.selected) {
                        resizedNote = {
                            note: note,
                            direction: resizeDirection,
                            startX: note.x,
                            startY: note.y,
                            startWidth: note.width,
                            startHeight: note.height,
                            currentX: note.x,
                            currentY: note.y,
                            currentWidth: note.width,
                            currentHeight: note.height
                        };
                    }
                });
            }
            
            // Initialize pan state if we're in pan mode and have all required data
            // This can happen on any MouseMove event, not just the first one (handles timing issues)
            if (mode === "pan" && !isPanning && panStartOffset && panBoundaries && props) {
                // Validate all required data before initializing
                if (typeof panStartOffset.x === 'number' && typeof panStartOffset.y === 'number' &&
                    typeof panBoundaries.x === 'number' && typeof panBoundaries.y === 'number' &&
                    typeof panBoundaries.width === 'number' && typeof panBoundaries.height === 'number' &&
                    typeof event.x === 'number' && typeof event.y === 'number' &&
                    typeof props.offsetX === 'number' && typeof props.offsetY === 'number') {
                    // Use current mouse position as the start position (wherever we are when data arrives)
                    isPanning = true;
                    panInitialized = false;  // Not fully initialized until we process first move
                    panStartMouseX = event.x;
                    panStartMouseY = event.y;
                    
                    // IMPORTANT: Use the current server offset (props.offsetX/Y) as the starting point,
                    // not panStartOffset which might be stale due to timing
                    // This prevents the "jump" when pan mode initializes
                    panStartOffsetX = Math.round(props.offsetX);
                    panStartOffsetY = Math.round(props.offsetY);
                    panCurrentOffsetX = panStartOffsetX;
                    panCurrentOffsetY = panStartOffsetY;
                    panBounds = {
                        x: panBoundaries.x,
                        y: panBoundaries.y,
                        width: panBoundaries.width,
                        height: panBoundaries.height
                    };
                }
            }
            
            // Handle pan mode - calculate offset in real-time
            if (mode === "pan" && isPanning && panBounds && props && typeof panStartOffsetX === 'number') {
                // Get magnification from props (it's not set as global var yet)
                const mag = props.magnification;
                
                // Validate we have all required data before calculating
                if (mag && mag > 0 && typeof panStartMouseX === 'number' && typeof panStartMouseY === 'number') {
                    // If mouse start position is still 0 (initialized from Paint), set it now
                    if (panStartMouseX === 0 && panStartMouseY === 0) {
                        panStartMouseX = event.x;
                        panStartMouseY = event.y;
                        panInitialized = false;  // Reset since we just set the start position
                    }
                    
                    // On the very first MouseMove after initialization, just update the reference point
                    // without calculating any delta - this prevents the initial jump
                    if (!panInitialized) {
                        panInitialized = true;
                        panStartMouseX = event.x;
                        panStartMouseY = event.y;
                        // Keep the current offset as-is, don't calculate delta yet
                        x2 = event.x / mag;
                        y2 = event.y / mag;
                    } else {
                        // Normal pan calculation after first move
                        // Calculate the zoom factor (matching server-side calculation)
                        const nativeZoomFactor = 1.0;
                        const zoomFactor = nativeZoomFactor * Math.max(0.1, mag);
                        
                        // Validate event coordinates are numbers
                        const currentMouseX = (typeof event.x === 'number' && !isNaN(event.x)) ? event.x : panStartMouseX;
                        const currentMouseY = (typeof event.y === 'number' && !isNaN(event.y)) ? event.y : panStartMouseY;
                        
                        // Calculate delta from pan start position
                        const deltaX = (panStartMouseX - currentMouseX) / zoomFactor;
                        const deltaY = (panStartMouseY - currentMouseY) / zoomFactor;
                    
                        // Validate deltas are finite numbers
                        if (!isFinite(deltaX) || !isFinite(deltaY)) {
                            // Invalid delta, skip this update
                            x2 = event.x / mag;
                            y2 = event.y / mag;
                        } else {
                            // Apply delta to starting offset
                            let newOffsetX = panStartOffsetX - deltaX;
                            let newOffsetY = panStartOffsetY - deltaY;
                            
                            // Validate bounds exist and are valid
                            if (typeof panBounds.x === 'number' && typeof panBounds.width === 'number' &&
                                typeof panBounds.y === 'number' && typeof panBounds.height === 'number') {
                                // Apply boundary constraints
                                if (newOffsetX < panBounds.x) {
                                    newOffsetX = panBounds.x;
                                }
                                if (newOffsetX > panBounds.width) {
                                    newOffsetX = panBounds.width;
                                }
                                if (newOffsetY < panBounds.y) {
                                    newOffsetY = panBounds.y;
                                }
                                if (newOffsetY > panBounds.height) {
                                    newOffsetY = panBounds.height;
                                }
                            }
                            
                            // Store the calculated offset for Paint to use (only if finite)
                            if (isFinite(newOffsetX) && isFinite(newOffsetY)) {
                                panCurrentOffsetX = Math.round(newOffsetX);
                                panCurrentOffsetY = Math.round(newOffsetY);
                            }
                            
                            x2 = currentMouseX / mag;
                            y2 = currentMouseY / mag;
                        }
                    }
                } else {
                    // Data not ready yet, use fallback
                    x2 = event.x / (props ? props.magnification : 1.0);
                    y2 = event.y / (props ? props.magnification : 1.0);
                }
            } else if (mode === "resize" && resizedNote && resizedNote.direction) {
                // Handle resize mode - calculate new dimensions
                const mag = props ? props.magnification : 1.0;
                const deltaX = (event.x - resizeStartMouseX) / mag;
                const deltaY = (event.y - resizeStartMouseY) / mag;
                const minWidth = 100;
                const minHeight = 50;
                
                // Calculate new position and size based on resize direction
                // Always reset position to start values first, then adjust as needed
                resizedNote.currentX = resizedNote.startX;
                resizedNote.currentY = resizedNote.startY;
                resizedNote.currentWidth = resizedNote.startWidth;
                resizedNote.currentHeight = resizedNote.startHeight;
                
                switch (resizedNote.direction) {
                    case "EAST":
                        resizedNote.currentWidth = Math.max(minWidth, resizedNote.startWidth + deltaX);
                        break;
                    case "WEST":
                        const newWidthW = Math.max(minWidth, resizedNote.startWidth - deltaX);
                        resizedNote.currentX = resizedNote.startX + (resizedNote.startWidth - newWidthW);
                        resizedNote.currentWidth = newWidthW;
                        break;
                    case "SOUTH":
                        resizedNote.currentHeight = Math.max(minHeight, resizedNote.startHeight + deltaY);
                        break;
                    case "NORTH":
                        const newHeightN = Math.max(minHeight, resizedNote.startHeight - deltaY);
                        resizedNote.currentY = resizedNote.startY + (resizedNote.startHeight - newHeightN);
                        resizedNote.currentHeight = newHeightN;
                        break;
                    case "SOUTH_EAST":
                        resizedNote.currentWidth = Math.max(minWidth, resizedNote.startWidth + deltaX);
                        resizedNote.currentHeight = Math.max(minHeight, resizedNote.startHeight + deltaY);
                        break;
                    case "SOUTH_WEST":
                        const newWidthSW = Math.max(minWidth, resizedNote.startWidth - deltaX);
                        resizedNote.currentX = resizedNote.startX + (resizedNote.startWidth - newWidthSW);
                        resizedNote.currentWidth = newWidthSW;
                        resizedNote.currentHeight = Math.max(minHeight, resizedNote.startHeight + deltaY);
                        break;
                    case "NORTH_EAST":
                        resizedNote.currentWidth = Math.max(minWidth, resizedNote.startWidth + deltaX);
                        const newHeightNE = Math.max(minHeight, resizedNote.startHeight - deltaY);
                        resizedNote.currentY = resizedNote.startY + (resizedNote.startHeight - newHeightNE);
                        resizedNote.currentHeight = newHeightNE;
                        break;
                    case "NORTH_WEST":
                        const newWidthNW = Math.max(minWidth, resizedNote.startWidth - deltaX);
                        const newHeightNW = Math.max(minHeight, resizedNote.startHeight - deltaY);
                        resizedNote.currentX = resizedNote.startX + (resizedNote.startWidth - newWidthNW);
                        resizedNote.currentY = resizedNote.startY + (resizedNote.startHeight - newHeightNW);
                        resizedNote.currentWidth = newWidthNW;
                        resizedNote.currentHeight = newHeightNW;
                        break;
                }
                
                x2 = event.x / mag;
                y2 = event.y / mag;
            } else {
                x2 = event.x / (props ? props.magnification : 1.0);
                y2 = event.y / (props ? props.magnification : 1.0);
            }
            
            if (mode == null) {
                break;
            }
            if (mode !== "null") {
                event.widget.redraw();
            }
            break;
        case SWT.Paint:
            // Initialize pan state from Paint event if MouseMove hasn't done it yet
            // This handles the case where Paint arrives with data before MouseMove
            if (mode === "pan" && !isPanning && panStartOffset && panBoundaries && props) {
                // Validate panStartOffset and panBoundaries have valid values
                if (typeof panStartOffset.x === 'number' && typeof panStartOffset.y === 'number' &&
                    typeof panBoundaries.x === 'number' && typeof panBoundaries.y === 'number' &&
                    typeof panBoundaries.width === 'number' && typeof panBoundaries.height === 'number' &&
                    typeof props.offsetX === 'number' && typeof props.offsetY === 'number') {
                    // We don't have mouse position in Paint, so we'll initialize with offset only
                    // The first MouseMove will set the mouse position
                    isPanning = true;
                    panInitialized = false;  // Not fully initialized until we process first move
                    panStartMouseX = 0; // Will be set on first MouseMove
                    panStartMouseY = 0;
                    
                    // Use current server offset to prevent jump
                    panStartOffsetX = Math.round(props.offsetX);
                    panStartOffsetY = Math.round(props.offsetY);
                    panCurrentOffsetX = panStartOffsetX;
                    panCurrentOffsetY = panStartOffsetY;
                    panBounds = {
                        x: panBoundaries.x,
                        y: panBoundaries.y,
                        width: panBoundaries.width,
                        height: panBoundaries.height
                    };
                }
            }
            
            // Client-side does not redraw when first-drawing (null) and after mouseup ("null")
            if (mode == null || mode === "null") {
                break;
            }

            getThemeColors(props.themeId);

            const gc = event.gc;
            
            // Store canvas dimensions
            if (gc && gc.canvas) {
                canvasWidth = gc.canvas.width;
                canvasHeight = gc.canvas.height;
            }
            
            // Calculate delta matching desktop client logic:
            // Desktop: icon.x = real.x - iconOffset.x  (where icon top-left should be)
            //          dx = icon.x - selectedTransform.getLocation().x
            // Web equivalent:
            const iconTargetX = x2 - iconOffsetX;  // Where icon top-left should be
            const iconTargetY = y2 - iconOffsetY;
            const iconStartX = x1 - iconOffsetX;   // Where icon top-left was at start
            const iconStartY = y1 - iconOffsetY;
            const dx = iconTargetX - iconStartX;   // Delta for icon top-left
            const dy = iconTargetY - iconStartY;

            // Set the appropriate font size.
            //
            let fontString = gc.font;
            let pxIdx = fontString.indexOf('px')
            let oldSize = fontString.substring(0, pxIdx);
            let newSize = Math.round(oldSize * magnification);
            gc.font = newSize + fontString.substring(pxIdx);

            // Fill canvas with solid background color matching the application
            // This creates the overlay effect during drag/hop operations
            gc.fillStyle = bgColor;
            gc.fillRect(0, 0, gc.canvas.width, gc.canvas.height);

            // Draw grids (only if showGrid is enabled)
            //
            if (showGrid && gridSize > 1) {
                drawGrid(gc, gridSize);
            }

            // Draw existing hops (skip in hop mode for cleaner view)
            if (mode !== "hop") {
                drawHops(hops, gc, mode, nodes, dx, iconSize, dy);
            }

            // Always draw node outlines so users can see what they're targeting
            drawNodes(nodes, mode, dx, dy, gc, iconSize);

            // Draw notes
            drawNotes(notes, gc, mode, dx, dy);

            // Draw a new hop candidate line (matching desktop client style)
            if (mode === "hop" && hopStartNode) {
                const startX = Math.round(fx(hopStartNode.x) + iconSize / 2);
                const startY = Math.round(fy(hopStartNode.y) + iconSize / 2);
                
                // Use raw screen coordinates for the mouse position (not transformed)
                const endX = mouseScreenX;
                const endY = mouseScreenY;
                
                // Draw solid blue line (matching desktop client)
                gc.strokeStyle = 'rgb(0, 93, 166)';  // Blue color
                gc.lineWidth = 2;
                gc.beginPath();
                gc.moveTo(startX, startY);
                gc.lineTo(endX, endY);
                gc.stroke();
                
                // Draw arrow head at the end
                const angle = Math.atan2(endY - startY, endX - startX);
                const arrowLength = 10;
                const arrowWidth = 5;
                
                gc.fillStyle = 'rgb(0, 93, 166)';
                gc.beginPath();
                gc.moveTo(endX, endY);
                gc.lineTo(
                    Math.round(endX - arrowLength * Math.cos(angle - Math.PI / 6)),
                    Math.round(endY - arrowLength * Math.sin(angle - Math.PI / 6))
                );
                gc.lineTo(
                    Math.round(endX - arrowLength * Math.cos(angle + Math.PI / 6)),
                    Math.round(endY - arrowLength * Math.sin(angle + Math.PI / 6))
                );
                gc.closePath();
                gc.fill();
                
                // Reset
                gc.lineWidth = 1;
            }

            // Draw a selection rectangle
            if (mode === "select") {
                gc.beginPath();
                let rx = x1;
                let ry = y1;
                let rw = Math.abs(dx);
                let rh = Math.abs(dy);
                if (dx < 0) {
                    rx += dx;
                }
                if (dy < 0) {
                    ry += dy;
                }
                gc.setLineDash([5, 15]);
                gc.rect(fx(rx), fy(ry), rw * magnification, rh * magnification);
                gc.stroke();
                gc.setLineDash([]);
            }

            // Put the font right back where it was.
            //
            gc.font = fontString;
            break;
    }
};

// This gets called by the RAP code for the Canvas widget.
function drawGrid(gc, gridSize) {
    gc.fillStyle = fgColor;
    gc.globalAlpha = 0.3;  // Make grid more subtle
    
    // Calculate visible area bounds
    const width = gc.canvas.width;
    const height = gc.canvas.height;
    
    // Calculate the spacing in screen coordinates (rounded for pixel-perfect alignment)
    const spacing = Math.round(gridSize * magnification);
    
    // Find where the first snapped logical grid coordinate (0) appears on screen
    // We draw grid dots at screen positions that correspond to snapped logical coordinates
    const originX = Math.round(fx(0));
    const originY = Math.round(fy(0));
    
    // Use proper modulo that handles negatives correctly for finding first visible grid dot
    // JavaScript % can return negative values, so we need ((n % m) + m) % m
    const firstX = ((originX % spacing) + spacing) % spacing;
    const firstY = ((originY % spacing) + spacing) % spacing;
    
    // Draw dots at grid intersections (matching desktop client)
    // Desktop uses: gc.drawPoint() which draws using current lineWidth (typically 2px)
    // We'll draw 2x2 pixel dots to match the desktop appearance
    const dotSize = 2;
    for (let x = firstX; x <= width; x += spacing) {
        for (let y = firstY; y <= height; y += spacing) {
            gc.fillRect(x, y, dotSize, dotSize);
        }
    }
    
    gc.globalAlpha = 1.0;  // Reset alpha
}

function drawHops(hops, gc, mode, nodes, dx, iconSize, dy) {
    // Set consistent stroke style for all hops
    gc.strokeStyle = fgColor;
    gc.lineWidth = 1;
    
    hops.forEach(function (hop) {
        // Validate that both nodes exist before attempting to draw
        const fromNode = nodes[hop.from];
        const toNode = nodes[hop.to];
        
        if (!fromNode || !toNode) {
            // Skip this hop if either node is missing
            console.warn("Skipping hop with missing node - from:", hop.from, "to:", hop.to);
            return;
        }
        
        gc.beginPath();
        
        // Calculate from coordinates (with client-side snapping when configured)
        let fromX, fromY;
        if (mode === "drag" && fromNode.selected) {
            let x = fromNode.x + dx;
            let y = fromNode.y + dy;
            if (gridSize > 1) {
                x = snapToGrid(x);
                y = snapToGrid(y);
            }
            fromX = Math.round(fx(x) + iconSize / 2);
            fromY = Math.round(fy(y) + iconSize / 2);
        } else {
            fromX = Math.round(fx(fromNode.x) + iconSize / 2);
            fromY = Math.round(fy(fromNode.y) + iconSize / 2);
        }
        
        // Calculate to coordinates (with client-side snapping when configured)
        let toX, toY;
        if (mode === "drag" && toNode.selected) {
            let x = toNode.x + dx;
            let y = toNode.y + dy;
            if (gridSize > 1) {
                x = snapToGrid(x);
                y = snapToGrid(y);
            }
            toX = Math.round(fx(x) + iconSize / 2);
            toY = Math.round(fy(y) + iconSize / 2);
        } else {
            toX = Math.round(fx(toNode.x) + iconSize / 2);
            toY = Math.round(fy(toNode.y) + iconSize / 2);
        }
        
        gc.moveTo(fromX, fromY);
        gc.lineTo(toX, toY);
        gc.stroke();
    });
}

function drawNodes(nodes, mode, dx, dy, gc, iconSize) {
    for (let nodeName in nodes) {
        const node = nodes[nodeName];
        let x = node.x;
        let y = node.y;

        // Move selected nodes with client-side snapping
        //
        if (mode === "drag" && (node.selected || node === clicked)) {
            const origX = node.x;
            const origY = node.y;
            x = node.x + dx;
            y = node.y + dy;
            
            // Apply snap-to-grid during dragging (when grid size is configured)
            if (gridSize > 1) {
                x = snapToGrid(x);
                y = snapToGrid(y);
            }
        }
        
        // Skip drawing backgrounds - keep transparent so SVG icons show through
        // Only draw the bounding rectangle outline
        //
        if (node.selected || node === clicked) {
            gc.lineWidth = 3;
            gc.strokeStyle = selectedNodeColor;
        } else {
            gc.lineWidth = 1;
            gc.strokeStyle = nodeColor;
        }
        // Use rounded screen coordinates for pixel-perfect rendering
        drawRoundRectangle(gc, Math.round(fx(x - 1)), Math.round(fy(y - 1)), iconSize + 1, iconSize + 1, 8, 8, false);
        gc.strokeStyle = fgColor;
        gc.lineWidth = 1;

        // Draw node name
        //
        gc.fillStyle = fgColor;

        // Calculate the font size and magnify it as well.
        //
        gc.fillText(nodeName,
            Math.round(fx(x) + iconSize / 2 - gc.measureText(nodeName).width / 2),
            Math.round(fy(y) + iconSize + 7));
    }
}

function drawNotes(notes, gc, mode, dx, dy) {
    notes.forEach(function (note) {
        let noteX = note.x;
        let noteY = note.y;
        let noteWidth = note.width;
        let noteHeight = note.height;
        
        // Handle drag mode - apply client-side offset preview
        if (mode === "drag" && note.selected) {
            noteX = note.x + dx;
            noteY = note.y + dy;
        }
        // Handle resize mode - use client-calculated dimensions
        // Match by selected state since note object gets recreated on server updates
        else if (mode === "resize" && note.selected && resizedNote) {
            noteX = resizedNote.currentX;
            noteY = resizedNote.currentY;
            noteWidth = resizedNote.currentWidth;
            noteHeight = resizedNote.currentHeight;
        }
        
        // Draw note rectangle
        // Note: Width and height need to be scaled by magnification, just like position
        const screenW = Math.round((noteWidth + 10) * magnification);
        const screenH = Math.round((noteHeight + 10) * magnification);
        
        const screenX = Math.round(fx(noteX));
        const screenY = Math.round(fy(noteY));
        
        gc.beginPath();
        gc.rect(screenX, screenY, screenW, screenH);
        gc.stroke();
        
        // Draw resize handles on selected notes (always visible, not just in resize mode)
        // But skip when dragging
        if (note.selected && mode !== "drag" && mode !== "pan" && mode !== "hop") {
            const handleSize = 8;
            // Use the same screen coordinates we used for drawing the rectangle
            const handleScreenX = screenX;
            const handleScreenY = screenY;
            const handleScreenW = screenW;
            const handleScreenH = screenH;
            
            // Save current style
            const savedStrokeStyle = gc.strokeStyle;
            const savedFillStyle = gc.fillStyle;
            
            // Set handle style
            gc.fillStyle = 'rgb(0, 93, 166)';  // Blue handles
            gc.strokeStyle = 'rgb(255, 255, 255)';  // White border
            gc.lineWidth = 1;
            
            // Draw 8 resize handles (corners and edges)
            const handles = [
                { x: handleScreenX - handleSize/2, y: handleScreenY - handleSize/2 },  // NW
                { x: handleScreenX + handleScreenW/2 - handleSize/2, y: handleScreenY - handleSize/2 },  // N
                { x: handleScreenX + handleScreenW - handleSize/2, y: handleScreenY - handleSize/2 },  // NE
                { x: handleScreenX + handleScreenW - handleSize/2, y: handleScreenY + handleScreenH/2 - handleSize/2 },  // E
                { x: handleScreenX + handleScreenW - handleSize/2, y: handleScreenY + handleScreenH - handleSize/2 },  // SE
                { x: handleScreenX + handleScreenW/2 - handleSize/2, y: handleScreenY + handleScreenH - handleSize/2 },  // S
                { x: handleScreenX - handleSize/2, y: handleScreenY + handleScreenH - handleSize/2 },  // SW
                { x: handleScreenX - handleSize/2, y: handleScreenY + handleScreenH/2 - handleSize/2 }   // W
            ];
            
            handles.forEach(function(handle) {
                gc.fillRect(handle.x, handle.y, handleSize, handleSize);
                gc.strokeRect(handle.x, handle.y, handleSize, handleSize);
            });
            
            // Restore styles
            gc.strokeStyle = savedStrokeStyle;
            gc.fillStyle = savedFillStyle;
        }
    });
}

function fx(x) {
    // Don't clamp negative values - allow drawing outside visible area
    const result = (x + offsetX) * magnification + offsetX;
    return result;
}

function fy(y) {
    // Don't clamp negative values - allow drawing outside visible area
    const result = (y + offsetY) * magnification + offsetY;
    return result;
}

function snapToGrid(x) {
    // Match Java behavior: integer division first, then multiply
    // Java: gridSize * (int) Math.round((float) (p.x / gridSize))
    // where (p.x / gridSize) is integer division in Java
    
    // JavaScript equivalent: Math.trunc() truncates towards zero (like Java int division)
    const gridCell = Math.trunc(x / gridSize);
    const snapped = Math.round(gridSize * gridCell);
    
    return snapped;
}

/*
 * Port of GCOperationWriter#drawRoundRectangle
 */
function drawRoundRectangle(gc, x, y, w, h, arcWidth, arcHeight, fill) {
    let offset = 0;
    if (!fill && gc.lineWidth % 2 !== 0) {
        offset = 0.5;
    }
    x = x + offset;
    y = y + offset;
    const rx = arcWidth / 2 + 1;
    const ry = arcHeight / 2 + 1;
    gc.beginPath();
    gc.moveTo(x, y + ry);
    gc.lineTo(x, y + h - ry);
    gc.quadraticCurveTo(x, y + h, x + rx, y + h);
    gc.lineTo(x + w - rx, y + h);
    gc.quadraticCurveTo(x + w, y + h, x + w, y + h - ry);
    gc.lineTo(x + w, y + ry);
    gc.quadraticCurveTo(x + w, y, x + w - rx, y);
    gc.lineTo(x + rx, y);
    gc.quadraticCurveTo(x, y, x, y + ry);
    if (fill) {
        gc.fill();
    } else {
        gc.stroke();
    }
}