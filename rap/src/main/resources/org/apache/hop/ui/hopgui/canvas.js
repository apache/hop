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
let clicked = null;
let hopStartNode = null;  // Track the start node for hop creation separately
let iconOffsetX = 0, iconOffsetY = 0;  // Track where in the icon the user clicked (like desktop client)
let offsetX, offsetY, magnification, gridSize, showGrid;
let fgColor, bgColor, selectedNodeColor, nodeColor;

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

    // Global vars to make the coordinate calculation function simpler.
    //
    // Round offsets to avoid sub-pixel grid misalignment
    offsetX = Math.round(props.offsetX);
    offsetY = Math.round(props.offsetY);
    magnification = props.magnification;
    gridSize = props.gridSize;  // Always set for snapping
    showGrid = props.showGrid;  // Control visibility only

    const iconSize = Math.round(props.iconSize * magnification);
    
    // Synchronize hopStartNode with server data
    if (mode === "hop" && startHopNodeName && nodes[startHopNodeName]) {
        // Always update hopStartNode when we have server data
        hopStartNode = nodes[startHopNodeName];
    } else if (mode !== "hop") {
        // Clear hopStartNode when not in hop mode
        hopStartNode = null;
    }

    switch (event.type) {
        case SWT.MouseDown:
            // Convert screen coordinates to logical coordinates
            x1 = event.x / magnification;
            y1 = event.y / magnification;
            
            // Set x2/y2 to current position (needed for hop drawing after mode change)
            x2 = x1;
            y2 = y1;

            // Determine which node is clicked if any
            const iconLogicalSize = iconSize / magnification;
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
            break;
        case SWT.MouseUp:
            // Only reset clicked for non-hop modes
            // In hop mode, keep the start node until mode changes
            if (mode !== "hop") {
                clicked = null;
                iconOffsetX = 0;
                iconOffsetY = 0;
            }
            break;
        case SWT.MouseMove:
            x2 = event.x / magnification;
            y2 = event.y / magnification;
            if (mode == null) {
                break;
            }
            if (mode !== "null") {
                event.widget.redraw();
            }
            break;
        case SWT.Paint:
            // Client-side does not redraw when first-drawing (null) and after mouseup ("null")
            if (mode == null || mode === "null") {
                break;
            }

            getThemeColors(props.themeId);

            const gc = event.gc;
            
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
                // Use current mouse position, or fall back to x2/y2
                const targetX = x2 !== undefined ? x2 : x1;
                const targetY = y2 !== undefined ? y2 : y1;
                
                const startX = Math.round(fx(hopStartNode.x) + iconSize / 2);
                const startY = Math.round(fy(hopStartNode.y) + iconSize / 2);
                const endX = Math.round(fx(targetX));
                const endY = Math.round(fy(targetY));
                
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
        gc.beginPath();
        if (mode === "drag" && note.selected) {
            gc.rect(
                Math.round(fx(note.x + dx)),
                Math.round(fy(note.y + dy)),
                note.width + 10, note.height + 10);
        } else {
            gc.rect(
                Math.round(fx(note.x)),
                Math.round(fy(note.y)),
                note.width + 10, note.height + 10);
        }
        gc.stroke();
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