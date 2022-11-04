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
let offsetX, offsetY, magnification, gridSize;
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
        //
        fgColor = 'rgb(50, 50, 50)';
        bgColor = 'rgb(210, 210, 210)';
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

    // Global vars to make the coordinate calculation function simpler.
    //
    offsetX = props.offsetX;
    offsetY = props.offsetY;
    magnification = props.magnification;

    const gridSize = props.gridSize;
    const iconSize = props.iconSize * magnification;

    switch (event.type) {
        case SWT.MouseDown:
            x1 = event.x / magnification;
            y1 = event.y / magnification;

            // Determine which node is clicked if any
            for (let key in nodes) {
                const node = nodes[key];
                if (node.x <= x1 && x1 < node.x + iconSize
                    && node.y <= y1 && y1 < node.y + iconSize) {
                    clicked = node;
                    break;
                }
            }
            break;
        case SWT.MouseUp:
            clicked = null;
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
            const dx = x2 - x1;
            const dy = y2 - y1;

            // Set the appropriate font size.
            //
            let fontString = gc.font;
            let pxIdx = fontString.indexOf('px')
            let oldSize = fontString.substring(0, pxIdx);
            let newSize = Math.round(oldSize * magnification);
            gc.font = newSize + fontString.substring(pxIdx);

            // If we're not dragging the cursor with the mouse
            //
            // Clear the canvas, regardless of what happens below
            //
            gc.rect(0, 0, gc.canvas.width / magnification, gc.canvas.height / magnification);
            gc.fillStyle = bgColor;
            gc.fill();

            // Draw grids
            //
            if (gridSize > 1) {
                drawGrid(gc, gridSize);
            }

            // Draw hops
            drawHops(hops, gc, mode, nodes, dx, iconSize, dy);

            // The nodes are action or transform icons
            //
            drawNodes(nodes, mode, dx, dy, gc, iconSize);

            // Draw notes
            drawNotes(notes, gc, mode, dx, dy);

            // Draw a new hop
            if (mode === "hop" && clicked) {
                gc.beginPath();
                gc.moveTo(
                    fx(clicked.x) + iconSize / 2,
                    fy(clicked.y) + iconSize / 2);
                gc.lineTo(fx(x2), fy(y2));
                gc.stroke();
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
    gc.beginPath();
    gc.setLineDash([1, gridSize - 1]);
    // vertical grid
    for (let i = gridSize; i < gc.canvas.width / magnification; i += gridSize) {
        gc.moveTo(fx(i), fy(0));
        gc.lineTo(fx(i), fy(gc.canvas.height / magnification));
    }
    // horizontal grid
    for (let j = gridSize; j < gc.canvas.height / magnification; j += gridSize) {
        gc.moveTo(fx(0), fy(j));
        gc.lineTo(fx(gc.canvas.width / magnification), fy(j));
    }
    gc.stroke();
    gc.setLineDash([]);
    gc.fillStyle = bgColor;
}

function drawHops(hops, gc, mode, nodes, dx, iconSize, dy) {
    hops.forEach(function (hop) {
        gc.beginPath();
        if (mode === "drag" && nodes[hop.from].selected) {
            gc.moveTo(
                fx(nodes[hop.from].x + dx) + iconSize / 2,
                fy(nodes[hop.from].y + dy) + iconSize / 2);
        } else {
            gc.moveTo(
                fx(nodes[hop.from].x) + iconSize / 2,
                fy(nodes[hop.from].y) + iconSize / 2);
        }
        if (mode === "drag" && nodes[hop.to].selected) {
            gc.lineTo(
                fx(nodes[hop.to].x + dx) + iconSize / 2,
                fy(nodes[hop.to].y + dy) + iconSize / 2);
        } else {
            gc.lineTo(
                fx(nodes[hop.to].x) + iconSize / 2,
                fy(nodes[hop.to].y) + iconSize / 2);
        }
        gc.stroke();
    });
}

function drawNodes(nodes, mode, dx, dy, gc, iconSize) {
    for (let nodeName in nodes) {
        const node = nodes[nodeName];
        let x = node.x;
        let y = node.y;

        // Move selected nodes
        //
        if (mode === "drag" && (node.selected || node === clicked)) {
            x = node.x + dx;
            y = node.y + dy;
        }
        // Draw the icon background
        //
        gc.rect(fx(x), fy(y), iconSize, iconSize);
        gc.fillStyle = bgColor;
        gc.fill();

        // Draw a bounding rectangle
        //
        if (node.selected || node === clicked) {
            gc.lineWidth = 3;
            gc.strokeStyle = selectedNodeColor;
        } else {
            gc.strokeStyle = nodeColor; //colorCrystalText
        }
        drawRoundRectangle(gc, fx(x - 1), fy(y - 1), iconSize + 1, iconSize + 1, 8, 8, false);
        gc.strokeStyle = fgColor;
        gc.lineWidth = 1;

        // Draw node name
        //
        gc.fillStyle = fgColor;

        // Calculate the font size and magnify it as well.
        //
        gc.fillText(nodeName,
            fx(x) + iconSize / 2 - gc.measureText(nodeName).width / 2,
            fy(y) + iconSize + 7);
        gc.fillStyle = bgColor;
    }
}

function drawNotes(notes, gc, mode, dx, dy) {
    notes.forEach(function (note) {
        gc.beginPath();
        if (mode === "drag" && note.selected) {
            gc.rect(
                fx(note.x + dx),
                fy(note.y + dy),
                note.width + 10, note.height + 10);
        } else {
            gc.rect(
                fx(note.x),
                fy(note.y),
                note.width + 10, note.height + 10);
        }
        gc.stroke();
    });
}

function fx(x) {
    if (x<0) {
        return 0;
    }
    return (x + offsetX) * magnification + offsetX;
}

function fy(y) {
    if (y<0) {
        return 0;
    }
    return (y + offsetY) * magnification + offsetY;
}

function snapToGrid(x) {
    return gridSize * Math.floor(x / gridSize);
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