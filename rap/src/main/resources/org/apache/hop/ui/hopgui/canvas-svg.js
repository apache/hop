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

//# sourceURL=canvas-svg.js
(function () {
    "use strict";

    if (!window.hop) {
        window.hop = {};
    }

    var serviceBasePath = (function () {
        var path = window.location.pathname;
        if (path.indexOf("/ui-dark") >= 0) {
            return "/ui-dark";
        }
        if (path.indexOf("/ui") >= 0) {
            return "/ui";
        }
        return "";
    })();

    function graphCoords(screenX, screenY, props) {
        var mag = props.magnification || 1.0;
        var offsetX = props.offsetX || 0;
        var offsetY = props.offsetY || 0;
        return {
            x: Math.round(screenX / mag - offsetX),
            y: Math.round(screenY / mag - offsetY)
        };
    }

    function findCanvasForWidget(canvasId) {
        if (canvasId) {
            var widgetElement = document.getElementById(canvasId);
            if (widgetElement) {
                if (widgetElement.tagName === "CANVAS") {
                    return widgetElement;
                }
                var nestedCanvas = widgetElement.querySelector("canvas");
                if (nestedCanvas) {
                    return nestedCanvas;
                }
            }
        }
        return findVisibleGraphCanvas();
    }

    function findVisibleGraphCanvas() {
        var allCanvases = document.querySelectorAll("canvas");
        for (var i = 0; i < allCanvases.length; i++) {
            var c = allCanvases[i];
            var rect = c.getBoundingClientRect();
            if (rect.width > 500 && rect.height > 500 && c.offsetParent !== null) {
                return c;
            }
        }
        return null;
    }

    function buildServiceHandlerUrl(serviceId) {
        if (typeof rwt !== "undefined" && rwt.remote && rwt.remote.Connection) {
            var connection = rwt.remote.Connection.getInstance();
            return connection.getUrl()
                + "?servicehandler=" + encodeURIComponent(serviceId)
                + "&cid=" + encodeURIComponent(connection.getConnectionId());
        }
        return serviceBasePath + "?servicehandler=" + encodeURIComponent(serviceId);
    }

    function getVisibleArea(areas, graphX, graphY) {
        if (!areas || areas.length === 0) {
            return null;
        }
        for (var i = areas.length - 1; i >= 0; i--) {
            var a = areas[i];
            if (graphX >= a.x && graphX < a.x + a.width &&
                graphY >= a.y && graphY < a.y + a.height) {
                return a;
            }
        }
        return null;
    }

    function snapToGrid(value, gridSize) {
        if (gridSize > 1) {
            return Math.round(value / gridSize) * gridSize;
        }
        return value;
    }

    function graphRectToScreen(graphX, graphY, graphW, graphH, props, offsetX, offsetY) {
        var mag = props.magnification || 1.0;
        var ox = offsetX != null ? offsetX : (props.offsetX || 0);
        var oy = offsetY != null ? offsetY : (props.offsetY || 0);
        return {
            left: (graphX + ox) * mag,
            top: (graphY + oy) * mag,
            width: graphW * mag,
            height: graphH * mag
        };
    }

    function extendGraphBounds(bounds, x, y, w, h) {
        bounds.minX = Math.min(bounds.minX, x);
        bounds.minY = Math.min(bounds.minY, y);
        bounds.maxX = Math.max(bounds.maxX, x + w);
        bounds.maxY = Math.max(bounds.maxY, y + h);
        return bounds;
    }

    function findIconAreaAt(areas, graphX, graphY) {
        if (!areas || areas.length === 0) {
            return null;
        }
        for (var i = areas.length - 1; i >= 0; i--) {
            var area = areas[i];
            if (area.areaType !== "TRANSFORM_ICON" && area.areaType !== "ACTION_ICON") {
                continue;
            }
            if (graphX >= area.x && graphX < area.x + area.width &&
                graphY >= area.y && graphY < area.y + area.height) {
                return area;
            }
        }
        return null;
    }

    function iconOwnerName(area) {
        if (!area || !area.owner) {
            return null;
        }
        if (area.owner.kind === "transform" || area.owner.kind === "action") {
            return area.owner.name;
        }
        // Plugin model graphs (Data Vault, etc.) register TRANSFORM_ICON areas with a
        // string owner serialized as kind "label" / value.
        if (area.owner.kind === "label" && area.owner.value) {
            return area.owner.value;
        }
        if (area.owner.name) {
            return area.owner.name;
        }
        return null;
    }

    /** Logical width/height for a node; falls back to iconSize for pipeline icons. */
    function nodeSize(node, props, area) {
        var fallback = (props && props.iconSize) ? props.iconSize : 32;
        var w = fallback;
        var h = fallback;
        if (node && node.width > 0) {
            w = node.width;
        } else if (area && area.width > 0) {
            w = area.width;
        }
        if (node && node.height > 0) {
            h = node.height;
        } else if (area && area.height > 0) {
            h = area.height;
        }
        return { width: w, height: h };
    }

    /**
     * Hit-test nodes map (logical graph coords). Used when area list is empty/stale so plugin
     * model cards still get client drag previews on the SVG effects layer.
     */
    function findNodeAt(nodes, graphX, graphY, props) {
        if (!nodes) {
            return null;
        }
        var fallback = (props && props.iconSize) ? props.iconSize : 32;
        var best = null;
        for (var name in nodes) {
            if (!nodes.hasOwnProperty(name)) {
                continue;
            }
            var n = nodes[name];
            if (!n) {
                continue;
            }
            var w = n.width > 0 ? n.width : fallback;
            var h = n.height > 0 ? n.height : fallback;
            if (graphX >= n.x && graphX < n.x + w && graphY >= n.y && graphY < n.y + h) {
                // Prefer smaller (top-most) cards when overlapping.
                if (!best || w * h < best.area) {
                    best = { name: name, node: n, width: w, height: h, area: w * h };
                }
            }
        }
        return best;
    }

    /** Resize edge near a NOTE area (logical coords). Margin matches desktop/web hit testing. */
    function noteResizeEdge(area, graphX, graphY) {
        if (!area || area.areaType !== "NOTE") {
            return null;
        }
        var m = 10;
        var left = graphX <= area.x + m;
        var right = graphX >= area.x + area.width - m;
        var top = graphY <= area.y + m;
        var bottom = graphY >= area.y + area.height - m;
        if (left && top) {
            return "nw-resize";
        }
        if (right && top) {
            return "ne-resize";
        }
        if (left && bottom) {
            return "sw-resize";
        }
        if (right && bottom) {
            return "se-resize";
        }
        if (left) {
            return "w-resize";
        }
        if (right) {
            return "e-resize";
        }
        if (top) {
            return "n-resize";
        }
        if (bottom) {
            return "s-resize";
        }
        return null;
    }

    function getCanvasWidget(canvasId) {
        if (!canvasId || typeof rap === "undefined") {
            return null;
        }
        try {
            return rap.getObject(canvasId);
        } catch (e) {
            return null;
        }
    }

    function getWidgetData(canvasId, key) {
        var widget = getCanvasWidget(canvasId);
        return widget && widget.getData ? widget.getData(key) : null;
    }

    function containsRect(rect, x, y) {
        return rect
            && x >= rect.x
            && x < rect.x + rect.width
            && y >= rect.y
            && y < rect.y + rect.height;
    }

    function clampNavPreviewRect(viewPort, graphPort, x, y) {
        if (!graphPort || !viewPort) {
            return { x: x, y: y };
        }
        var maxX = graphPort.x + graphPort.width - viewPort.width;
        var maxY = graphPort.y + graphPort.height - viewPort.height;
        return {
            x: Math.max(graphPort.x, Math.min(x, maxX)),
            y: Math.max(graphPort.y, Math.min(y, maxY))
        };
    }

    hop.CanvasSvgRenderer = function (properties) {
        this._canvas = null;
        this._overlay = null;
        this._sessionUuid = null;
        this._canvasId = null;
        this._revision = 0;
        this._areas = [];
        this._props = {};
        this._remoteObject = null;
        this._serviceHandlerUrl = null;
        this._pollTimer = null;
        this._pollCount = 0;
        this._emptyRetries = 0;
        this._mousemoveHandler = null;
        this._mouseleaveHandler = null;
        this._lastHoverKey = null;
        this._svgHost = null;
        this._effectsLayer = null;
        this._dragPreviewRects = null;
        this._dragActive = false;
        this._dragClickedName = null;
        this._dragIconOffset = null;
        this._dragIconArea = null;
        this._dragStartPositions = null;
        this._dragNodes = null;
        this._dragNotes = null;
        this._noteHandleRects = null;
        this._hopLineEl = null;
        this._mousedownHandler = null;
        this._mouseupHandler = null;
        this._documentMouseMoveHandler = null;
        // Last mousedown (graph + screen) so mode=drag / mode=resize previews can init after
        // the server arms mode on a later RAP round-trip (same timing pattern as canvas.js).
        this._lastMouseDownGraph = null;
        this._lastMouseDownScreen = null;
        this._noteResizeActive = false;
        this._resizedNote = null;
        this._resizeStartScreenX = 0;
        this._resizeStartScreenY = 0;
        this._modeDragListening = false;
        // RAP/browser often reports event.buttons === 0 during drag; track locally.
        this._pointerHeld = false;
        this._ghostSvg = null;
        this._ghostRectPool = null;
        this._panBoundsOutline = null;
        this._panActive = false;
        this._panInitialized = false;
        this._panStartMouseX = 0;
        this._panStartMouseY = 0;
        this._panStartOffsetX = 0;
        this._panStartOffsetY = 0;
        this._panCurrentOffsetX = 0;
        this._panCurrentOffsetY = 0;
        this._panBounds = null;
        this._navDragActive = false;
        this._navDragStartX = 0;
        this._navDragStartY = 0;
        this._navDragBaseViewPort = null;
        this._navViewportPreview = null;
        this._selectLasso = null;
        this._selectActive = false;
        this._selectStartX = 0;
        this._selectStartY = 0;
    };

    hop.CanvasSvgRenderer.prototype = {
        destroy: function () {
            if (this._pollTimer) {
                clearInterval(this._pollTimer);
                this._pollTimer = null;
            }
            if (this._canvas && this._mousemoveHandler) {
                this._canvas.removeEventListener("mousemove", this._mousemoveHandler);
            }
            if (this._canvas && this._mouseleaveHandler) {
                this._canvas.removeEventListener("mouseleave", this._mouseleaveHandler);
            }
            this._detachInteractionListeners();
            if (this._overlay && this._overlay.parentNode) {
                this._overlay.parentNode.removeChild(this._overlay);
            }
        },

        attachListener: function () {
            this._findAndAttachCanvas();
        },

        setSessionUuid: function (properties) {
            this._sessionUuid = properties.value;
        },

        setCanvasId: function (properties) {
            this._canvasId = properties.value;
        },

        setRenderRevision: function (properties) {
            if (properties.value !== this._revision) {
                this._revision = 0;
                this._fetchAndRender(0);
            }
        },

        _findAndAttachCanvas: function () {
            var self = this;
            var attempts = 0;
            var maxAttempts = 20;

            var tryFind = function () {
                attempts++;
                var canvas = findCanvasForWidget(self._canvasId);
                if (!canvas) {
                    if (attempts < maxAttempts) {
                        setTimeout(tryFind, 100);
                    }
                    return;
                }
                if (self._canvas === canvas) {
                    return;
                }
                self._attachToCanvas(canvas);
            };
            tryFind();
        },

        _attachToCanvas: function (canvas) {
            var self = this;
            if (this._canvas && this._mousemoveHandler) {
                this._canvas.removeEventListener("mousemove", this._mousemoveHandler);
            }
            if (this._canvas && this._mouseleaveHandler) {
                this._canvas.removeEventListener("mouseleave", this._mouseleaveHandler);
            }
            if (this._canvas && this._mousedownHandler) {
                this._canvas.removeEventListener("mousedown", this._mousedownHandler);
            }
            this._detachInteractionListeners();
            this._canvas = canvas;

            if (!this._overlay) {
                this._overlay = document.createElement("div");
                this._overlay.style.position = "absolute";
                this._overlay.style.left = "0";
                this._overlay.style.top = "0";
                this._overlay.style.width = "100%";
                this._overlay.style.height = "100%";
                this._overlay.style.pointerEvents = "none";
                this._overlay.style.overflow = "hidden";
                this._overlay.style.zIndex = "10";

                this._svgHost = document.createElement("div");
                this._svgHost.style.width = "100%";
                this._svgHost.style.height = "100%";
                // Critical: without this, the SVG subtree captures wheel events and canvas-zoom.js
                // (listening on the RAP canvas under the overlay) never sees scroll zoom.
                this._svgHost.style.pointerEvents = "none";

                this._effectsLayer = document.createElement("div");
                this._effectsLayer.style.position = "absolute";
                this._effectsLayer.style.left = "0";
                this._effectsLayer.style.top = "0";
                this._effectsLayer.style.width = "100%";
                this._effectsLayer.style.height = "100%";
                this._effectsLayer.style.pointerEvents = "none";
                // Keep ghosts above the SVG host (opacity on the host creates a stacking context).
                this._effectsLayer.style.zIndex = "20";

                this._panBoundsOutline = document.createElement("div");
                this._panBoundsOutline.style.position = "absolute";
                this._panBoundsOutline.style.display = "none";
                this._panBoundsOutline.style.pointerEvents = "none";
                this._panBoundsOutline.style.boxSizing = "border-box";
                this._panBoundsOutline.style.border = "1px dashed rgb(61, 99, 128)";
                this._panBoundsOutline.style.backgroundColor = "transparent";
                this._effectsLayer.appendChild(this._panBoundsOutline);

                this._navViewportPreview = document.createElement("div");
                this._navViewportPreview.style.position = "absolute";
                this._navViewportPreview.style.display = "none";
                this._navViewportPreview.style.pointerEvents = "none";
                this._navViewportPreview.style.boxSizing = "border-box";
                this._navViewportPreview.style.border = "1px solid rgb(0, 0, 0)";
                this._navViewportPreview.style.backgroundColor = "rgba(0, 0, 255, 0.75)";
                this._effectsLayer.appendChild(this._navViewportPreview);

                this._selectLasso = document.createElement("div");
                this._selectLasso.style.position = "absolute";
                this._selectLasso.style.display = "none";
                this._selectLasso.style.pointerEvents = "none";
                this._selectLasso.style.boxSizing = "border-box";
                this._selectLasso.style.border = "1px dashed rgb(61, 99, 128)";
                this._selectLasso.style.backgroundColor = "transparent";
                this._effectsLayer.appendChild(this._selectLasso);

                this._overlay.appendChild(this._svgHost);
                this._overlay.appendChild(this._effectsLayer);
            }

            var parent = canvas.parentElement;
            if (parent && this._overlay.parentNode !== parent) {
                if (window.getComputedStyle(parent).position === "static") {
                    parent.style.position = "relative";
                }
                parent.appendChild(this._overlay);
            }
            this._syncOverlayLayout(canvas);

            if (!this._remoteObject) {
                this._remoteObject = rap.getRemoteObject(this);
            }

            this._mousemoveHandler = function (event) {
                self._handleMouseMove(event);
            };
            this._mouseleaveHandler = function () {
                if (!self._dragActive && !self._panActive && !self._navDragActive && !self._selectActive) {
                    self._lastHoverKey = null;
                    self._updateHoverChrome(null);
                    self._clearNoteResizeHandles();
                    self._clearHopLine();
                }
            };
            this._mousedownHandler = function (event) {
                self._handleMouseDown(event);
            };
            this._mouseupHandler = function (event) {
                self._handleMouseUp(event);
            };
            this._documentMouseMoveHandler = function (event) {
                self._handleDocumentMouseMove(event);
            };
            canvas.addEventListener("mousemove", this._mousemoveHandler);
            canvas.addEventListener("mouseleave", this._mouseleaveHandler);
            canvas.addEventListener("mousedown", this._mousedownHandler);

            if (!this._pollTimer) {
                this._pollTimer = setInterval(function () {
                    self._pollCount++;
                    if (self._canvas) {
                        self._syncOverlayLayout(self._canvas);
                    }
                    // Periodically force a full refresh to recover from missed updates.
                    var clientRev = (self._pollCount % 10 === 0) ? 0 : self._revision;
                    self._fetchAndRender(clientRev);
                }, 500);
            }

            this._revision = 0;
            this._fetchAndRender(0);
        },

        _syncOverlayLayout: function (canvas) {
            if (!this._overlay || !canvas) {
                return;
            }
            var parent = canvas.parentElement;
            if (!parent) {
                return;
            }
            var left = canvas.offsetLeft;
            var top = canvas.offsetTop;
            if (canvas.offsetParent !== parent) {
                var canvasRect = canvas.getBoundingClientRect();
                var parentRect = parent.getBoundingClientRect();
                left = canvasRect.left - parentRect.left;
                top = canvasRect.top - parentRect.top;
            }
            this._overlay.style.left = Math.round(left) + "px";
            this._overlay.style.top = Math.round(top) + "px";
            this._overlay.style.width = canvas.clientWidth + "px";
            this._overlay.style.height = canvas.clientHeight + "px";
        },

        _serviceUrl: function (rev) {
            var baseUrl = this._serviceHandlerUrl || buildServiceHandlerUrl("canvasRender");
            return baseUrl
                + "&session=" + encodeURIComponent(this._sessionUuid || "")
                + "&canvas=" + encodeURIComponent(this._canvasId || "")
                + "&rev=" + encodeURIComponent(rev || 0);
        },

        _fetchAndRender: function (clientRev) {
            var self = this;
            if (!this._sessionUuid || !this._canvasId) {
                return;
            }
            fetch(this._serviceUrl(clientRev), { credentials: "same-origin" })
                .then(function (response) {
                    if (response.status === 304) {
                        return null;
                    }
                    if (response.status === 204) {
                        if (self._emptyRetries < 30) {
                            self._emptyRetries++;
                            setTimeout(function () {
                                self._fetchAndRender(0);
                            }, 500);
                        }
                        return null;
                    }
                    if (!response.ok) {
                        if (self._emptyRetries < 30) {
                            self._emptyRetries++;
                            setTimeout(function () {
                                self._fetchAndRender(0);
                            }, 1000);
                        }
                        return null;
                    }
                    return response.json();
                })
                .then(function (data) {
                    if (!data) {
                        return;
                    }
                    self._emptyRetries = 0;
                    self._revision = data.revision;
                    self._areas = data.areas || [];
                    self._props = data.props || {};
                    if (data.svg && self._svgHost) {
                        self._svgHost.innerHTML = data.svg;
                        var svg = self._svgHost.querySelector("svg");
                        if (svg) {
                            svg.setAttribute("width", "100%");
                            svg.setAttribute("height", "100%");
                            svg.style.display = "block";
                            svg.style.pointerEvents = "none";
                        }
                        if (self._canvas) {
                            self._syncOverlayLayout(self._canvas);
                        }
                        // Post-pan/server redraw must not keep a leftover dim from client previews.
                        self._restoreIdleChrome();
                    }
                })
                .catch(function (err) {
                    if (window.console && console.debug) {
                        console.debug("Hop canvas SVG fetch failed", err);
                    }
                });
        },

        _updateHoverChrome: function (area, graphX, graphY) {
            if (!this._canvas) {
                return;
            }
            if (this._panActive || this._navDragActive || this._dragActive) {
                this._canvas.style.cursor = "grabbing";
                this._clearNoteResizeHandles();
                return;
            }
            // Note edges: resize cursors (do not use generic pointer on whole note).
            if (area && area.areaType === "NOTE" && graphX != null && graphY != null) {
                var edge = noteResizeEdge(area, graphX, graphY);
                if (edge) {
                    this._canvas.style.cursor = edge;
                    this._drawNoteResizeHandles(area);
                    return;
                }
                this._drawNoteResizeHandles(area);
                this._canvas.style.cursor = "move";
                return;
            }
            this._clearNoteResizeHandles();
            // Name/link: hand. Icon body: default (move). Others: clear.
            if (area && (area.areaType === "TRANSFORM_NAME" || area.areaType === "NOTE_LINK"
                || area.areaType === "ACTION_NAME")) {
                this._canvas.style.cursor = "pointer";
            } else if (area && (area.areaType === "TRANSFORM_ICON" || area.areaType === "ACTION_ICON")) {
                this._canvas.style.cursor = "default";
            } else {
                this._canvas.style.cursor = "";
            }
        },

        _ensureNoteHandle: function (index) {
            if (!this._noteHandleRects) {
                this._noteHandleRects = [];
            }
            while (this._noteHandleRects.length <= index) {
                var div = document.createElement("div");
                div.style.position = "absolute";
                div.style.boxSizing = "border-box";
                div.style.pointerEvents = "none";
                div.style.display = "none";
                div.style.width = "8px";
                div.style.height = "8px";
                div.style.backgroundColor = "rgb(0, 93, 166)";
                div.style.border = "1px solid rgb(255, 255, 255)";
                this._effectsLayer.appendChild(div);
                this._noteHandleRects.push(div);
            }
            return this._noteHandleRects[index];
        },

        _clearNoteResizeHandles: function () {
            if (!this._noteHandleRects) {
                return;
            }
            for (var i = 0; i < this._noteHandleRects.length; i++) {
                this._noteHandleRects[i].style.display = "none";
            }
        },

        _drawNoteResizeHandles: function (area) {
            // Prefer all selected notes from widget data; fall back to the hovered NOTE area.
            var notes = getWidgetData(this._canvasId, "notes");
            var props = this._getCanvasProps();
            var list = [];
            if (notes && notes.length) {
                for (var n = 0; n < notes.length; n++) {
                    if (notes[n] && notes[n].selected) {
                        list.push(notes[n]);
                    }
                }
            }
            if (!list.length && area && area.areaType === "NOTE") {
                list.push({
                    x: area.x,
                    y: area.y,
                    width: area.width,
                    height: area.height,
                    selected: true
                });
            }
            if (!list.length || !this._effectsLayer) {
                this._clearNoteResizeHandles();
                return;
            }
            var hs = 8;
            var handleIndex = 0;
            for (var i = 0; i < list.length; i++) {
                var note = list[i];
                var screen = graphRectToScreen(note.x, note.y, note.width, note.height, props);
                var positions = [
                    [screen.left - hs / 2, screen.top - hs / 2],
                    [screen.left + screen.width / 2 - hs / 2, screen.top - hs / 2],
                    [screen.left + screen.width - hs / 2, screen.top - hs / 2],
                    [screen.left + screen.width - hs / 2, screen.top + screen.height / 2 - hs / 2],
                    [screen.left + screen.width - hs / 2, screen.top + screen.height - hs / 2],
                    [screen.left + screen.width / 2 - hs / 2, screen.top + screen.height - hs / 2],
                    [screen.left - hs / 2, screen.top + screen.height - hs / 2],
                    [screen.left - hs / 2, screen.top + screen.height / 2 - hs / 2]
                ];
                for (var p = 0; p < positions.length; p++) {
                    var el = this._ensureNoteHandle(handleIndex++);
                    el.style.display = "block";
                    el.style.left = Math.round(positions[p][0]) + "px";
                    el.style.top = Math.round(positions[p][1]) + "px";
                }
            }
            // Hide any leftover handles from a previous multi-note selection.
            if (this._noteHandleRects) {
                for (var h = handleIndex; h < this._noteHandleRects.length; h++) {
                    this._noteHandleRects[h].style.display = "none";
                }
            }
        },

        _ensureHopLine: function () {
            if (!this._hopLineEl && this._effectsLayer) {
                var svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
                svg.style.position = "absolute";
                svg.style.left = "0";
                svg.style.top = "0";
                svg.style.width = "100%";
                svg.style.height = "100%";
                svg.style.pointerEvents = "none";
                svg.style.overflow = "visible";
                var line = document.createElementNS("http://www.w3.org/2000/svg", "line");
                line.setAttribute("stroke", "rgb(0, 93, 166)");
                line.setAttribute("stroke-width", "2");
                line.setAttribute("stroke-dasharray", "6,4");
                svg.appendChild(line);
                this._effectsLayer.appendChild(svg);
                this._hopLineEl = { svg: svg, line: line };
            }
            return this._hopLineEl;
        },

        _clearHopLine: function () {
            if (this._hopLineEl) {
                this._hopLineEl.svg.style.display = "none";
            }
        },

        _updateHopLine: function (screenX, screenY) {
            var mode = getWidgetData(this._canvasId, "mode");
            if (mode !== "hop") {
                this._clearHopLine();
                return;
            }
            var startName = getWidgetData(this._canvasId, "startHopNode");
            var nodes = getWidgetData(this._canvasId, "nodes") || {};
            var startNode = startName ? nodes[startName] : null;
            if (!startNode) {
                this._clearHopLine();
                return;
            }
            var props = this._getCanvasProps();
            var size = nodeSize(startNode, props, null);
            var startScreen = graphRectToScreen(
                startNode.x + size.width / 2,
                startNode.y + size.height / 2,
                0,
                0,
                props);
            var hop = this._ensureHopLine();
            hop.svg.style.display = "block";
            hop.line.setAttribute("x1", Math.round(startScreen.left));
            hop.line.setAttribute("y1", Math.round(startScreen.top));
            hop.line.setAttribute("x2", Math.round(screenX));
            hop.line.setAttribute("y2", Math.round(screenY));
        },

        _getCanvasProps: function () {
            return getWidgetData(this._canvasId, "props") || this._props || {};
        },

        _detachInteractionListeners: function () {
            if (this._documentMouseMoveHandler) {
                document.removeEventListener("mousemove", this._documentMouseMoveHandler);
            }
            if (this._mouseupHandler) {
                document.removeEventListener("mouseup", this._mouseupHandler);
                document.removeEventListener("pointerup", this._mouseupHandler);
                document.removeEventListener("pointercancel", this._mouseupHandler);
            }
            this._modeDragListening = false;
            this._endPan();
            this._endNoteResize();
            this._endDrag();
            this._endNavDrag();
            this._endSelect();
            this._restoreIdleChrome();
        },

        _refreshPanBoundaries: function () {
            var props = this._getCanvasProps();
            if (props.panBoundaries) {
                this._panBounds = props.panBoundaries;
            }
        },

        /**
         * Dim SVG while client wireframe previews run (pan/drag/resize). Always clear when idle —
         * leaving opacity at 0.35 after pan end (or after a post-pan SVG refresh) makes the model
         * look permanently muted.
         */
        _setSvgDimmed: function (dimmed) {
            if (!this._svgHost) {
                return;
            }
            this._svgHost.style.opacity = dimmed ? "0.35" : "";
        },

        _isInteractionActive: function () {
            return !!(this._panActive || this._dragActive || this._noteResizeActive
                || this._navDragActive || this._selectActive);
        },

        _restoreIdleChrome: function () {
            if (this._isInteractionActive()) {
                return;
            }
            this._setSvgDimmed(false);
            this._clearDragPreview();
            if (this._panBoundsOutline) {
                this._panBoundsOutline.style.display = "none";
            }
            if (this._canvas) {
                this._canvas.style.cursor = "";
            }
        },

        _beginPan: function (screenX, screenY) {
            var props = this._getCanvasProps();
            this._panActive = true;
            this._panInitialized = false;
            this._panStartMouseX = screenX;
            this._panStartMouseY = screenY;
            this._panStartOffsetX = Math.round(props.offsetX || 0);
            this._panStartOffsetY = Math.round(props.offsetY || 0);
            this._panCurrentOffsetX = this._panStartOffsetX;
            this._panCurrentOffsetY = this._panStartOffsetY;
            this._panBounds = props.panBoundaries || null;
            this._lastHoverKey = null;
            this._updateHoverChrome(null);
            this._setSvgDimmed(true);
            if (this._canvas) {
                this._canvas.style.cursor = "grabbing";
            }
            document.addEventListener("mousemove", this._documentMouseMoveHandler);
            document.addEventListener("mouseup", this._mouseupHandler);
            // Middle-button pan: also listen for auxclick / lost pointer in case mouseup is skipped.
            document.addEventListener("pointerup", this._mouseupHandler);
            document.addEventListener("pointercancel", this._mouseupHandler);
            var self = this;
            setTimeout(function () {
                if (self._panActive) {
                    self._refreshPanBoundaries();
                    self._updatePanPreview();
                }
            }, 0);
        },

        _endPan: function () {
            this._panActive = false;
            this._panInitialized = false;
            this._panBounds = null;
            this._clearDragPreview();
            if (this._panBoundsOutline) {
                this._panBoundsOutline.style.display = "none";
            }
            // Always undim — do not gate on other flags (stuck drag/resize left canvas muted).
            this._setSvgDimmed(false);
            if (this._canvas && !this._dragActive && !this._noteResizeActive) {
                this._canvas.style.cursor = "";
            }
            this._restoreIdleChrome();
        },

        _computePanOffset: function (screenX, screenY) {
            var props = this._getCanvasProps();
            var mag = props.magnification || 1.0;
            var zoomFactor = Math.max(0.1, mag);

            if (!this._panInitialized) {
                this._panInitialized = true;
                this._panStartMouseX = screenX;
                this._panStartMouseY = screenY;
                return;
            }

            var deltaX = (this._panStartMouseX - screenX) / zoomFactor;
            var deltaY = (this._panStartMouseY - screenY) / zoomFactor;
            var newOffsetX = this._panStartOffsetX - deltaX;
            var newOffsetY = this._panStartOffsetY - deltaY;

            if (this._panBounds) {
                if (newOffsetX < this._panBounds.x) {
                    newOffsetX = this._panBounds.x;
                }
                if (newOffsetX > this._panBounds.width) {
                    newOffsetX = this._panBounds.width;
                }
                if (newOffsetY < this._panBounds.y) {
                    newOffsetY = this._panBounds.y;
                }
                if (newOffsetY > this._panBounds.height) {
                    newOffsetY = this._panBounds.height;
                }
            }

            this._panCurrentOffsetX = Math.round(newOffsetX);
            this._panCurrentOffsetY = Math.round(newOffsetY);
        },

        _updatePanPreview: function () {
            if (!this._panActive) {
                this._clearDragPreview();
                if (this._panBoundsOutline) {
                    this._panBoundsOutline.style.display = "none";
                }
                return;
            }

            var props = this._getCanvasProps();
            var nodes = getWidgetData(this._canvasId, "nodes");
            var notes = getWidgetData(this._canvasId, "notes");
            var iconSize = props.iconSize || 32;
            var offsetX = this._panCurrentOffsetX;
            var offsetY = this._panCurrentOffsetY;
            var selectedColor = "rgb(0, 93, 166)";
            var previewIndex = 0;
            var graphBounds = {
                minX: Number.POSITIVE_INFINITY,
                minY: Number.POSITIVE_INFINITY,
                maxX: Number.NEGATIVE_INFINITY,
                maxY: Number.NEGATIVE_INFINITY
            };
            var hasBounds = false;

            if (nodes) {
                for (var name in nodes) {
                    if (!nodes.hasOwnProperty(name)) {
                        continue;
                    }
                    var node = nodes[name];
                    var x = node.x;
                    var y = node.y;
                    var size = nodeSize(node, props, null);
                    extendGraphBounds(graphBounds, x, y, size.width, size.height);
                    hasBounds = true;
                    var screen = graphRectToScreen(
                        x - 1, y - 1, size.width + 1, size.height + 1, props, offsetX, offsetY);
                    var rect = this._ensureDragPreviewRect(previewIndex++);
                    rect.style.display = "block";
                    rect.style.left = Math.round(screen.left) + "px";
                    rect.style.top = Math.round(screen.top) + "px";
                    rect.style.width = Math.round(screen.width) + "px";
                    rect.style.height = Math.round(screen.height) + "px";
                    rect.style.border = node.selected
                        ? "3px solid " + selectedColor
                        : "1px solid rgb(61, 99, 128)";
                }
            }

            if (notes && notes.length) {
                for (var i = 0; i < notes.length; i++) {
                    var note = notes[i];
                    extendGraphBounds(graphBounds, note.x, note.y, note.width, note.height);
                    hasBounds = true;
                    var noteScreen = graphRectToScreen(
                        note.x, note.y, note.width, note.height, props, offsetX, offsetY);
                    var noteRect = this._ensureDragPreviewRect(previewIndex++);
                    noteRect.style.display = "block";
                    noteRect.style.left = Math.round(noteScreen.left) + "px";
                    noteRect.style.top = Math.round(noteScreen.top) + "px";
                    noteRect.style.width = Math.round(noteScreen.width) + "px";
                    noteRect.style.height = Math.round(noteScreen.height) + "px";
                    noteRect.style.borderRadius = "4px";
                    noteRect.style.border = note.selected
                        ? "2px solid " + selectedColor
                        : "1px solid rgb(120, 120, 120)";
                }
            }

            if (hasBounds && this._panBoundsOutline) {
                var pad = 8;
                var boundsScreen = graphRectToScreen(
                    graphBounds.minX - pad,
                    graphBounds.minY - pad,
                    (graphBounds.maxX - graphBounds.minX) + (pad * 2),
                    (graphBounds.maxY - graphBounds.minY) + (pad * 2),
                    props,
                    offsetX,
                    offsetY);
                this._panBoundsOutline.style.display = "block";
                this._panBoundsOutline.style.left = Math.round(boundsScreen.left) + "px";
                this._panBoundsOutline.style.top = Math.round(boundsScreen.top) + "px";
                this._panBoundsOutline.style.width = Math.round(boundsScreen.width) + "px";
                this._panBoundsOutline.style.height = Math.round(boundsScreen.height) + "px";
            } else if (this._panBoundsOutline) {
                this._panBoundsOutline.style.display = "none";
            }

            for (var j = previewIndex; j < this._dragPreviewRects.length; j++) {
                this._dragPreviewRects[j].style.display = "none";
            }
        },

        _captureDragNodes: function () {
            var nodes = getWidgetData(this._canvasId, "nodes");
            this._dragNodes = nodes;
            this._dragStartPositions = {};
            if (nodes) {
                for (var name in nodes) {
                    if (nodes.hasOwnProperty(name)) {
                        this._dragStartPositions[name] = {
                            x: nodes[name].x,
                            y: nodes[name].y
                        };
                    }
                }
            }
        },

        /**
         * Normalize notes widget data to a plain array (RAP JsonArray is array-like).
         */
        _notesAsArray: function (notes) {
            if (!notes) {
                return [];
            }
            if (Object.prototype.toString.call(notes) === "[object Array]") {
                return notes;
            }
            // RAP JsonArray: length + numeric index, or values() / asArray()
            if (typeof notes.length === "number") {
                var out = [];
                for (var i = 0; i < notes.length; i++) {
                    out.push(notes[i]);
                }
                return out;
            }
            if (typeof notes.asArray === "function") {
                return notes.asArray();
            }
            return [];
        },

        _findNoteAreaAt: function (graphX, graphY) {
            if (!this._areas || !this._areas.length) {
                return null;
            }
            for (var i = this._areas.length - 1; i >= 0; i--) {
                var a = this._areas[i];
                if (!a || a.areaType !== "NOTE") {
                    continue;
                }
                var w = a.width > 0 ? a.width : 0;
                var h = a.height > 0 ? a.height : 0;
                if (w > 0 && h > 0
                    && graphX >= a.x && graphX < a.x + w
                    && graphY >= a.y && graphY < a.y + h) {
                    return a;
                }
            }
            return null;
        },

        _noteFromArea: function (area, selected) {
            return {
                x: area.x,
                y: area.y,
                width: area.width > 0 ? area.width : 100,
                height: area.height > 0 ? area.height : 50,
                selected: !!selected
            };
        },

        _captureDragNotes: function () {
            var notes = this._notesAsArray(getWidgetData(this._canvasId, "notes"));
            this._dragNotes = [];
            var anySelected = false;
            var i;
            for (i = 0; i < notes.length; i++) {
                var note = notes[i];
                if (!note) {
                    continue;
                }
                var w = note.width > 0 ? note.width : 0;
                var h = note.height > 0 ? note.height : 0;
                // Geometry from matching NOTE area when widget data omits size.
                if (!(w > 0) || !(h > 0)) {
                    var areaMatch = null;
                    if (this._areas) {
                        for (var ai = 0; ai < this._areas.length; ai++) {
                            var ar = this._areas[ai];
                            if (ar && ar.areaType === "NOTE"
                                && ar.x === note.x && ar.y === note.y) {
                                areaMatch = ar;
                                break;
                            }
                        }
                    }
                    if (areaMatch) {
                        w = areaMatch.width > 0 ? areaMatch.width : w;
                        h = areaMatch.height > 0 ? areaMatch.height : h;
                    }
                }
                var sel = !!note.selected;
                if (sel) {
                    anySelected = true;
                }
                this._dragNotes.push({
                    x: note.x,
                    y: note.y,
                    width: w > 0 ? w : 100,
                    height: h > 0 ? h : 50,
                    selected: sel
                });
            }

            // Prefer geometry from the SVG area map (always has full note bounds).
            if (this._lastMouseDownGraph) {
                var gx = this._lastMouseDownGraph.x;
                var gy = this._lastMouseDownGraph.y;
                var hitArea = this._findNoteAreaAt(gx, gy);
                if (hitArea) {
                    // Mark matching captured note selected, or inject from area.
                    var found = false;
                    for (var j = 0; j < this._dragNotes.length; j++) {
                        var n = this._dragNotes[j];
                        var near = Math.abs(n.x - hitArea.x) < 2 && Math.abs(n.y - hitArea.y) < 2;
                        var inside = gx >= n.x && gx < n.x + n.width
                            && gy >= n.y && gy < n.y + n.height;
                        if (near || inside) {
                            n.x = hitArea.x;
                            n.y = hitArea.y;
                            n.width = hitArea.width > 0 ? hitArea.width : n.width;
                            n.height = hitArea.height > 0 ? hitArea.height : n.height;
                            n.selected = true;
                            anySelected = true;
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        this._dragNotes.push(this._noteFromArea(hitArea, true));
                        anySelected = true;
                    }
                }
            }

            // Race: mode=drag before selected flags — pick note under mousedown by bounds.
            if (!anySelected && this._lastMouseDownGraph) {
                var gx2 = this._lastMouseDownGraph.x;
                var gy2 = this._lastMouseDownGraph.y;
                for (var k = 0; k < this._dragNotes.length; k++) {
                    var nn = this._dragNotes[k];
                    if (gx2 >= nn.x && gx2 < nn.x + nn.width
                        && gy2 >= nn.y && gy2 < nn.y + nn.height) {
                        nn.selected = true;
                        anySelected = true;
                        break;
                    }
                }
            }
            if (!anySelected && this._dragNotes.length === 1) {
                this._dragNotes[0].selected = true;
            }
        },

        _hasSelectedDragNotes: function () {
            if (!this._dragNotes || !this._dragNotes.length) {
                return false;
            }
            for (var i = 0; i < this._dragNotes.length; i++) {
                var n = this._dragNotes[i];
                if (n && n.selected && n.width > 0 && n.height > 0) {
                    return true;
                }
            }
            return false;
        },

        _ensureDocumentDragListeners: function () {
            document.addEventListener("mousemove", this._documentMouseMoveHandler);
            document.addEventListener("mouseup", this._mouseupHandler);
            this._modeDragListening = true;
        },

        _beginDrag: function (clickedName, graphX, graphY, iconArea) {
            this._dragActive = true;
            this._dragClickedName = clickedName;
            this._dragIconArea = iconArea || null;
            this._dragIconOffset = {
                x: graphX - (iconArea ? iconArea.x : graphX),
                y: graphY - (iconArea ? iconArea.y : graphY)
            };
            this._captureDragNodes();
            this._captureDragNotes();
            if (iconArea && !this._dragStartPositions[clickedName]) {
                this._dragStartPositions[clickedName] = { x: iconArea.x, y: iconArea.y };
            }
            // Enrich node size from hit area when server nodes omit width/height.
            if (this._dragNodes && this._dragNodes[clickedName] && iconArea) {
                var n = this._dragNodes[clickedName];
                if (!(n.width > 0) && iconArea.width > 0) {
                    n.width = iconArea.width;
                }
                if (!(n.height > 0) && iconArea.height > 0) {
                    n.height = iconArea.height;
                }
            }
            this._lastHoverKey = null;
            this._updateHoverChrome(null);
            this._clearNoteResizeHandles();
            this._setSvgDimmed(true);
            if (this._canvas) {
                this._canvas.style.cursor = "grabbing";
            }
            this._ensureDocumentDragListeners();
            this._updateDragPreview(graphX, graphY);
        },

        /**
         * Server armed mode=drag (notes and/or nodes) but client did not start via icon hit.
         * Uses last mousedown graph point so dx matches canvas.js (screen delta / mag).
         */
        _beginModeDrag: function (graphX, graphY) {
            if (this._dragActive || this._noteResizeActive) {
                return;
            }
            var start = this._lastMouseDownGraph || { x: graphX, y: graphY };
            this._dragActive = true;
            // Synthetic anchor: iconOffset 0 → dx = graphX - start.x (mousedown origin).
            this._dragClickedName = "__mode_drag__";
            this._dragIconArea = null;
            this._dragIconOffset = { x: 0, y: 0 };
            // Capture nodes first (rewrites _dragStartPositions), then seed synthetic anchor.
            this._captureDragNodes();
            this._captureDragNotes();
            if (!this._dragStartPositions) {
                this._dragStartPositions = {};
            }
            this._dragStartPositions["__mode_drag__"] = { x: start.x, y: start.y };
            this._lastHoverKey = null;
            this._updateHoverChrome(null);
            this._clearNoteResizeHandles();
            this._setSvgDimmed(true);
            if (this._canvas) {
                this._canvas.style.cursor = "grabbing";
            }
            this._ensureDocumentDragListeners();
            this._updateDragPreview(graphX, graphY);
        },

        _endDrag: function () {
            this._dragActive = false;
            this._dragClickedName = null;
            this._dragIconOffset = null;
            this._dragIconArea = null;
            this._dragStartPositions = null;
            this._dragNodes = null;
            this._dragNotes = null;
            this._modeDragListening = false;
            this._clearDragPreview();
            this._clearHopLine();
            this._restoreIdleChrome();
        },

        /**
         * SVG ghost layer for drag/resize outlines. HTML div borders were unreliable under RAP
         * (stacking / empty node maps); SVG rects match the hop rubber-band path that works.
         */
        _ensureGhostSvg: function () {
            if (!this._ghostSvg && this._effectsLayer) {
                var svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
                svg.setAttribute("width", "100%");
                svg.setAttribute("height", "100%");
                svg.style.position = "absolute";
                svg.style.left = "0";
                svg.style.top = "0";
                svg.style.width = "100%";
                svg.style.height = "100%";
                svg.style.pointerEvents = "none";
                svg.style.overflow = "visible";
                svg.style.zIndex = "30";
                this._effectsLayer.appendChild(svg);
                this._ghostSvg = svg;
                this._ghostRectPool = [];
            }
            return this._ghostSvg;
        },

        _ensureGhostRect: function (index) {
            this._ensureGhostSvg();
            if (!this._ghostRectPool) {
                this._ghostRectPool = [];
            }
            while (this._ghostRectPool.length <= index) {
                var rect = document.createElementNS("http://www.w3.org/2000/svg", "rect");
                rect.setAttribute("fill", "rgba(0, 93, 166, 0.12)");
                rect.setAttribute("stroke", "rgb(0, 93, 166)");
                rect.setAttribute("stroke-width", "2");
                rect.setAttribute("rx", "6");
                rect.setAttribute("ry", "6");
                rect.style.display = "none";
                this._ghostSvg.appendChild(rect);
                this._ghostRectPool.push(rect);
            }
            return this._ghostRectPool[index];
        },

        _placeGhostRect: function (index, left, top, width, height, selected) {
            var rect = this._ensureGhostRect(index);
            var w = Math.max(4, Math.round(width));
            var h = Math.max(4, Math.round(height));
            rect.style.display = "block";
            rect.setAttribute("x", Math.round(left));
            rect.setAttribute("y", Math.round(top));
            rect.setAttribute("width", w);
            rect.setAttribute("height", h);
            rect.setAttribute("stroke-width", selected ? "3" : "2");
            rect.setAttribute("stroke", selected ? "rgb(0, 93, 166)" : "rgb(61, 99, 128)");
            rect.setAttribute("fill", selected ? "rgba(0, 93, 166, 0.15)" : "rgba(61, 99, 128, 0.08)");
            return rect;
        },

        _clearDragPreview: function () {
            if (this._dragPreviewRects) {
                for (var i = 0; i < this._dragPreviewRects.length; i++) {
                    this._dragPreviewRects[i].style.display = "none";
                }
            }
            if (this._ghostRectPool) {
                for (var g = 0; g < this._ghostRectPool.length; g++) {
                    this._ghostRectPool[g].style.display = "none";
                }
            }
            if (this._ghostSvg) {
                this._ghostSvg.style.display = "none";
            }
        },

        _showGhostSvg: function () {
            var svg = this._ensureGhostSvg();
            if (svg) {
                svg.style.display = "block";
            }
        },

        _nodeNames: function (nodes) {
            var names = [];
            if (!nodes) {
                return names;
            }
            // Prefer for-in: RAP/JSON maps are not always plain objects for Object.keys.
            for (var name in nodes) {
                if (Object.prototype.hasOwnProperty.call(nodes, name)) {
                    names.push(name);
                }
            }
            return names;
        },

        _updateDragPreview: function (graphX, graphY) {
            if (!this._dragActive || !this._dragClickedName || !this._dragIconOffset
                || !this._dragStartPositions) {
                this._clearDragPreview();
                return;
            }

            // Live widget props (offset/magnification) — snapshot this._props can be stale/empty.
            var props = this._getCanvasProps();
            // Prefer SVG snapshot props when widget props lack magnification (first paint race).
            if (props.magnification == null && this._props && this._props.magnification != null) {
                props = this._props;
            }
            var gridSize = props.showGrid ? (props.gridSize || 1) : 1;
            var clickedStart = this._dragStartPositions[this._dragClickedName];
            if (!clickedStart) {
                // Recover synthetic / missing anchor so we still draw something.
                clickedStart = this._lastMouseDownGraph || { x: graphX, y: graphY };
                this._dragStartPositions[this._dragClickedName] = clickedStart;
            }

            var iconTargetX = graphX - this._dragIconOffset.x;
            var iconTargetY = graphY - this._dragIconOffset.y;
            iconTargetX = snapToGrid(iconTargetX, gridSize);
            iconTargetY = snapToGrid(iconTargetY, gridSize);
            var dx = iconTargetX - clickedStart.x;
            var dy = iconTargetY - clickedStart.y;

            this._showGhostSvg();
            var previewIndex = 0;
            var nodes = this._dragNodes;
            var names = this._nodeNames(nodes);
            var modeDrag = this._dragClickedName === "__mode_drag__";
            if (!modeDrag && names.indexOf(this._dragClickedName) < 0) {
                names = names.concat([this._dragClickedName]);
            }

            for (var i = 0; i < names.length; i++) {
                var name = names[i];
                if (name === "__mode_drag__") {
                    continue;
                }
                var node = nodes ? nodes[name] : null;
                var start = this._dragStartPositions[name];
                if (!start) {
                    continue;
                }
                var isClicked = name === this._dragClickedName;
                if (node && !node.selected && !isClicked) {
                    continue;
                }
                if (!node && !isClicked) {
                    continue;
                }
                if (modeDrag && node && !node.selected) {
                    continue;
                }

                var x = start.x + dx;
                var y = start.y + dy;
                var size = nodeSize(node, props, isClicked ? this._dragIconArea : null);
                var screen = graphRectToScreen(x - 1, y - 1, size.width + 1, size.height + 1, props);
                this._placeGhostRect(
                    previewIndex++,
                    screen.left,
                    screen.top,
                    screen.width,
                    screen.height,
                    (node && node.selected) || isClicked);
            }

            // Selected notes move with the same dx/dy (canvas.js drawNotes mode=drag).
            // Re-capture once if empty so late-arriving notes[] / areas populate geometry.
            if ((!this._dragNotes || !this._dragNotes.length) && modeDrag) {
                this._captureDragNotes();
            }
            var notes = this._dragNotes;
            var anyNoteDrawn = false;
            if (notes && notes.length) {
                for (var ni = 0; ni < notes.length; ni++) {
                    var note = notes[ni];
                    if (!note.selected) {
                        continue;
                    }
                    var nw = note.width > 0 ? note.width : 100;
                    var nh = note.height > 0 ? note.height : 50;
                    var nx = note.x + dx;
                    var ny = note.y + dy;
                    var noteScreen = graphRectToScreen(nx, ny, nw, nh, props);
                    this._placeGhostRect(
                        previewIndex++,
                        noteScreen.left,
                        noteScreen.top,
                        noteScreen.width,
                        noteScreen.height,
                        true);
                    anyNoteDrawn = true;
                }
            }

            // Fallback: NOTE area under mousedown (full pad size / top-left), never a 32px cursor square.
            if (!anyNoteDrawn && this._lastMouseDownGraph) {
                var hitNote = this._findNoteAreaAt(
                    this._lastMouseDownGraph.x, this._lastMouseDownGraph.y);
                if (hitNote) {
                    var hs = graphRectToScreen(
                        hitNote.x + dx,
                        hitNote.y + dy,
                        hitNote.width > 0 ? hitNote.width : 100,
                        hitNote.height > 0 ? hitNote.height : 50,
                        props);
                    this._placeGhostRect(
                        previewIndex++, hs.left, hs.top, hs.width, hs.height, true);
                    anyNoteDrawn = true;
                }
            }

            // Fallback: node icon area (tables/cards).
            if (previewIndex === 0 && this._dragIconArea) {
                var fb = this._dragIconArea;
                var fbScreen = graphRectToScreen(
                    fb.x + dx, fb.y + dy, fb.width || 32, fb.height || 32, props);
                this._placeGhostRect(
                    previewIndex++, fbScreen.left, fbScreen.top, fbScreen.width, fbScreen.height, true);
            }

            if (this._ghostRectPool) {
                for (var j = previewIndex; j < this._ghostRectPool.length; j++) {
                    this._ghostRectPool[j].style.display = "none";
                }
            }
        },

        _beginNoteResize: function (screenX, screenY) {
            if (this._noteResizeActive) {
                return;
            }
            var direction = getWidgetData(this._canvasId, "resizeDirection");
            var notes = getWidgetData(this._canvasId, "notes");
            if (!direction || !notes || !notes.length) {
                return;
            }
            var selected = null;
            for (var i = 0; i < notes.length; i++) {
                if (notes[i] && notes[i].selected) {
                    selected = notes[i];
                    break;
                }
            }
            // Race: mode=resize before selected flags — use note under mousedown / first note.
            if (!selected && this._lastMouseDownGraph) {
                var gx = this._lastMouseDownGraph.x;
                var gy = this._lastMouseDownGraph.y;
                for (var j = 0; j < notes.length; j++) {
                    var cand = notes[j];
                    if (!cand) {
                        continue;
                    }
                    if (gx >= cand.x && gx < cand.x + cand.width
                        && gy >= cand.y && gy < cand.y + cand.height) {
                        selected = cand;
                        break;
                    }
                }
            }
            if (!selected && notes.length === 1) {
                selected = notes[0];
            }
            if (!selected) {
                return;
            }
            var startScreen = this._lastMouseDownScreen || { x: screenX, y: screenY };
            this._noteResizeActive = true;
            this._resizeStartScreenX = startScreen.x;
            this._resizeStartScreenY = startScreen.y;
            this._resizedNote = {
                direction: direction,
                startX: selected.x,
                startY: selected.y,
                startWidth: selected.width,
                startHeight: selected.height,
                currentX: selected.x,
                currentY: selected.y,
                currentWidth: selected.width,
                currentHeight: selected.height
            };
            this._lastHoverKey = null;
            this._updateHoverChrome(null);
            this._setSvgDimmed(true);
            this._ensureDocumentDragListeners();
            this._updateNoteResizePreview(screenX, screenY);
        },

        _endNoteResize: function () {
            this._noteResizeActive = false;
            this._resizedNote = null;
            this._clearDragPreview();
            this._clearNoteResizeHandles();
            this._restoreIdleChrome();
        },

        _updateNoteResizePreview: function (screenX, screenY) {
            if (!this._noteResizeActive || !this._resizedNote) {
                return;
            }
            var props = this._getCanvasProps();
            var mag = props.magnification || 1.0;
            var deltaX = (screenX - this._resizeStartScreenX) / mag;
            var deltaY = (screenY - this._resizeStartScreenY) / mag;
            var minWidth = 100;
            var minHeight = 50;
            var rn = this._resizedNote;
            rn.currentX = rn.startX;
            rn.currentY = rn.startY;
            rn.currentWidth = rn.startWidth;
            rn.currentHeight = rn.startHeight;
            var dir = rn.direction;
            var newW;
            var newH;

            if (dir === "EAST" || dir === "SOUTH_EAST" || dir === "NORTH_EAST") {
                rn.currentWidth = Math.max(minWidth, rn.startWidth + deltaX);
            }
            if (dir === "WEST" || dir === "SOUTH_WEST" || dir === "NORTH_WEST") {
                newW = Math.max(minWidth, rn.startWidth - deltaX);
                rn.currentX = rn.startX + (rn.startWidth - newW);
                rn.currentWidth = newW;
            }
            if (dir === "SOUTH" || dir === "SOUTH_EAST" || dir === "SOUTH_WEST") {
                rn.currentHeight = Math.max(minHeight, rn.startHeight + deltaY);
            }
            if (dir === "NORTH" || dir === "NORTH_EAST" || dir === "NORTH_WEST") {
                newH = Math.max(minHeight, rn.startHeight - deltaY);
                rn.currentY = rn.startY + (rn.startHeight - newH);
                rn.currentHeight = newH;
            }

            var screen = graphRectToScreen(
                rn.currentX, rn.currentY, rn.currentWidth, rn.currentHeight, props);
            this._showGhostSvg();
            this._placeGhostRect(
                0, screen.left, screen.top, screen.width, screen.height, true);
            if (this._ghostRectPool) {
                for (var j = 1; j < this._ghostRectPool.length; j++) {
                    this._ghostRectPool[j].style.display = "none";
                }
            }

            // Move the 8 resize handles with the live outline (not stale notes[] sizes).
            var hs = 8;
            var handleIndex = 0;
            var positions = [
                [screen.left - hs / 2, screen.top - hs / 2],
                [screen.left + screen.width / 2 - hs / 2, screen.top - hs / 2],
                [screen.left + screen.width - hs / 2, screen.top - hs / 2],
                [screen.left + screen.width - hs / 2, screen.top + screen.height / 2 - hs / 2],
                [screen.left + screen.width - hs / 2, screen.top + screen.height - hs / 2],
                [screen.left + screen.width / 2 - hs / 2, screen.top + screen.height - hs / 2],
                [screen.left - hs / 2, screen.top + screen.height - hs / 2],
                [screen.left - hs / 2, screen.top + screen.height / 2 - hs / 2]
            ];
            for (var p = 0; p < positions.length; p++) {
                var el = this._ensureNoteHandle(handleIndex++);
                el.style.display = "block";
                el.style.left = Math.round(positions[p][0]) + "px";
                el.style.top = Math.round(positions[p][1]) + "px";
            }
            if (this._noteHandleRects) {
                for (var h = handleIndex; h < this._noteHandleRects.length; h++) {
                    this._noteHandleRects[h].style.display = "none";
                }
            }
        },

        /**
         * Mirror canvas.js: when the server sets mode=drag / mode=resize, draw ghosts on the
         * SVG effects layer (canvas.js paint sits under the SVG and is invisible).
         */
        _syncServerModePreviews: function (screenX, screenY, graphX, graphY, buttonsDown) {
            var mode = getWidgetData(this._canvasId, "mode");
            // Prefer local pointer flag: RAP often reports event.buttons === 0 while dragging.
            var held = buttonsDown || this._pointerHeld;
            if (mode === "resize" && held) {
                if (!this._noteResizeActive) {
                    this._beginNoteResize(screenX, screenY);
                }
                if (this._noteResizeActive) {
                    this._updateNoteResizePreview(screenX, screenY);
                    return true;
                }
            }
            if (mode === "drag" && held) {
                if (!this._dragActive) {
                    // Prefer icon-based drag when mousedown was on a node (grab offset accurate).
                    var props = this._getCanvasProps();
                    var nodes = getWidgetData(this._canvasId, "nodes") || {};
                    var down = this._lastMouseDownGraph || { x: graphX, y: graphY };
                    var hit = findNodeAt(nodes, down.x, down.y, props);
                    if (hit) {
                        this._beginDrag(hit.name, down.x, down.y, {
                            x: hit.node.x,
                            y: hit.node.y,
                            width: hit.width,
                            height: hit.height
                        });
                    } else {
                        // Notes-only (or miss): mode-drag uses mousedown as origin for dx/dy.
                        this._beginModeDrag(down.x, down.y);
                    }
                }
                if (this._dragActive) {
                    // One-shot re-capture if we never got note geometry (late notes[] / areas).
                    if (!this._hasSelectedDragNotes()) {
                        this._captureDragNotes();
                    }
                    this._updateDragPreview(graphX, graphY);
                    return true;
                }
            }
            return false;
        },

        _isPanGesture: function (event) {
            return event.button === 1
                || (event.button === 0 && (event.ctrlKey || event.metaKey));
        },

        _beginSelect: function (screenX, screenY) {
            this._selectActive = true;
            this._selectStartX = screenX;
            this._selectStartY = screenY;
            this._lastHoverKey = null;
            this._updateHoverChrome(null);
            document.addEventListener("mousemove", this._documentMouseMoveHandler);
            document.addEventListener("mouseup", this._mouseupHandler);
            if (this._canvas) {
                this._canvas.style.cursor = "crosshair";
            }
            this._updateSelectLasso(screenX, screenY);
        },

        _updateSelectLasso: function (screenX, screenY) {
            if (!this._selectActive || !this._selectLasso) {
                return;
            }
            var left = Math.min(this._selectStartX, screenX);
            var top = Math.min(this._selectStartY, screenY);
            var width = Math.abs(screenX - this._selectStartX);
            var height = Math.abs(screenY - this._selectStartY);
            this._selectLasso.style.display = "block";
            this._selectLasso.style.left = Math.round(left) + "px";
            this._selectLasso.style.top = Math.round(top) + "px";
            this._selectLasso.style.width = Math.round(width) + "px";
            this._selectLasso.style.height = Math.round(height) + "px";
        },

        _endSelect: function () {
            this._selectActive = false;
            if (this._selectLasso) {
                this._selectLasso.style.display = "none";
            }
            if (this._canvas && !this._panActive && !this._dragActive && !this._navDragActive) {
                this._canvas.style.cursor = "";
            }
        },

        _beginNavDrag: function (screenX, screenY, viewPort) {
            this._navDragActive = true;
            this._navDragStartX = screenX;
            this._navDragStartY = screenY;
            this._navDragBaseViewPort = viewPort;
            this._lastHoverKey = null;
            this._updateHoverChrome(null);
            document.addEventListener("mousemove", this._documentMouseMoveHandler);
            document.addEventListener("mouseup", this._mouseupHandler);
            this._updateNavPreview(screenX, screenY);
        },

        _endNavDrag: function () {
            this._navDragActive = false;
            this._navDragBaseViewPort = null;
            if (this._navViewportPreview) {
                this._navViewportPreview.style.display = "none";
            }
            if (this._canvas && !this._panActive && !this._dragActive) {
                this._canvas.style.cursor = "";
            }
        },

        _updateNavPreview: function (screenX, screenY) {
            if (!this._navDragActive || !this._navDragBaseViewPort || !this._navViewportPreview) {
                if (this._navViewportPreview) {
                    this._navViewportPreview.style.display = "none";
                }
                return;
            }

            var viewPort = this._navDragBaseViewPort;
            var props = this._getCanvasProps();
            var preview = clampNavPreviewRect(
                viewPort,
                props.graphPort,
                viewPort.x + (screenX - this._navDragStartX),
                viewPort.y + (screenY - this._navDragStartY));

            this._navViewportPreview.style.display = "block";
            this._navViewportPreview.style.left = Math.round(preview.x) + "px";
            this._navViewportPreview.style.top = Math.round(preview.y) + "px";
            this._navViewportPreview.style.width = Math.round(viewPort.width) + "px";
            this._navViewportPreview.style.height = Math.round(viewPort.height) + "px";
        },

        _handleMouseDown: function (event) {
            if (!this._canvas) {
                return;
            }
            var rect = this._canvas.getBoundingClientRect();
            var screenX = event.clientX - rect.left;
            var screenY = event.clientY - rect.top;
            // Prefer live widget props (includes mode/nodes) over last SVG snapshot props.
            var props = this._getCanvasProps();
            var graphProps = props.magnification != null ? props : (this._props || props);
            if (event.button === 0) {
                this._pointerHeld = true;
            }
            if (event.button === 0 && props.viewPort && containsRect(props.viewPort, screenX, screenY)) {
                this._beginNavDrag(screenX, screenY, props.viewPort);
                return;
            }
            var graph = graphCoords(screenX, screenY, graphProps);
            // Always remember mousedown for mode=drag / mode=resize previews after server arms mode.
            this._lastMouseDownGraph = { x: graph.x, y: graph.y };
            this._lastMouseDownScreen = { x: screenX, y: screenY };

            var iconArea = this._areas.length
                ? findIconAreaAt(this._areas, graph.x, graph.y)
                : null;
            var clickedName = iconArea ? iconOwnerName(iconArea) : null;

            // Fallback: hit-test nodes map (plugin model cards). Areas may be empty/stale while
            // nodes still carry correct logical positions and sizes.
            if (!clickedName) {
                var nodes = getWidgetData(this._canvasId, "nodes");
                var hit = findNodeAt(nodes, graph.x, graph.y, graphProps);
                if (hit) {
                    clickedName = hit.name;
                    iconArea = {
                        x: hit.node.x,
                        y: hit.node.y,
                        width: hit.width,
                        height: hit.height
                    };
                }
            }

            if (this._isPanGesture(event)) {
                if (!iconArea) {
                    this._beginPan(screenX, screenY);
                }
                return;
            }

            if (event.button === 0 && !iconArea) {
                // Note area: do not start lasso — server arms mode=drag / mode=resize; client
                // draws ghosts via _syncServerModePreviews once mode arrives.
                var noteArea = getVisibleArea(this._areas, graph.x, graph.y);
                if (noteArea && noteArea.areaType === "NOTE") {
                    this._ensureDocumentDragListeners();
                    return;
                }
                // Also treat notes[] hit when areas are empty/stale.
                var notes = getWidgetData(this._canvasId, "notes");
                if (notes && notes.length) {
                    for (var ni = 0; ni < notes.length; ni++) {
                        var note = notes[ni];
                        if (!note) {
                            continue;
                        }
                        if (graph.x >= note.x && graph.x < note.x + note.width
                            && graph.y >= note.y && graph.y < note.y + note.height) {
                            this._ensureDocumentDragListeners();
                            return;
                        }
                    }
                }
                if (!props.viewPort || !containsRect(props.viewPort, screenX, screenY)) {
                    this._beginSelect(screenX, screenY);
                }
                return;
            }

            if (event.button !== 0 || !clickedName || !iconArea) {
                return;
            }
            this._beginDrag(clickedName, graph.x, graph.y, iconArea);
            var self = this;
            setTimeout(function () {
                if (self._dragActive) {
                    self._captureDragNodes();
                    self._captureDragNotes();
                    self._updateDragPreview(graph.x, graph.y);
                }
            }, 0);
        },

        _handleDocumentMouseMove: function (event) {
            if (!this._canvas) {
                return;
            }
            var rect = this._canvas.getBoundingClientRect();
            var screenX = event.clientX - rect.left;
            var screenY = event.clientY - rect.top;
            var props = this._getCanvasProps();
            var graph = graphCoords(screenX, screenY, props);
            var buttonsDown = event.buttons === 1 || event.which === 1;
            var buttons = typeof event.buttons === "number" ? event.buttons : 0;
            var panButtonsHeld = (buttons & 4) !== 0 || (buttons & 1) !== 0;
            if (this._panActive) {
                var panMode = getWidgetData(this._canvasId, "mode");
                if (panMode !== "pan" && !panButtonsHeld) {
                    this._endPan();
                    this._restoreIdleChrome();
                    return;
                }
                this._computePanOffset(screenX, screenY);
                this._updatePanPreview();
                return;
            }
            if (this._navDragActive) {
                this._updateNavPreview(screenX, screenY);
                return;
            }
            if (this._selectActive) {
                this._updateSelectLasso(screenX, screenY);
                return;
            }
            if (this._noteResizeActive) {
                this._updateNoteResizePreview(screenX, screenY);
                return;
            }
            if (this._dragActive) {
                this._updateDragPreview(graph.x, graph.y);
                return;
            }
            // mode=drag / mode=resize may arrive after mousedown; pick up here while button held.
            this._syncServerModePreviews(screenX, screenY, graph.x, graph.y, buttonsDown);
        },

        _handleMouseUp: function (event) {
            this._pointerHeld = false;
            if (!this._panActive && !this._dragActive && !this._navDragActive
                && !this._selectActive && !this._noteResizeActive && !this._modeDragListening) {
                // Still force undim — covers lost mouseup during pan where flags already cleared.
                this._restoreIdleChrome();
                return;
            }
            document.removeEventListener("mousemove", this._documentMouseMoveHandler);
            document.removeEventListener("mouseup", this._mouseupHandler);
            document.removeEventListener("pointerup", this._mouseupHandler);
            document.removeEventListener("pointercancel", this._mouseupHandler);
            this._modeDragListening = false;
            if (this._panActive) {
                this._endPan();
            }
            if (this._navDragActive) {
                this._endNavDrag();
            }
            if (this._noteResizeActive) {
                this._endNoteResize();
            }
            if (this._dragActive) {
                this._endDrag();
            }
            if (this._selectActive) {
                this._endSelect();
            }
            this._restoreIdleChrome();
        },

        _handleMouseMove: function (event) {
            if (!this._canvas || !this._remoteObject) {
                return;
            }
            var rect = this._canvas.getBoundingClientRect();
            var screenX = event.clientX - rect.left;
            var screenY = event.clientY - rect.top;
            var props = this._getCanvasProps();
            var graphProps = props.magnification != null ? props : (this._props || props);
            var graph = graphCoords(screenX, screenY, graphProps);
            var buttonsDown = event.buttons === 1 || event.which === 1;
            var buttons = typeof event.buttons === "number" ? event.buttons : 0;
            // Middle = 4; left = 1. Ctrl-left pan uses left.
            var panButtonsHeld = (buttons & 4) !== 0 || (buttons & 1) !== 0;
            var mode = getWidgetData(this._canvasId, "mode");

            // Relationship hop rubber-band on the SVG effects layer (above SVG; canvas.js is under).
            this._updateHopLine(screenX, screenY);

            if (this._panActive) {
                // Server already cleared pan and no button held → end wireframe even if mouseup missed.
                if (mode !== "pan" && !panButtonsHeld) {
                    this._endPan();
                } else {
                    this._computePanOffset(screenX, screenY);
                    this._updatePanPreview();
                    return;
                }
            }
            if (this._navDragActive) {
                this._updateNavPreview(screenX, screenY);
                return;
            }
            if (this._selectActive) {
                this._updateSelectLasso(screenX, screenY);
                return;
            }
            if (this._noteResizeActive) {
                this._updateNoteResizePreview(screenX, screenY);
                return;
            }
            if (this._dragActive) {
                this._updateDragPreview(graph.x, graph.y);
                return;
            }
            // Server mode=drag / mode=resize: draw ghosts on effects layer (canvas.js is under SVG).
            if (this._syncServerModePreviews(screenX, screenY, graph.x, graph.y, buttonsDown)) {
                return;
            }
            if (props.viewPort && containsRect(props.viewPort, screenX, screenY)) {
                this._canvas.style.cursor = "grab";
                this._clearNoteResizeHandles();
                return;
            }
            if (mode === "pan" && !this._panActive) {
                this._beginPan(screenX, screenY);
                return;
            }
            // Idle with leftover dim (e.g. after pan + SVG refresh race).
            this._restoreIdleChrome();
            var area = this._areas.length
                ? getVisibleArea(this._areas, graph.x, graph.y)
                : null;
            this._updateHoverChrome(area, graph.x, graph.y);

            var hoverKey = area
                ? area.areaType + ":" + JSON.stringify(area.owner) + ":" + Math.round(graph.x / 4) + ":" + Math.round(graph.y / 4)
                : "";
            if (hoverKey === this._lastHoverKey) {
                return;
            }
            this._lastHoverKey = hoverKey;
            var interaction = hop._canvasInteractionInstance;
            if (area && area.hover && interaction && interaction._remoteObject) {
                interaction._remoteObject.notify("hover", {
                    canvasId: this._canvasId,
                    graphX: graph.x,
                    graphY: graph.y,
                    screenX: Math.round(screenX),
                    screenY: Math.round(screenY)
                });
            }
        }
    };

    rap.registerTypeHandler("hop.CanvasSvgRenderer", {
        factory: function (properties) {
            return new hop.CanvasSvgRenderer(properties);
        },
        destructor: "destroy",
        properties: ["sessionUuid", "canvasId", "renderRevision", "serviceHandlerUrl"],
        methods: ["attachListener"],
        events: ["hover"],
        propertyHandler: {
            sessionUuid: function (widget, value) {
                widget._sessionUuid = value;
            },
            canvasId: function (widget, value) {
                var changed = widget._canvasId !== value;
                widget._canvasId = value;
                // Tab switch: re-bind overlay to the newly active canvas and force SVG fetch.
                if (changed) {
                    widget._revision = 0;
                    if (widget._svgHost) {
                        widget._svgHost.innerHTML = "";
                    }
                    widget._findAndAttachCanvas();
                    widget._fetchAndRender(0);
                } else {
                    widget._findAndAttachCanvas();
                }
            },
            renderRevision: function (widget, value) {
                if (value !== widget._revision) {
                    widget._revision = 0;
                    widget._fetchAndRender(0);
                }
            },
            serviceHandlerUrl: function (widget, value) {
                widget._serviceHandlerUrl = value;
            }
        }
    });

    hop.CanvasInteraction = function (properties) {
        this._canvasId = properties.canvas;
        this._remoteObject = null;
        hop._canvasInteractionInstance = this;
    };

    hop.CanvasInteraction.prototype = {
        destroy: function () {
            if (hop._canvasInteractionInstance === this) {
                hop._canvasInteractionInstance = null;
            }
        },
        attachListener: function () {
            this._remoteObject = rap.getRemoteObject(this);
        }
    };

    rap.registerTypeHandler("hop.CanvasInteraction", {
        factory: function (properties) {
            return new hop.CanvasInteraction(properties);
        },
        destructor: "destroy",
        properties: ["canvas"],
        methods: ["attachListener"],
        events: ["hover"],
        propertyHandler: {
            canvas: function (widget, value) {
                widget._canvasId = value;
            }
        }
    });

})();