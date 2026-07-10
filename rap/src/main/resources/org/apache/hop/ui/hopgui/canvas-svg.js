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
        this._dragStartPositions = null;
        this._dragNodes = null;
        this._mousedownHandler = null;
        this._mouseupHandler = null;
        this._documentMouseMoveHandler = null;
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

                this._effectsLayer = document.createElement("div");
                this._effectsLayer.style.position = "absolute";
                this._effectsLayer.style.left = "0";
                this._effectsLayer.style.top = "0";
                this._effectsLayer.style.width = "100%";
                this._effectsLayer.style.height = "100%";
                this._effectsLayer.style.pointerEvents = "none";

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
                        }
                        if (self._canvas) {
                            self._syncOverlayLayout(self._canvas);
                        }
                    }
                })
                .catch(function (err) {
                    if (window.console && console.debug) {
                        console.debug("Hop canvas SVG fetch failed", err);
                    }
                });
        },

        _updateHoverChrome: function (area) {
            if (!this._canvas) {
                return;
            }
            if (this._panActive || this._navDragActive || this._dragActive) {
                this._canvas.style.cursor = "grabbing";
                return;
            }
            this._canvas.style.cursor = (area && area.hover) ? "pointer" : "";
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
            }
            this._endPan();
            this._endDrag();
            this._endNavDrag();
            this._endSelect();
        },

        _refreshPanBoundaries: function () {
            var props = this._getCanvasProps();
            if (props.panBoundaries) {
                this._panBounds = props.panBoundaries;
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
            if (this._svgHost) {
                this._svgHost.style.opacity = "0.35";
            }
            document.addEventListener("mousemove", this._documentMouseMoveHandler);
            document.addEventListener("mouseup", this._mouseupHandler);
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
            if (this._svgHost && !this._dragActive) {
                this._svgHost.style.opacity = "";
            }
            if (this._canvas && !this._dragActive) {
                this._canvas.style.cursor = "";
            }
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
                    extendGraphBounds(graphBounds, x, y, iconSize, iconSize);
                    hasBounds = true;
                    var screen = graphRectToScreen(
                        x - 1, y - 1, iconSize + 1, iconSize + 1, props, offsetX, offsetY);
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

        _beginDrag: function (clickedName, graphX, graphY, iconArea) {
            this._dragActive = true;
            this._dragClickedName = clickedName;
            this._dragIconOffset = {
                x: graphX - iconArea.x,
                y: graphY - iconArea.y
            };
            this._captureDragNodes();
            if (!this._dragStartPositions[clickedName]) {
                this._dragStartPositions[clickedName] = { x: iconArea.x, y: iconArea.y };
            }
            this._lastHoverKey = null;
            this._updateHoverChrome(null);
            if (this._svgHost) {
                this._svgHost.style.opacity = "0.35";
            }
            document.addEventListener("mousemove", this._documentMouseMoveHandler);
            document.addEventListener("mouseup", this._mouseupHandler);
            this._updateDragPreview(graphX, graphY);
        },

        _endDrag: function () {
            this._dragActive = false;
            this._dragClickedName = null;
            this._dragIconOffset = null;
            this._dragStartPositions = null;
            this._dragNodes = null;
            this._clearDragPreview();
            if (this._svgHost && !this._panActive) {
                this._svgHost.style.opacity = "";
            }
            if (this._canvas && !this._panActive) {
                this._canvas.style.cursor = "";
            }
        },

        _ensureDragPreviewRect: function (index) {
            if (!this._dragPreviewRects) {
                this._dragPreviewRects = [];
            }
            while (this._dragPreviewRects.length <= index) {
                var div = document.createElement("div");
                div.style.position = "absolute";
                div.style.boxSizing = "border-box";
                div.style.pointerEvents = "none";
                div.style.display = "none";
                div.style.backgroundColor = "transparent";
                div.style.borderRadius = "8px";
                this._effectsLayer.appendChild(div);
                this._dragPreviewRects.push(div);
            }
            return this._dragPreviewRects[index];
        },

        _clearDragPreview: function () {
            if (!this._dragPreviewRects) {
                return;
            }
            for (var i = 0; i < this._dragPreviewRects.length; i++) {
                this._dragPreviewRects[i].style.display = "none";
            }
        },

        _updateDragPreview: function (graphX, graphY) {
            if (!this._dragActive || !this._dragClickedName || !this._dragIconOffset
                || !this._dragStartPositions) {
                this._clearDragPreview();
                return;
            }

            var props = this._props || {};
            var iconSize = props.iconSize || 32;
            var gridSize = props.showGrid ? (props.gridSize || 1) : 1;
            var clickedStart = this._dragStartPositions[this._dragClickedName];
            if (!clickedStart) {
                this._clearDragPreview();
                return;
            }

            var iconTargetX = graphX - this._dragIconOffset.x;
            var iconTargetY = graphY - this._dragIconOffset.y;
            iconTargetX = snapToGrid(iconTargetX, gridSize);
            iconTargetY = snapToGrid(iconTargetY, gridSize);
            var dx = iconTargetX - clickedStart.x;
            var dy = iconTargetY - clickedStart.y;

            var selectedColor = "rgb(0, 93, 166)";
            var previewIndex = 0;
            var nodes = this._dragNodes;
            var names = nodes ? Object.keys(nodes) : [this._dragClickedName];

            for (var i = 0; i < names.length; i++) {
                var name = names[i];
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

                var x = start.x + dx;
                var y = start.y + dy;
                var screen = graphRectToScreen(x - 1, y - 1, iconSize + 1, iconSize + 1, props);
                var rect = this._ensureDragPreviewRect(previewIndex++);
                rect.style.display = "block";
                rect.style.left = Math.round(screen.left) + "px";
                rect.style.top = Math.round(screen.top) + "px";
                rect.style.width = Math.round(screen.width) + "px";
                rect.style.height = Math.round(screen.height) + "px";
                rect.style.border = (node && node.selected) || isClicked
                    ? "3px solid " + selectedColor
                    : "1px solid rgb(61, 99, 128)";
            }

            for (var j = previewIndex; j < this._dragPreviewRects.length; j++) {
                this._dragPreviewRects[j].style.display = "none";
            }
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
            var props = this._getCanvasProps();
            if (event.button === 0 && props.viewPort && containsRect(props.viewPort, screenX, screenY)) {
                this._beginNavDrag(screenX, screenY, props.viewPort);
                return;
            }
            var graph = graphCoords(screenX, screenY, this._props);
            var iconArea = this._areas.length
                ? findIconAreaAt(this._areas, graph.x, graph.y)
                : null;

            if (this._isPanGesture(event)) {
                if (!iconArea) {
                    this._beginPan(screenX, screenY);
                }
                return;
            }

            if (event.button === 0 && !iconArea) {
                if (!props.viewPort || !containsRect(props.viewPort, screenX, screenY)) {
                    this._beginSelect(screenX, screenY);
                }
                return;
            }

            if (event.button !== 0 || !this._areas.length || !iconArea) {
                return;
            }
            var clickedName = iconOwnerName(iconArea);
            if (!clickedName) {
                return;
            }
            this._beginDrag(clickedName, graph.x, graph.y, iconArea);
            var self = this;
            setTimeout(function () {
                if (self._dragActive) {
                    self._captureDragNodes();
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
            if (this._panActive) {
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
            if (this._dragActive) {
                var graph = graphCoords(screenX, screenY, this._props);
                this._updateDragPreview(graph.x, graph.y);
            }
        },

        _handleMouseUp: function (event) {
            if (!this._panActive && !this._dragActive && !this._navDragActive && !this._selectActive) {
                return;
            }
            document.removeEventListener("mousemove", this._documentMouseMoveHandler);
            document.removeEventListener("mouseup", this._mouseupHandler);
            if (this._panActive) {
                this._endPan();
            }
            if (this._navDragActive) {
                this._endNavDrag();
            }
            if (this._dragActive) {
                this._endDrag();
            }
            if (this._selectActive) {
                this._endSelect();
            }
        },

        _handleMouseMove: function (event) {
            if (!this._canvas || !this._remoteObject || !this._areas.length) {
                return;
            }
            var rect = this._canvas.getBoundingClientRect();
            var screenX = event.clientX - rect.left;
            var screenY = event.clientY - rect.top;
            var graph = graphCoords(screenX, screenY, this._props);
            if (this._panActive) {
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
            if (this._dragActive) {
                this._updateDragPreview(graph.x, graph.y);
                return;
            }
            var props = this._getCanvasProps();
            if (props.viewPort && containsRect(props.viewPort, screenX, screenY)) {
                this._canvas.style.cursor = "grab";
                return;
            }
            var panMode = getWidgetData(this._canvasId, "mode");
            if (panMode === "pan" && !this._panActive) {
                this._beginPan(screenX, screenY);
                return;
            }
            var area = getVisibleArea(this._areas, graph.x, graph.y);
            this._updateHoverChrome(area);

            var hoverKey = area ? area.areaType + ":" + JSON.stringify(area.owner) : "";
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
                widget._canvasId = value;
                widget._revision = 0;
                widget._findAndAttachCanvas();
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