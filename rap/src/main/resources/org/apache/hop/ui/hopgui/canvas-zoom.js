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

//# sourceURL=canvas-zoom.js
(function() {
    'use strict';

    // Ensure hop namespace exists FIRST
    if (!window.hop) {
        window.hop = {};
    }

    /**
     * RAP does not put widget ids on DOM elements unless enableUITests is on, so
     * document.getElementById(canvasId) is usually null. Do not guess by size: that
     * binds wheel zoom to a dialog canvas or to nothing when the graph is small.
     */
    function getWidgetDomElement(widgetId) {
        if (!widgetId) {
            return null;
        }
        try {
            if (typeof rap !== "undefined" && typeof rap.getObject === "function") {
                var proxy = rap.getObject(widgetId);
                if (proxy && proxy.$el) {
                    var queried = proxy.$el.get ? proxy.$el.get(0) : (proxy.$el[0] || proxy.$el);
                    if (queried && queried.tagName) {
                        return queried;
                    }
                }
            }
            if (typeof rwt !== "undefined" && rwt.remote && rwt.remote.ObjectRegistry) {
                var nativeWidget = rwt.remote.ObjectRegistry.getObject(widgetId);
                if (nativeWidget) {
                    if (typeof nativeWidget.getElement === "function") {
                        var element = nativeWidget.getElement();
                        if (element) {
                            return element;
                        }
                    }
                    if (typeof nativeWidget._getTargetNode === "function") {
                        var target = nativeWidget._getTargetNode();
                        if (target) {
                            return target;
                        }
                    }
                    if (nativeWidget._element) {
                        return nativeWidget._element;
                    }
                }
            }
        } catch (ignored) {
            // RAP has not registered this widget on the client yet.
        }
        return document.getElementById(widgetId);
    }

    function findCanvasForWidget(canvasId) {
        var widgetElement = getWidgetDomElement(canvasId);
        if (!widgetElement) {
            return null;
        }
        if (widgetElement.tagName === "CANVAS") {
            return widgetElement;
        }
        return widgetElement.querySelector("canvas");
    }

    // Define the CanvasZoom constructor BEFORE registering the type handler
    hop.CanvasZoom = function(properties) {
        properties = properties || {};
        this._canvas = null;
        this._canvasId = properties.canvas; // RAP Canvas widget id, not the HTML <canvas>
        this._remoteObject = null;
        this._wheelHandler = null;
        this._sizeCheckInterval = null;
        this._findTimer = null;
        this._destroyed = false;

        // DON'T attach in constructor - wait for explicit attachListener call from Java
        // This ensures the canvas is fully created and the remote object is ready
    };

    hop.CanvasZoom.prototype = {
        destroy: function() {
            this._destroyed = true;
            if (this._findTimer) {
                clearTimeout(this._findTimer);
                this._findTimer = null;
            }
            if (this._canvas && this._wheelHandler) {
                this._canvas.removeEventListener('wheel', this._wheelHandler);
            }
            if (this._sizeCheckInterval) {
                clearInterval(this._sizeCheckInterval);
                this._sizeCheckInterval = null;
            }
        },

        // Method called from Java backend to attach/reattach the listener
        attachListener: function() {
            this._findAndAttachCanvas();
        },
        
        // Fix for canvas shrinking at low zoom levels
        _applyCanvasSizeFix: function() {
            if (!this._canvas) return;
            
            // Check for scaling transforms on the canvas
            var computedStyle = window.getComputedStyle(this._canvas);
            var transform = computedStyle.transform;
            
            // Remove any transform that includes scaling
            if (transform && transform !== 'none') {
                // Parse matrix values to check for scaling
                var match = transform.match(/matrix\(([^,]+),\s*([^,]+),\s*([^,]+),\s*([^,]+)/);
                if (match) {
                    var scaleX = parseFloat(match[1]);
                    var scaleY = parseFloat(match[4]);
                    
                    // If there's scaling (not 1.0), remove the transform
                    if (Math.abs(scaleX - 1.0) > 0.01 || Math.abs(scaleY - 1.0) > 0.01) {
                        this._canvas.style.transform = 'none';
                    }
                }
            }
            
            // Ensure canvas fills its container
            this._canvas.style.width = '100%';
            this._canvas.style.height = '100%';
            this._canvas.style.display = 'block';
        },
        
        // Method called when the canvas property is updated from Java
        setCanvas: function(properties) {
            this._canvasId = properties.canvasId;
            this._findAndAttachCanvas();
        },

        _findAndAttachCanvas: function() {
            var self = this;
            var attempts = 0;
            if (this._findTimer) {
                clearTimeout(this._findTimer);
                this._findTimer = null;
            }

            var tryFindCanvas = function() {
                if (self._destroyed) {
                    return;
                }
                var canvas = findCanvasForWidget(self._canvasId);

                if (!canvas) {
                    attempts++;
                    var widgetPresent = !!getWidgetDomElement(self._canvasId);
                    var maxAttempts = widgetPresent ? 300 : 100;
                    if (self._canvasId && attempts < maxAttempts) {
                        self._findTimer = setTimeout(tryFindCanvas, 100);
                    }
                    return;
                }
                self._findTimer = null;
                
                // Same canvas element: still re-ensure the wheel listener (RAP may replace nodes).
                if (self._canvas === canvas && self._wheelHandler) {
                    self._applyCanvasSizeFix();
                    return;
                }
                
                // Remove old listener if switching to a different canvas
                if (self._canvas && self._wheelHandler) {
                    self._canvas.removeEventListener('wheel', self._wheelHandler);
                }
                
                // Setup successful - attach listener
                self._canvas = canvas;
                
                // Apply initial canvas sizing fix
                self._applyCanvasSizeFix();
                
                // Start periodic check to continuously remove any transforms
                // This catches transforms applied by RAP at any time, including initial load at low zoom
                if (!self._sizeCheckInterval) {
                    self._sizeCheckInterval = setInterval(function() {
                        if (self._canvas && !self._canvas.parentNode) {
                            self._canvas = null;
                            self._findAndAttachCanvas();
                            return;
                        }
                        self._applyCanvasSizeFix();
                    }, 200); // Check every 200ms
                }
                
                // Get or create remote object
                if (!self._remoteObject) {
                    self._remoteObject = rap.getRemoteObject(self);
                }
                
                // Create wheel handler if it doesn't exist
                if (!self._wheelHandler) {
                    self._wheelHandler = function(event) {
                        event.preventDefault();
                        event.stopPropagation();
                        
                        var count = event.deltaY < 0 ? 1 : -1;
                        var rect = self._canvas.getBoundingClientRect();
                        var x = event.clientX - rect.left;
                        var y = event.clientY - rect.top;
                        
                        self._remoteObject.notify("zoom", {
                            count: count,
                            x: Math.round(x),
                            y: Math.round(y)
                        });
                        
                        // Fix canvas sizing after zoom event
                        // This prevents the canvas from visually shrinking at low zoom levels
                        setTimeout(function() {
                            self._applyCanvasSizeFix();
                        }, 10);
                    };
                }
                
                // Attach the listener to the canvas
                self._canvas.addEventListener('wheel', self._wheelHandler, { passive: false });
            };
            
            // Start trying to find the canvas
            tryFindCanvas();
        }
    };
    
    // Register the type handler AFTER the class is fully defined
    rap.registerTypeHandler("hop.CanvasZoom", {
        factory: function(properties) {
            return new hop.CanvasZoom(properties);
        },
        destructor: "destroy",
        properties: ["canvas"],
        methods: ["attachListener"],
        events: ["zoom"],
        propertyHandler: {
            canvas: function(widget, value) {
                // When canvas property is updated from Java, re-attach wheel to that canvas.
                widget._canvasId = value;
                widget._findAndAttachCanvas();
            }
        }
    });
    
})();
