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

    // Define the CanvasZoom constructor BEFORE registering the type handler
    hop.CanvasZoom = function(properties) {
        this._canvas = null;
        this._canvasId = properties.canvas; // This is the Canvas widget ID (Composite), not the actual canvas element
        this._remoteObject = null;
        this._wheelHandler = null;
        this._sizeCheckInterval = null;
        
        // DON'T attach in constructor - wait for explicit attachListener call from Java
        // This ensures the canvas is fully created and the remote object is ready
    };

    hop.CanvasZoom.prototype = {
        destroy: function() {
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
            var maxAttempts = 10;
            
            var tryFindCanvas = function() {
                attempts++;
                
                // Find the active/visible canvas - typically the one in the currently selected tab
                // Look for canvas elements that are large and visible (not hidden)
                var allCanvases = document.querySelectorAll('canvas');
                var canvas = null;
                
                for (var i = 0; i < allCanvases.length; i++) {
                    var c = allCanvases[i];
                    // Check if canvas is large enough (graph canvases are typically > 500px)
                    // and is visible (not display:none or visibility:hidden)
                    var rect = c.getBoundingClientRect();
                    if (rect.width > 500 && rect.height > 500 && 
                        c.offsetParent !== null) { // offsetParent is null if element or ancestor is hidden
                        canvas = c;
                        break;
                    }
                }
                
                if (!canvas) {
                    if (attempts < maxAttempts) {
                        setTimeout(tryFindCanvas, 100);
                        return;
                    } else {
                        return;
                    }
                }
                
                // Check if this is a different canvas than the one we already have
                if (self._canvas === canvas) {
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
                // When canvas property is updated from Java, update _canvasId
                widget._canvasId = value;
            }
        }
    });
    
})();
