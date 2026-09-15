/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//# sourceURL=context-dialog-svg.js
(function () {
    "use strict";

    if (!window.hop) {
        window.hop = {};
    }

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
        } catch (ignored) {}
        return document.getElementById(widgetId);
    }

    function findCanvasElement(canvasId) {
        var el = getWidgetDomElement(canvasId);
        if (!el) {
            return null;
        }
        if (el.tagName === "CANVAS") {
            return el;
        }
        return el.querySelector("canvas") || el;
    }

    hop.ContextDialogSvgRenderer = function (properties) {
        this._canvasId = properties.canvasId;
        this._tooltipId = properties.tooltipId;
        this._svg = null;
        this._areas = [];
        this._contentHeight = 0;
        this._selectedTooltip = "";
        this._overlay = null;
        this._hoverBox = null;
        this._canvas = null;
        this._mousemoveHandler = null;
        this._mouseleaveHandler = null;
        this._findTimer = null;
    };

    hop.ContextDialogSvgRenderer.prototype = {
        destroy: function () {
            if (this._findTimer) {
                clearTimeout(this._findTimer);
                this._findTimer = null;
            }
            if (this._canvas && this._mousemoveHandler) {
                this._canvas.removeEventListener("mousemove", this._mousemoveHandler);
            }
            if (this._canvas && this._mouseleaveHandler) {
                this._canvas.removeEventListener("mouseleave", this._mouseleaveHandler);
            }
            if (this._overlay && this._overlay.parentNode) {
                this._overlay.parentNode.removeChild(this._overlay);
            }
            this._overlay = null;
            this._hoverBox = null;
            this._canvas = null;
        },

        setCanvasId: function (properties) {
            this._canvasId = properties.value;
            this._attach();
        },

        setTooltipId: function (properties) {
            this._tooltipId = properties.value;
        },

        setSelectedTooltip: function (properties) {
            this._selectedTooltip = properties.value || "";
            this._updateTooltipText(this._selectedTooltip);
        },

        setSvg: function (properties) {
            this._svg = properties.value;
            this._render();
        },

        setAreas: function (properties) {
            this._areas = properties.value || [];
            if (this._hoverBox) {
                this._hoverBox.style.display = "none";
            }
        },

        setContentHeight: function (properties) {
            this._contentHeight = properties.value || 0;
            if (this._overlay) {
                this._overlay.style.height = this._contentHeight + "px";
                var svgEl = this._overlay.querySelector("svg");
                if (svgEl && this._contentHeight > 0) {
                    svgEl.setAttribute("height", String(this._contentHeight));
                }
            }
        },

        _attach: function () {
            var self = this;
            var attempts = 0;
            var find = function () {
                var target = findCanvasElement(self._canvasId);
                if (target && target.parentNode) {
                    self._canvas = target;
                    self._initOverlay();
                    self._render();
                } else if (attempts++ < 30) {
                    self._findTimer = setTimeout(find, 50);
                }
            };
            find();
        },

        _initOverlay: function () {
            if (this._overlay || !this._canvas) {
                return;
            }
            var container = this._canvas;
            if (container.tagName === "CANVAS" && container.parentElement) {
                container = container.parentElement;
            }
            if (window.getComputedStyle(container).position === "static") {
                container.style.position = "relative";
            }

            this._overlay = document.createElement("div");
            this._overlay.setAttribute("data-hop-context-dialog-svg", "true");
            this._overlay.style.position = "absolute";
            this._overlay.style.left = "0px";
            this._overlay.style.top = "0px";
            this._overlay.style.width = "100%";
            this._overlay.style.pointerEvents = "none";
            this._overlay.style.zIndex = "10";

            this._hoverBox = document.createElement("div");
            this._hoverBox.style.position = "absolute";
            this._hoverBox.style.display = "none";
            this._hoverBox.style.pointerEvents = "none";
            this._hoverBox.style.boxSizing = "border-box";
            this._hoverBox.style.border = "2px solid #005da6";
            this._hoverBox.style.borderRadius = "4px";
            this._hoverBox.style.backgroundColor = "rgba(201, 232, 251, 0.25)";
            this._overlay.appendChild(this._hoverBox);

            container.appendChild(this._overlay);

            var self = this;
            this._mousemoveHandler = function (e) {
                self._handleMouseMove(e);
            };
            this._mouseleaveHandler = function () {
                self._handleMouseLeave();
            };

            this._canvas.addEventListener("mousemove", this._mousemoveHandler);
            this._canvas.addEventListener("mouseleave", this._mouseleaveHandler);
        },

        _render: function () {
            if (!this._overlay || !this._svg) {
                return;
            }
            var existingSvg = this._overlay.querySelector("svg");
            if (existingSvg) {
                this._overlay.removeChild(existingSvg);
            }
            var temp = document.createElement("div");
            temp.innerHTML = this._svg;
            var svgEl = temp.querySelector("svg");
            if (svgEl) {
                svgEl.style.display = "block";
                svgEl.style.pointerEvents = "none";
                if (this._contentHeight <= 0) {
                    var hAttr = parseInt(svgEl.getAttribute("height") || "0", 10);
                    if (hAttr > 0) {
                        this._contentHeight = hAttr;
                    }
                }
                if (this._contentHeight > 0) {
                    svgEl.setAttribute("height", String(this._contentHeight));
                    this._overlay.style.height = this._contentHeight + "px";
                }
                svgEl.setAttribute("preserveAspectRatio", "none");
                this._overlay.insertBefore(svgEl, this._hoverBox);
            }
        },

        _handleMouseMove: function (event) {
            if (!this._areas || this._areas.length === 0 || !this._canvas) {
                return;
            }
            var rect = this._canvas.getBoundingClientRect();
            var x = event.clientX - rect.left;
            var y = event.clientY - rect.top;

            var hitItem = null;
            for (var i = this._areas.length - 1; i >= 0; i--) {
                var a = this._areas[i];
                if (x >= a.x && x < a.x + a.width && y >= a.y && y < a.y + a.height) {
                    if (a.owner && a.owner.kind === "contextItem") {
                        hitItem = a;
                        break;
                    }
                }
            }

            if (hitItem && this._hoverBox) {
                var m = 4;
                this._hoverBox.style.left = (hitItem.x - m) + "px";
                this._hoverBox.style.top = (hitItem.y - m) + "px";
                this._hoverBox.style.width = (hitItem.width + 2 * m) + "px";
                this._hoverBox.style.height = (hitItem.height + 2 * m) + "px";
                this._hoverBox.style.display = "block";
                this._updateTooltipText(hitItem.owner.tooltip || "");
            } else {
                this._handleMouseLeave();
            }
        },

        _handleMouseLeave: function () {
            if (this._hoverBox) {
                this._hoverBox.style.display = "none";
            }
            this._updateTooltipText(this._selectedTooltip || "");
        },

        _updateTooltipText: function (text) {
            if (!this._tooltipId) {
                return;
            }
            var el = getWidgetDomElement(this._tooltipId);
            if (el) {
                el.textContent = text || "";
            }
        }
    };

    rap.registerTypeHandler("hop.ContextDialogSvgRenderer", {
        factory: function (properties) {
            return new hop.ContextDialogSvgRenderer(properties);
        },
        destructor: "destroy",
        properties: ["canvasId", "tooltipId", "contentHeight", "svg", "areas", "selectedTooltip"],
        propertyHandler: {
            canvasId: function (widget, value) {
                widget.setCanvasId({ value: value });
            },
            tooltipId: function (widget, value) {
                widget.setTooltipId({ value: value });
            },
            contentHeight: function (widget, value) {
                widget.setContentHeight({ value: value });
            },
            svg: function (widget, value) {
                widget.setSvg({ value: value });
            },
            areas: function (widget, value) {
                widget.setAreas({ value: value });
            },
            selectedTooltip: function (widget, value) {
                widget.setSelectedTooltip({ value: value });
            }
        }
    });
})();
