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

/*
 * The Hop Web log view (see WebLogConsole.java): a scrolling <div> of lines inside a RAP
 * composite. The server sends the new lines only; this side keeps at most maxLines of them,
 * colours error lines, marks the filter term, follows the tail unless the user scrolled up and
 * reports what the user selected so "copy selection" on the server works.
 */
(function () {
    "use strict";

    var SELECTION_DEBOUNCE_MS = 250;

    function getParentElement(parentId) {
        var obj = rap.getObject(parentId);
        if (obj) {
            if (obj.$el) {
                var el = obj.$el.get ? obj.$el.get(0) : (obj.$el[0] || obj.$el);
                if (el) {
                    return el;
                }
            }
            if (obj.getDomNode) {
                return obj.getDomNode();
            }
            if (obj._domNode) {
                return obj._domNode;
            }
        }
        return document.getElementById(parentId);
    }

    rwt.define("hop");

    hop.LogConsole = function (properties) {
        this._parentId = properties.parent;
        this._maxLines = properties.maxLines > 0 ? properties.maxLines : 20000;
        this._maxSelection = properties.maxSelection > 0 ? properties.maxSelection : 65536;
        this._highlight = properties.highlight || "";
        this._highlightCaseSensitive = properties.highlightCaseSensitive === true;
        this._font = properties.font || "";
        this._color = properties.color || "";
        this._container = null;
        this._lineCount = 0;
        this._followTail = true;
        this._pending = [];
        this._retryId = null;
        this._selectionTimer = null;
        this._lastReportedSelection = "";
        this._destroyed = false;
        this._mount();
    };

    hop.LogConsole.prototype = {

        _mount: function () {
            var self = this;
            var parentEl = getParentElement(this._parentId);
            if (!parentEl) {
                // The composite's element is created lazily; try again shortly.
                if (!this._retryId) {
                    this._retryId = setInterval(function () {
                        if (self._destroyed) {
                            clearInterval(self._retryId);
                            self._retryId = null;
                            return;
                        }
                        if (getParentElement(self._parentId)) {
                            clearInterval(self._retryId);
                            self._retryId = null;
                            self._mount();
                        }
                    }, 100);
                }
                return;
            }
            var container = document.createElement("div");
            container.setAttribute("data-hop-log-console", "true");
            var style = container.style;
            style.position = "absolute";
            style.left = "0";
            style.top = "0";
            style.right = "0";
            style.bottom = "0";
            style.overflow = "auto";
            style.whiteSpace = "pre";
            style.font = this._font || "inherit";
            style.color = this._color || "inherit";
            style.background = "transparent";
            style.cursor = "text";
            style.userSelect = "text";
            style.webkitUserSelect = "text";
            style.padding = "2px 4px";
            style.boxSizing = "border-box";
            parentEl.appendChild(container);
            this._container = container;
            // RAP cancels native text selection unless the widget under the mouse is selectable.
            try {
                var widget = rwt.remote.ObjectRegistry.getObject(this._parentId);
                if (widget && typeof widget.setSelectable === "function") {
                    widget.setSelectable(true);
                }
            } catch (ignored) {}

            container.addEventListener("scroll", function () {
                self._followTail =
                    container.scrollTop + container.clientHeight >= container.scrollHeight - 4;
            });
            this._selectionHandler = function () {
                self._scheduleSelectionReport();
            };
            document.addEventListener("selectionchange", this._selectionHandler);

            var pending = this._pending;
            this._pending = [];
            for (var i = 0; i < pending.length; i++) {
                this.append(pending[i]);
            }
        },

        destroy: function () {
            this._destroyed = true;
            if (this._retryId) {
                clearInterval(this._retryId);
                this._retryId = null;
            }
            if (this._selectionTimer) {
                clearTimeout(this._selectionTimer);
                this._selectionTimer = null;
            }
            if (this._selectionHandler) {
                document.removeEventListener("selectionchange", this._selectionHandler);
                this._selectionHandler = null;
            }
            if (this._container && this._container.parentNode) {
                this._container.parentNode.removeChild(this._container);
            }
            this._container = null;
        },

        setMaxLines: function (value) {
            this._maxLines = value > 0 ? value : 20000;
            this._trim();
        },

        setMaxSelection: function (value) {
            this._maxSelection = value > 0 ? value : 65536;
        },

        setHighlight: function (value) {
            this._highlight = value || "";
        },

        setFont: function (value) {
            this._font = value || "";
            if (this._container) {
                this._container.style.font = this._font || "inherit";
            }
        },

        setColor: function (value) {
            this._color = value || "";
            if (this._container) {
                this._container.style.color = this._color || "inherit";
            }
        },

        setHighlightCaseSensitive: function (value) {
            this._highlightCaseSensitive = value === true;
        },

        /** Server call: lines and their error flags, in order. */
        append: function (params) {
            if (!this._container) {
                this._pending.push(params);
                return;
            }
            var lines = params.lines || [];
            var errors = params.errors || [];
            var fragment = document.createDocumentFragment();
            for (var i = 0; i < lines.length; i++) {
                fragment.appendChild(this._lineElement(lines[i], errors[i] === true));
            }
            this._container.appendChild(fragment);
            this._lineCount += lines.length;
            this._trim();
            if (this._followTail) {
                this._container.scrollTop = this._container.scrollHeight;
            }
        },

        clear: function () {
            this._pending = [];
            this._lineCount = 0;
            this._followTail = true;
            if (this._container) {
                this._container.textContent = "";
            }
        },

        /** Server call: select a character range, offsets into the text as the server holds it. */
        select: function (params) {
            if (!this._container || !window.getSelection) {
                return;
            }
            var start = params.start || 0;
            var end = params.end || start;
            var range = document.createRange();
            var startPoint = this._pointAt(start);
            var endPoint = this._pointAt(end);
            if (!startPoint || !endPoint) {
                return;
            }
            range.setStart(startPoint.node, startPoint.offset);
            range.setEnd(endPoint.node, endPoint.offset);
            var selection = window.getSelection();
            selection.removeAllRanges();
            selection.addRange(range);
            var line = startPoint.node.parentNode;
            if (line && line.scrollIntoView) {
                line.scrollIntoView({ block: "center" });
            }
        },

        _lineElement: function (text, error) {
            var line = document.createElement("div");
            if (error) {
                line.style.color = "#d0021b";
            }
            var term = this._highlight;
            if (term) {
                this._fillWithMarks(line, text, term);
            } else {
                line.textContent = text;
            }
            // Keep the text node count predictable for select(): one line = one <div>.
            if (!line.firstChild) {
                line.appendChild(document.createTextNode(""));
            }
            return line;
        },

        _fillWithMarks: function (line, text, term) {
            var haystack = this._highlightCaseSensitive ? text : text.toLowerCase();
            var needle = this._highlightCaseSensitive ? term : term.toLowerCase();
            var from = 0;
            var idx = haystack.indexOf(needle, from);
            if (idx < 0) {
                line.textContent = text;
                return;
            }
            while (idx >= 0) {
                if (idx > from) {
                    line.appendChild(document.createTextNode(text.substring(from, idx)));
                }
                var mark = document.createElement("mark");
                mark.textContent = text.substring(idx, idx + needle.length);
                line.appendChild(mark);
                from = idx + needle.length;
                idx = haystack.indexOf(needle, from);
            }
            if (from < text.length) {
                line.appendChild(document.createTextNode(text.substring(from)));
            }
        },

        _trim: function () {
            if (!this._container) {
                return;
            }
            while (this._lineCount > this._maxLines && this._container.firstChild) {
                this._container.removeChild(this._container.firstChild);
                this._lineCount--;
            }
        },

        /** The text node and offset for a character offset into the joined text (lines + "\n"). */
        _pointAt: function (offset) {
            var line = this._container.firstChild;
            var remaining = offset;
            while (line) {
                var length = line.textContent.length;
                if (remaining <= length) {
                    return this._pointInLine(line, remaining);
                }
                remaining -= length + 1;
                line = line.nextSibling;
            }
            var last = this._container.lastChild;
            return last ? this._pointInLine(last, last.textContent.length) : null;
        },

        _pointInLine: function (line, offset) {
            var node = line.firstChild;
            var remaining = offset;
            while (node) {
                var text = node.nodeType === 3 ? node : node.firstChild;
                var length = text ? text.textContent.length : 0;
                if (remaining <= length) {
                    return { node: text || line, offset: text ? remaining : 0 };
                }
                remaining -= length;
                node = node.nextSibling;
            }
            var lastText = line.lastChild && line.lastChild.nodeType === 3
                ? line.lastChild : (line.lastChild ? line.lastChild.firstChild : null);
            return { node: lastText || line, offset: lastText ? lastText.textContent.length : 0 };
        },

        _scheduleSelectionReport: function () {
            var self = this;
            if (this._selectionTimer) {
                clearTimeout(this._selectionTimer);
            }
            this._selectionTimer = setTimeout(function () {
                self._selectionTimer = null;
                self._reportSelection();
            }, SELECTION_DEBOUNCE_MS);
        },

        _reportSelection: function () {
            if (this._destroyed || !this._container || !window.getSelection) {
                return;
            }
            var selection = window.getSelection();
            var text = "";
            if (selection && selection.rangeCount > 0 && !selection.isCollapsed) {
                var range = selection.getRangeAt(0);
                if (this._container.contains(range.commonAncestorContainer)) {
                    text = selection.toString();
                }
            }
            if (text.length > this._maxSelection) {
                text = text.substring(0, this._maxSelection);
            }
            if (text === this._lastReportedSelection) {
                return;
            }
            this._lastReportedSelection = text;
            var remoteObject = rap.getRemoteObject(this);
            if (remoteObject) {
                remoteObject.notify("selectionChanged", { text: text });
            }
        }
    };

    rap.registerTypeHandler("hop.LogConsole", {
        factory: function (properties) {
            return new hop.LogConsole(properties);
        },
        destructor: "destroy",
        properties: [ "maxLines", "maxSelection", "highlight", "highlightCaseSensitive", "font", "color" ],
        events: [ "selectionChanged" ],
        methods: [ "append", "clear", "select" ]
    });
})();
