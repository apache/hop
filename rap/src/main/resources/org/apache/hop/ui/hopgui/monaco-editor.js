/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use it except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

"use strict";

(function() {
  "use strict";

  var MONACO_BASE = "rwt-resources/monaco";
  var DEBOUNCE_MS = 300;

  function getParentElement(parentId) {
    var obj = rap.getObject(parentId);
    if (obj) {
      if (obj.$el) {
        var el = obj.$el.get ? obj.$el.get(0) : (obj.$el[0] || obj.$el);
        if (el) return el;
      }
      if (obj.getDomNode) return obj.getDomNode();
      if (obj._domNode) return obj._domNode;
    }
    return document.getElementById(parentId);
  }

  function toMonacoLang(lang) {
    if (!lang) return "plaintext";
    var map = {
      json: "json",
      xml: "xml",
      javascript: "javascript",
      js: "javascript",
      html: "html",
      java: "java",
      sql: "sql",
      python: "python",
      py: "python",
      shell: "shell",
      bash: "shell",
      sh: "shell",
      bat: "bat",
      cmd: "bat",
      batch: "bat",
      yaml: "yaml",
      yml: "yaml",
      properties: "ini"
    };
    return map[String(lang).toLowerCase()] || "plaintext";
  }

  /** Monaco built-in themes: vs (light), vs-dark, hc-black */
  function normalizeTheme(theme) {
    if (!theme) return "vs";
    var t = String(theme).toLowerCase();
    if (t === "vs-dark" || t === "dark") return "vs-dark";
    if (t === "hc-black" || t === "hc") return "hc-black";
    return "vs";
  }

  rap.registerTypeHandler("hop.MonacoEditor", {
    factory: function(properties) {
      return new hop.MonacoEditor(properties);
    },
    destructor: function(widget) {
      widget._notifyFocus(false);
      widget._destroyed = true;
      if (widget._retryId) { clearInterval(widget._retryId); widget._retryId = null; }
      if (widget._debounceId) { clearTimeout(widget._debounceId); widget._debounceId = null; }
      if (widget._editor && typeof widget._editor.dispose === "function") {
        try { widget._editor.dispose(); } catch (e) { /* ignore */ }
        widget._editor = null;
      }
      if (widget._container && widget._container.parentNode) {
        widget._container.parentNode.removeChild(widget._container);
      }
    },
    properties: [ "content", "language", "readOnly", "theme" ],
    events: [ "contentChanged", "focusChanged", "selectionChanged", "findRequested", "executeRequested" ],
    methods: [ "setSelection", "insert" ]
  });

  rwt.define("hop");

  hop.MonacoEditor = function(properties) {
    this._content = properties.content != null ? properties.content : "";
    this._language = properties.language != null ? properties.language : "plaintext";
    this._readOnly = properties.readOnly === true;
    this._theme = normalizeTheme(properties.theme);
    this._editor = null;
    this._container = null;
    this._parentId = properties.parent;
    this._destroyed = false;
    this._settingFromServer = false;
    this._debounceId = null;
    this._retryId = null;
    this._scheduleCreate();
  };

  hop.MonacoEditor.prototype = {

    setContent: function(content) {
      this._content = content != null ? content : "";
      if (this._debounceId) {
        clearTimeout(this._debounceId);
        this._debounceId = null;
      }
      if (this._editor) {
        var model = this._editor.getModel();
        if (model && model.getValue() !== this._content) {
          this._settingFromServer = true;
          model.setValue(this._content);
          this._settingFromServer = false;
        }
      }
    },

    setLanguage: function(lang) {
      this._language = lang != null ? lang : "plaintext";
      if (this._editor && window.monaco && window.monaco.editor) {
        var model = this._editor.getModel();
        if (model) {
          window.monaco.editor.setModelLanguage(model, toMonacoLang(this._language));
        }
      }
    },

    setReadOnly: function(readOnly) {
      this._readOnly = readOnly === true;
      if (this._editor && typeof this._editor.updateOptions === "function") {
        this._editor.updateOptions({ readOnly: this._readOnly });
      }
    },

    setTheme: function(theme) {
      this._theme = normalizeTheme(theme);
      if (this._editor && window.monaco && window.monaco.editor) {
        window.monaco.editor.setTheme(this._theme);
      }
    },

    setSelection: function(properties) {
      if (!this._editor || !this._editor.getModel()) {
        return;
      }
      var model = this._editor.getModel();
      var start = properties && properties.start != null ? properties.start : 0;
      var end = properties && properties.end != null ? properties.end : start;
      start = Math.max(0, Math.min(start, model.getValueLength()));
      end = Math.max(start, Math.min(end, model.getValueLength()));
      var startPos = model.getPositionAt(start);
      var endPos = model.getPositionAt(end);
      var range = window.monaco.Range.fromPositions(startPos, endPos);
      this._editor.setSelection(range);
      this._editor.revealRangeInCenterIfOutsideViewport(range);
    },

    insert: function(properties) {
      if (!this._editor || !this._editor.getModel()) {
        return;
      }
      var model = this._editor.getModel();
      var text = properties && properties.text != null ? properties.text : "";
      var start;
      var end;
      if (properties && properties.start != null) {
        start = Math.max(0, Math.min(properties.start, model.getValueLength()));
        end = properties.end != null
          ? Math.max(start, Math.min(properties.end, model.getValueLength()))
          : start;
      } else {
        var selection = this._editor.getSelection();
        if (!selection) {
          return;
        }
        start = model.getOffsetAt({
          lineNumber: selection.startLineNumber,
          column: selection.startColumn
        });
        end = model.getOffsetAt({
          lineNumber: selection.endLineNumber,
          column: selection.endColumn
        });
      }
      var range = window.monaco.Range.fromPositions(
        model.getPositionAt(start),
        model.getPositionAt(end)
      );
      this._editor.executeEdits("hop-find-replace", [{
        range: range,
        text: text,
        forceMoveMarkers: true
      }]);
    },

    _notifyServer: function() {
      if (this._destroyed || !this._editor || this._readOnly) return;
      var value = this._editor.getValue();
      try {
        var remote = rap.getRemoteObject(this);
        if (remote) {
          remote.notify("contentChanged", { content: value });
        }
      } catch (e) {
        console.warn("MonacoEditor: failed to notify server", e);
      }
    },

    _notifyFocus: function(focused) {
      try {
        var remote = rap.getRemoteObject(this);
        if (remote) {
          remote.notify("focusChanged", { focused: focused });
        }
      } catch (e) {
        console.warn("MonacoEditor: failed to notify focus", e);
      }
    },

    _notifySelection: function() {
      if (this._destroyed || !this._editor || !this._editor.getModel()) return;
      try {
        var model = this._editor.getModel();
        var sel = this._editor.getSelection();
        if (!sel) return;
        var start = model.getOffsetAt({
          lineNumber: sel.startLineNumber,
          column: sel.startColumn
        });
        var end = model.getOffsetAt({
          lineNumber: sel.endLineNumber,
          column: sel.endColumn
        });
        var remote = rap.getRemoteObject(this);
        if (remote) {
          remote.notify("selectionChanged", { start: start, end: end });
        }
      } catch (e) {
        console.warn("MonacoEditor: failed to notify selection", e);
      }
    },

    _notifyFind: function(replace) {
      if (this._destroyed) return;
      try {
        var remote = rap.getRemoteObject(this);
        if (remote) {
          remote.notify("findRequested", { replace: !!replace });
        }
      } catch (e) {
        console.warn("MonacoEditor: failed to notify find", e);
      }
    },

    _notifyExecute: function() {
      if (this._destroyed) return;
      try {
        var remote = rap.getRemoteObject(this);
        if (remote) {
          remote.notify("executeRequested", {});
        }
      } catch (e) {
        console.warn("MonacoEditor: failed to notify execute", e);
      }
    },

    _scheduleNotify: function() {
      var self = this;
      if (self._debounceId) clearTimeout(self._debounceId);
      self._debounceId = setTimeout(function() {
        self._debounceId = null;
        self._notifyServer();
      }, DEBOUNCE_MS);
    },

    _scheduleCreate: function() {
      var self = this;
      function tryCreate() {
        if (self._destroyed) return true;
        var parentEl = getParentElement(self._parentId);
        if (!parentEl) return false;
        self._initEditor(parentEl);
        return true;
      }
      if (!tryCreate()) {
        var attempt = 0;
        this._retryId = setInterval(function() {
          if (tryCreate() || ++attempt > 50) {
            clearInterval(self._retryId);
            self._retryId = null;
          }
        }, 100);
      }
    },

    _initEditor: function(parentEl) {
      var self = this;

      var container = document.createElement("div");
      container.style.width = "100%";
      container.style.height = "100%";
      container.style.position = "relative";
      container.style.minHeight = "100px";
      parentEl.appendChild(container);
      self._container = container;

      if (window.require && window.require.config) {
        window.require.config({
          paths: { vs: MONACO_BASE + "/vs" },
          "vs/nls": { availableLanguages: {} }
        });
      }

      window.require(["vs/editor/editor.main"], function() {
        if (self._destroyed || !self._container || !self._container.parentNode) return;

        var langForEditor = toMonacoLang(self._language);
        self._editor = window.monaco.editor.create(container, {
          value: self._content,
          language: langForEditor,
          theme: self._theme,
          readOnly: self._readOnly,
          automaticLayout: true,
          scrollBeyondLastLine: false,
          minimap: { enabled: true },
          fontSize: 14
        });

        self._editor.getModel().onDidChangeContent(function() {
          if (self._settingFromServer) return;
          self._scheduleNotify();
        });

        self._editor.onDidChangeCursorSelection(function() {
          self._notifySelection();
        });

        if (window.monaco && window.monaco.KeyMod && window.monaco.KeyCode) {
          self._editor.addCommand(
            window.monaco.KeyMod.CtrlCmd | window.monaco.KeyCode.KeyF,
            function() { self._notifyFind(false); }
          );
          self._editor.addCommand(
            window.monaco.KeyMod.CtrlCmd | window.monaco.KeyCode.KeyH,
            function() { self._notifyFind(true); }
          );
          self._editor.addCommand(
            window.monaco.KeyMod.CtrlCmd | window.monaco.KeyCode.Enter,
            function() { self._notifyExecute(); }
          );
        }

        self._editor.onDidFocusEditorWidget(function() {
          self._notifyFocus(true);
        });

        self._editor.onDidBlurEditorWidget(function() {
          self._notifyFocus(false);
          if (self._debounceId) {
            clearTimeout(self._debounceId);
            self._debounceId = null;
          }
          self._notifyServer();
        });
      });
    }
  };
})();
