<!--
SPDX-License-Identifier: Apache-2.0

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Agent Guide for hop

This file is read by automated agents (security scanners, code analyzers,
AI assistants) and human contributors working on this repository. Keep it
concise — we will expand the sections below over time.

## Build & test

- **Requirements:** Java 21 (JDK) and Maven 3.6.3+ — or use the bundled wrapper
  `./mvnw` (no local Maven needed).
- **Build + unit tests:** `./mvnw clean install` (build with Java 21, otherwise
  tests are skipped). On Linux wrap a desktop run with
  `./tools/with-isolated-display.sh` so SWT UI tests do not steal the
  interactive session. `-Pskip-uitest` excludes them; `-Puitest` runs *only*
  those tests.

## Conventions

- **Formatting:** code is formatted with Spotless. Run `./mvnw spotless:apply`
  before committing; CI runs `spotless:check`.
- **License headers:** every new source/text file needs the Apache License 2.0
  header — Apache RAT fails the build otherwise.
- **Contributing & PRs:** see [CONTRIBUTING.md](./CONTRIBUTING.md).

## Security

Security model: [SECURITY.md](./SECURITY.md)

Agents that scan this repository should consult `SECURITY.md` and the
threat model it links before reporting issues.

# Apache Commons VFS

Unless there really is no other choice, files are read from or written to using Apache Commons VFS, HopVfs.getInputStream() or HopVfs.getOutputStream().  Use of java.io.File is reserved for rare and exceptional cases. The default is to use org.apache.commons.vfs2.FileObject instead.

# Build Apache Hop with isolated display

When building the `ui` maven component make sure to do this with an isolated display to prevent instantiating focus-stealing UI artifacts on the developers display. Use tools/with-isolated-display.sh to do this, for example:

```
tools/with-isolated-display.sh mvn -T10 -pl ui clean install
```

or a full build of the Apache Hop assembly

```
tools/with-isolated-display.sh mvn -T10 clean install
```

# GUI-backed features

We want to avoid building any functionality that is only expressed in a file and not in a GUI element.

# Apache Hop GUI dialogs

When building or changing a transform, action, metadata, or run-configuration dialog in Apache Hop (core or a plugin), **do not hand-layout FormAttachment rows on the shell first**. Start with annotated metadata widgets and a grouped `GuiCompositeWidgets` container.

## Why groups

A flat `createCompositeWidgets` / `addScrolledComposite` form cannot simultaneously:

- keep the dialog resizable
- keep OK/Cancel pinned to the bottom of the shell
- keep those buttons strictly below the last widget

The fields and the button bar fight for the same vertical space, so the buttons overlap the lower widgets. Putting `@GuiWidgetElement` fields in a **group** makes `GuiCompositeWidgets` build a container that fills the area between the name/header line and the button bar, with a **scrolled composite inside** (tabs, boxes, or list). Resize works; buttons stay put; overflow scrolls.

## Default recipe

1. Annotate the Meta (or equivalent) class with `@GuiPlugin` and Lombok `@Getter` / `@Setter`.
2. Give the dialog a `GUI_PLUGIN_ELEMENT_PARENT_ID` constant.
3. Annotate every user-visible field with `@HopMetadataProperty` **and** `@GuiWidgetElement`.
4. Put every field in a group. Repeat `groupType` on each field (the first non-`NONE` value wins; mixed types fall back to tabs).
   One options panel: `groupType = GuiWidgetGroupType.BOXES` and a single `group` (see `DataSetOutputMeta`).
   Several sections: `GuiWidgetGroupType.TABS` with distinct `group` names and `groupOrder`.
5. In the dialog `open()`: `createShell(...)`, `buildButtonBar().ok(...).cancel(...).build()`, then `GuiCompositeWidgets.addScrolledComposite(shell, variables, wTransformName, wOk, PARENT_ID, input)`.
6. On OK, `widgets.getWidgetsContents(input, PARENT_ID)` and set the transform/action name. Do not copy fields by hand.

```java
@GuiWidgetElement(
    id = WIDGET_DATA_SET_NAME,
    order = "0100",
    type = GuiElementType.METADATA,
    metadata = DataSet.class,
    label = "i18n::DataSetOutputMeta.DataSetName.Label",
    toolTip = "i18n::DataSetOutputMeta.DataSetName.Tooltip",
    parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
    groupType = GuiWidgetGroupType.BOXES,
    group = "Data Set")
@HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.PIPELINE_DATA_SET)
private String dataSetName;
```

Use `GuiElementType` that matches the value: `METADATA`, `FOLDER`, `FILENAME`, `TEXT`, `CHECKBOX`, `COMBO`, and so on. Labels and tooltips live in the Meta's `messages_en_US.properties`; quote variable examples as `'${HOP_DATASETS_FOLDER}'`.

## When not to flatten the form

Hand-built SWT is for things annotations cannot express (canvas, custom painters). A `TableView` or similar extra composite belongs in the same grouped layout via `GuiCompositeWidgets.registerExtraGroup(...)`, not as a free-floating control under the button bar.

Listeners (`IGuiPluginCompositeWidgetsListener`) are for enable/disable and `setChanged()`, not for creating the fields.

# Hop i18n resource bundles

Values defined in resource bundles (resource files in `messages/messages\*.properties` files) need to be properly escaped and quoted.  This means that variables expressions like `${VARIABLE}` really need to be surrounded with single quotes like this: `'${VARIABLE}'`.

# Lombok

Apache Hop and related projects should use Lombok for all classes to avoid cluttering classes with boilerplate getter/setter methods.

