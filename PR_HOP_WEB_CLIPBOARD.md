<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# PR: Fix pipeline and workflow clipboard shortcuts in Hop Web

## Suggested PR title

```text
Fix pipeline and workflow clipboard operations in Hop Web
```

## Suggested PR description

### Summary

This change fixes copy, cut, and paste operations for pipeline and workflow
graph elements in Hop Web.

In the RAP application, `GuiResource` attempted to use the SWT
`Clipboard` for both writes and reads. Reading the clipboard in Hop Web could
fail because the RAP request did not have the desktop SWT `Display` expected by
the clipboard implementation:

```text
NullPointerException: Cannot invoke
"org.eclipse.swt.widgets.Display.readAndDispatch()"
because "this.display" is null
```

As a result:

- `Ctrl+C` followed by `Ctrl+V` displayed an exception instead of duplicating
  the selected graph elements.
- `Ctrl+X` could delete selected transforms or actions even when the clipboard
  copy failed.
- A subsequent `Ctrl+V` could not restore the deleted selection.

### Root cause

Desktop Hop and Hop Web followed the same SWT clipboard path, although their
clipboard lifecycles are different.

Desktop Hop owns an SWT `Display` and can use `org.eclipse.swt.dnd.Clipboard`.
Hop Web runs on RAP and its `GuiResource` is scoped to the RAP UI session. The
web path therefore cannot depend on a desktop `Display` to retrieve graph XML.

### Changes

- Added a small session-local `WebClipboard` value holder.
- Made `GuiResource.toClipboard()` select the implementation according to the
  runtime:
  - Desktop continues using the existing SWT clipboard.
  - Hop Web stores the serialized graph XML in the current RAP session.
- Kept forwarding copied text to the browser clipboard through
  `IHopWebUrlUpdater.copyToClipboard()` when the browser bridge is available.
- Made `GuiResource.fromClipboard()` read the session-local value in Hop Web.
- Changed pipeline and workflow clipboard delegates to report whether copying
  succeeded.
- Made `Ctrl+X` delete selected transforms, actions, or notes only after a
  successful copy.
- Added a focused unit test that verifies storage, browser forwarding, and
  isolation between clipboard instances.

### Behavior preserved

- The desktop SWT clipboard implementation is unchanged.
- Existing copy and paste XML formats are unchanged.
- Existing callers may ignore the new boolean return value when they do not
  need to conditionally delete a selection.
- Pipeline and workflow graph operations continue using their existing
  serialization and paste logic.

### Scope

This PR fixes graph-element clipboard operations inside a Hop Web session,
including copying between pipelines in that session.

It does not add general browser clipboard reads for arbitrary content copied
from another website, Word, or another application. Browser clipboard writes
are still forwarded through the existing bridge.

## Validation

### Automated tests

```text
mvn -pl :hop-ui -Dinsecure=true -Dtest=WebClipboardTest test
```

Result:

```text
Tests run: 1, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

```text
mvn -pl :hop-ui-rap -am -Dinsecure=true \
  -Dtest=WebClipboardTest,HopWebEntryPointTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Result:

```text
WebClipboardTest: 1 passed
HopWebEntryPointTest: 3 passed
BUILD SUCCESS
```

### Web assembly

```text
mvn -pl :hop-assemblies-web -am -Dinsecure=true -DskipTests package
```

Result:

```text
All 10 reactor modules: SUCCESS
```

### Manual Hop Web validation

Environment:

```text
Apache Hop Web: 2.19.0-SNAPSHOT
Tomcat: 10.1.57
Java: 21.0.2
URL: http://localhost:8080/ui
```

Test procedure:

1. Create a pipeline.
2. Add a `Dummy (do nothing)` transform.
3. Select the transform and press `Ctrl+C`, then `Ctrl+V`.
4. Verify that the transform is duplicated without an exception dialog.
5. Select all graph elements and press `Ctrl+X`.
6. Verify that the selected elements are removed.
7. Press `Ctrl+V`.
8. Verify that the cut elements are restored.
9. Check the browser console and RAP network requests.

Observed result:

- `Ctrl+C` and `Ctrl+V` duplicated the selected transforms.
- `Ctrl+X` removed the selection only after copying it.
- `Ctrl+V` restored the cut transforms.
- No `NullPointerException` or SWT clipboard error was displayed.
- No JavaScript console errors were produced.
- RAP requests completed successfully with HTTP `200` or cache-valid `304`
  responses.

## GitHub issues to mention

### Exact issue status

No currently open Apache Hop issue was found that reports this exact
`Display.readAndDispatch()` null regression in Hop Web 2.19.0-SNAPSHOT.

The following closed issues are direct historical precedents and should be
mentioned with `Related to`, not `Fixes` or `Closes`:

- [#1897 — Hop Web clipboard access is not available for non-localhost servers](https://github.com/apache/hop/issues/1897)
  - Closed.
  - Reports SWT clipboard failures from `GuiResource.toClipboard()` in Hop Web.
- [#4090 — Unable to Copy the Transform from one Pipeline to Another Pipeline](https://github.com/apache/hop/issues/4090)
  - Closed.
  - Matches the pipeline transform `Ctrl+C`/`Ctrl+V` workflow most closely.
- [#4810 — hop-web GUI crashes when trying to copy to clipboard](https://github.com/apache/hop/issues/4810)
  - Closed.
  - Reproduces the problem with a Dummy transform in Hop Web.
- [#5537 — about copy paste](https://github.com/apache/hop/issues/5537)
  - Closed.
  - Requests broader copy/paste support in Hop Web; this PR only addresses
    graph-element clipboard operations.

The following issue is open and touches the same pipeline clipboard delegate,
but has a different objective:

- [#6955 — Cleanup XML of class HopGuiPipelineClipboardDelegate](https://github.com/apache/hop/issues/6955)
  - Open.
  - Related code area, but it requests XML cleanup and is not fixed by this PR.
  - Mention it as `See also #6955`; do not use `Fixes #6955`.

### Recommended issue references for the PR

```text
Related to #1897, #4090, #4810, and #5537.
See also #6955.
```

Because all exact historical reports are closed, the cleanest option is to
open a new bug for the 2.19.0-SNAPSHOT regression and then add this line to the
PR:

```text
Fixes #<new-issue-number>.
```

Suggested new issue title:

```text
[Bug]: Ctrl+C, Ctrl+V and Ctrl+X fail for graph elements in Hop Web
```

Suggested issue summary:

```text
In Hop Web 2.19.0-SNAPSHOT, copying and pasting a selected pipeline transform
throws a NullPointerException because the RAP path attempts to read from an SWT
clipboard whose Display is null. Cutting is destructive because the selected
elements are deleted even when copying fails.

Steps to reproduce:
1. Open Hop Web and create a pipeline.
2. Add and select a Dummy transform.
3. Press Ctrl+C and Ctrl+V.
4. Observe the Display.readAndDispatch() NullPointerException.
5. Select the transform and press Ctrl+X.
6. Observe that it is deleted and cannot be restored with Ctrl+V.

Expected behavior:
- Ctrl+C/Ctrl+V duplicates the selected graph elements.
- Ctrl+X deletes the selection only after a successful copy.
- Ctrl+V restores the cut selection.
```

## Suggested reviewer notes

The session-local clipboard is intentionally small and contains only the latest
serialized graph selection. `GuiResource` is session-scoped in the RAP
implementation, so clipboard state is isolated between Hop Web UI sessions.

The new helper also keeps the browser writer optional. A browser clipboard
permission failure does not lose the session-local copy, which allows Hop Web
paste operations to continue working.

The boolean return value added to the delegates prevents destructive cuts:
pipeline transforms, workflow actions, and notes are deleted only when
serialization and clipboard storage complete successfully.
