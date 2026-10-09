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

# Testing Watch Files

Use Java 21 and the repository Maven wrapper:

```sh
./mvnw -B -pl plugins/transforms/watchfiles -Pskip-uitest clean install apache-rat:check spotless:check
tools/with-isolated-display.sh ./mvnw -B -pl plugins/transforms/watchfiles -Puitest -Dwatchfiles.gui.capture=true test
```

The first command runs portable headless tests and Linux filesystem tests when
supported. The second runs SWT GUI tests on a separate Linux display and optionally
captures screenshots under the module's `target` directory.

Coverage includes stable changes, recursive registration, overflow reconciliation,
exclusive state ownership, corrupt or incompatible checkpoints, atomic replacement
and interrupted fallback commits, separate-JVM termination, monotonic scheduling,
VFS paths (including Unicode, literal percent signs, Windows drive/UNC roots and
unusual valid Unix names), invalid native-key cleanup after capped drains,
directory recreation, source restarts, timeout completion and downstream draining, plus GUI
save/cancel, conditional fields and recovery actions.

Environment-specific tests are opt-in:

* SFTP: set `WATCHFILES_SFTP_HOST`, `WATCHFILES_SFTP_USER`,
  `WATCHFILES_SFTP_PASSWORD`, `WATCHFILES_SFTP_ROOT`, and optionally
  `WATCHFILES_SFTP_PORT`. Use a disposable test root; credentials belong in the
  process environment, not checked-in configuration.
* Disk full: set `WATCHFILES_FULL_FILESYSTEM` to a disposable, already-full local
  filesystem for the ENOSPC test.
* Sustained load: use `-Dtest=WatchFilesLoadTest -Dwatchfiles.load.seconds=86400`.
  The native/polling test uses concurrent writers, bounded rowsets, a slow sink
  and restarts. Its JSONL report defaults to `target/watchfiles-load.jsonl`.

The original implementation passed a dedicated Linux soak lasting 86,444 seconds,
with 94 cycles, 187,000 consumed events, zero duplicate and zero unexpected rows.
Later filename-pattern, GUI and timeout changes have separate regression checks;
this historical soak does not constitute a new 24-hour run of those changes.

State records observations handed to Hop, independently of downstream completion.
Tests do not establish end-to-end exactly-once delivery, power-loss durability,
macOS support or coverage of every remote VFS provider. Full reactor CI remains
the merge gate.
