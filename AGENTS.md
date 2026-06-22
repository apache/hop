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
  tests are skipped).

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
