<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Generic execution observation design

## Baseline and review status

Hop baseline: `2d995a647a223a53630ef958462628fe00c6c513`, version `2.21.0-SNAPSHOT`, Java 21.
Companion plugin baseline: `nadment/hop-opentelemetry` at `09e51e537163f9f8786b9bf7585842d59eba6a51`.
Both repositories use dedicated feature branches; no installed runtime or business project is part of this change.
Maintainer agreement on the new public contract has NOT been obtained or verified.
The code is a proposed contribution, not an assertion of upstream acceptance.

## Contract

An execution handle describes one engine invocation or actual runtime action/transform copy.
It has an opaque generated identity, an immutable parent handle, a subject, kind, name and copy number.
Data-flow hops do not create execution parentage.

Lifetime and activation are separate.
`begin` starts an invocation and `finish` ends it once.
`activate` temporarily installs observer context on the calling thread and restores it through a scope.
Scopes must be closed on their opening thread in reverse nesting order.
No scope is transported between threads.
`associate(child, launcher)` captures the real launcher handle before asynchronous dispatch.
The captured parent survives removal of the parent's active registry entry.
Pending associations use weak identity keys so an unstarted discarded child is not retained forever.

Observers are registered explicitly with a closeable registration.
The observer list is captured at execution opening.
Observer failures are isolated from business results; existing Hop extension points retain their existing semantics.
Providers must roll back a partially failed activation before throwing.
Observer attachment state belongs to the handle, allowing a child to retain its parent's transportable context after parent termination.
Attachments must not contain open thread scopes.

When no observer is installed there is no generated identity, active registry entry or context injection.
The processing hot path uses one activation around a multithreaded copy's loop, not one per row.
Single-threaded interleaving requires one activation per copy visit, reusing the same execution lifetime.
No engine dependency on OpenTelemetry, SLF4J MDC or exporters is introduced.

## Coverage and termination policy

Pipeline lifetime begins before preparation and ends after initialized copy disposal and completion notification.
A transform-copy lifetime covers initialization, processing and disposal.
Workflow lifetime covers the local engine invocation, including setup and cleanup.
An action lifetime belongs to the actual clone and excludes recursive successor traversal.
False branch evaluation alone is not an execution failure.
A stop request is not proof of worker termination.
An asynchronous child can outlive both its launching action and root workflow.

Existing workflow runner legacy postludes are distinct from the engine invocation.
A callback failure occurring only after engine completion cannot retroactively change an ended span.
Remote, Hop Server, Beam and Spark worker activation are not supported by this proposal.
Freely created third-party threads require explicit context propagation.

## Validation status

Validation results and remaining gaps are recorded with commands in the companion plugin's validation report.
This design document itself is not evidence that any test or runtime scenario passed.
