<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# AI integration tests

Tests for the transforms that call an AI model. They share one Ollama container, defined in
`docker/integration-tests/integration-tests-ai.yaml`, because the image is large and standing up
one container per transform would pay for it several times over.

Run them with:

    ./run-tests-docker.sh PROJECT_NAME=ai INCLUDE_DISABLED=true

They are disabled by default: the image and the model pulls are too expensive for every run of the
standard suite. See `disabled.txt`.

## Adding a test

1. Add a `main-000N-<name>.hwf` and the pipelines it drives. Each `main-*.hwf` is reported as its
   own test.
2. If it needs a model the container does not pull yet, add a `ollama pull` line to the compose
   file and extend the healthcheck so the test container waits for it.
3. Models are reached over compose DNS at `ollama:11434`. The `ai-provider` metadata objects in
   `metadata/ai-provider` point there.

## What each test covers

| Test | Covers |
|---|---|
| `0001-embed-text` | The Embed text transform against `nomic-embed-text`: the vector width, the model name on the row, and that an embedding comes back. The vector values themselves vary per call, so they are not compared. |
