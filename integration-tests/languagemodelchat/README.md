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

# Language Model Chat integration tests

The Language Model Chat transform talks to five providers, four of which need
an API key. Ollama does not, so it is the one that can be tested here: a real
model answers a real prompt over HTTP, which covers the request the transform
builds, the call itself and the response it reads back.

## Running

The project carries a `disabled.txt`: `ollama/ollama` is a large image and the
model still has to be pulled on top of it, which the standard suite should not
pay for on every run. Run it explicitly:

```
./run-tests-docker.sh PROJECT_NAME=languagemodelchat INCLUDE_DISABLED=true
```

`docker/integration-tests/integration-tests-languagemodelchat.yaml` starts
Ollama, pulls `smollm2:135m` (~270MB) and holds the test container back until
the pull has finished. The endpoint and the model are set in
`dev-env-config.json`, so a local Ollama can be used instead:

```
OLLAMA_BASE_URL=http://localhost:11434 OLLAMA_MODEL=phi3
```

## What is checked

A 135M parameter model writes what it likes, so the answer itself is not
compared against anything. The checks are on the shape of the result:

- `llm_finish_reason` is not `ERROR`. The transform catches every failure from
  the model call and writes `ERROR` into this field rather than failing the
  row, so without this check a pipeline that reached no model at all still ends
  green.
- `llm_output` is not null.
- `llm_total_token_count` is greater than zero, which the transform can only
  have read from a real response.

Any of the three failing routes the row to `Abort`, which fails the pipeline.
