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

# Language Model Chat API key integration tests

These tests check which API key the Language Model Chat transform sends, for every provider that
needs one. No real provider is called: an nginx stub answers each chat request with the key it
received, so a pipeline can compare the answer with the key it expects.

| Test | Key field | Expected key |
|---|---|---|
| 0001 | OpenAI, left at its default | `OPENAI_API_KEY` from the environment |
| 0002 | OpenAI, `'${OPENAI_API_KEY}'` | `OPENAI_API_KEY` from the environment |
| 0003 | OpenAI, the bare name `OPENAI_API_KEY` older pipelines stored | `OPENAI_API_KEY` from the environment |
| 0004 | OpenAI, a variable that is not set | none |
| 0005 | Mistral, left at its default | `MISTRAL_API_KEY` from the environment |
| 0006 | Anthropic, left at its default | `ANTHROPIC_API_KEY` from the environment |
| 0007 | Hugging Face, left at its default | `HF_ACCESS_TOKEN` from the environment |
| 0008 | Lints the Language Model Chat and GraphRAG samples | no SEC-002 (hardcoded secret) finding |

## Running

```
./run-tests-docker.sh PROJECT_NAME=languagemodelchat-api-keys
```

`docker/integration-tests/integration-tests-languagemodelchat-api-keys.yaml` starts the stub
(`nginx:alpine`, configured in `docker/integration-tests/resource/languagemodelchat/llm-stub.conf`)
and sets the four environment variables in the test container. The stub's address is
`LLM_STUB_URL` in `dev-env-config.json`.
