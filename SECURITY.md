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

# Security Policy

## Reporting a Vulnerability

`apache/hop` follows the [Apache Software Foundation security process](https://www.apache.org/security/). Please report suspected
vulnerabilities privately to `security@apache.org`; do not open public
GitHub issues or pull requests for security reports.

## Threat Model

What the project treats as in scope and out of scope, the security
properties it provides and disclaims, the adversary model, and how
findings are triaged are documented in [THREAT_MODEL.md](./THREAT_MODEL.md).

## Hardening a deployment

Hop runs operator-authored pipelines and workflows with full host capability;
it is not a sandbox. Operators exposing Hop on a network should, at minimum:

- **Change the default Hop Server credential.** The shipped credential is the
  well-known default `cluster`/`cluster`. Do not expose the Hop Server (or the
  separate `rest/` REST API, which has no built-in authentication) to an
  untrusted network without changed credentials and a fronting auth layer.
- **Enable TLS** for the Hop Server and sensitive backends (off by default), and
  set `javax.net.ssl.keyStore` so outbound TLS verification is not relaxed.
- **Protect credentials at rest** with the AES2 password encoder
  (`HOP_PASSWORD_ENCODER_PLUGIN=AES2` + `HOP_AES_ENCODER_KEY`) or a secrets
  resolver (Vault / Azure Key Vault / Google Secret Manager); the default
  metadata password protection is reversible obfuscation, not encryption.
- **Only run pipelines/workflows from trusted authors**, and parameterize any
  variable that may carry untrusted data.

See [THREAT_MODEL.md](./THREAT_MODEL.md) §10 for the full list.
