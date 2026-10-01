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

# Apache Hop — Threat Model

**Status:** Ratified by the Hop PMC. Originally drafted by the ASF Security Team
(2026-06-01) against `apache/hop` (default branch `main`); the inferred items
were then code-verified against the source and confirmed/corrected by the PMC
(2026-06-22). Claims below cite `file:line` evidence where it is load-bearing.

**Revision triggers:** a change to the Hop Server's auth / remote-execution
model; a change to the Hop Web authentication modes or to what the Hop Web
web application deploys; a new scripting/exec transform or action; a change to
how connection credentials / variables are stored or resolved; a new metadata
source.

---

## §1 Header

- **Project:** Apache Hop (`apache/hop`) — the "Hop Orchestration Platform":
  visual **data and metadata orchestration** (pipelines + workflows) authored
  in a GUI and run on a local engine, a remote Hop Server, or Beam runners.
- **Scope of this model:** the `apache/hop` repository only (`hop-website`
  excluded per the PMC, 2026-05-15).
- **What Hop is:** a tool that **executes operator-authored data pipelines and
  workflows** — by design those can read/write files, query databases, call
  services, and run scripting/exec steps. It is **not a sandbox** and **not a
  multi-tenant isolation boundary** (confirmed: no JVM `SecurityManager`/policy
  anywhere in `engine`/`core`/`plugins`; the scripting engines are deliberately
  configured for full host access — see §9).

## §2 Scope and intended use

Hop is operated by a **data engineer / operator** who authors pipelines
(`.hpl`) and workflows (`.hwf`) and runs them — interactively in Hop GUI, via
the `hop-run` CLI, or submitted to a **Hop Server** for local/remote execution.

### Component families (distinct threat profiles)

| Family | Path | Role / trust notes |
|---|---|---|
| Execution engine | `engine/`, `engine-beam/` | Runs pipelines/workflows the operator authored — incl. transforms that touch files, DBs, network, and **scripting/exec** steps. |
| Hop Server (servlet / HTTP) | `engine/` — package `org.apache.hop.www` + `HopServerMeta` in `org.apache.hop.server`; launched by the `hop-server` command (`org.apache.hop.www.HopServer`). *(The `org.apache.hop.server` connection helpers `HttpUtil`/`ServerConnectionManager` — see §9 — live in `core/`.)* | The **network trust boundary**: an embedded Jetty server whose servlets accept pipeline/workflow run requests over HTTP (`/hop/execPipeline`, `/hop/addPipeline`+`/hop/startExec`, `/hop/registerPackage`, …). Who may submit/run, and with what auth, lives here (§7, §8). |
| Hop JSON API | `engine/src/main/java/org/apache/hop/www/api/` | **Part of Hop Server**, not a separate deployable. Mounted on `/hop/api/v1/` ([`WebServer.java`](engine/src/main/java/org/apache/hop/www/WebServer.java)) and exposes `execute/sync` (run a web service), `metadata` CRUD, `plugins` and `location` execution info. In the standalone `hop-server` deployment it sits inside the same `ConstraintSecurityHandler` as every servlet, so it inherits Hop Server's Basic/JAAS auth; when the same API is co-deployed inside **Hop Web** it is gated instead by Hop Web's own filters (`HopBasicAuthFilter` / `HopOidcAuthFilter` / `HopServerAuthorizationFilter`) — see the Hop Web row and §8. |
| Arrow Flight server (gRPC) | `plugins/tech/arrow/` — package `org.apache.hop.arrow.flight`; launched by the `hop arrow` command (`org.apache.hop.arrow.command.ArrowCommand`) | A **separate network listener**, distinct from both Hop Server and Hop Web: a gRPC/Arrow Flight endpoint that hands rows to and from Data Stream metadata elements. Binds `0.0.0.0:33333` by default. TLS (incl. mutual TLS) and username/password authentication are configurable but **off unless the operator passes the options** (§8) — with neither, anything that can reach the port can read the data it streams. |
| GUI (desktop / web) | `ui/` (`hop-ui`, SWT core), `rcp/` (desktop fragment), `rap/` (`hop-ui-rap`, RAP/RWT **web** GUI) | Authoring surface used by the trusted operator. `rap/` is the **web GUI code**, not a server — but see the Hop Web row: the shipped web application deploys it together with Hop Server servlets. |
| Hop Web (deployed web application) | `assemblies/web/` (WAR + `apache/hop-web` image), built from `rap/` + `engine/` | A **distinct network-facing deployment**, separate from both `rap/` and `hop-server`. The one WAR co-deploys the RAP UI on `/ui` and `/ui-dark`, `HopServerServlet` on `/hop/*`, and `HopApiApplication` on `/hop/api/v1/*` on the same origin ([`web.xml`](assemblies/web/src/main/resources/WEB-INF/web.xml)). It does **not** use Hop Server's `enable_auth` / `hop.pwd` Basic auth; it has its own auth modes (§8). Docker image binds `0.0.0.0:8080`. |
| Plugins | `plugins/` | The large transform/action set, incl. scripting (GraalJS/Rhino/Groovy), shell/exec, SQL, and per-DB/per-cloud connectors (`plugins/tech/*`). |
| Connection / driver layer | `lib-jdbc/` (bundled JDBC drivers), `core/` (`org.apache.hop.core.database`, `org.apache.hop.metadata`) | DB/file/cloud credentials + JDBC drivers the operator configures. (There is no standalone `metadata/` module — connection/metadata code is in `core/`.) |

**In scope:** engine + Hop Server + Hop Web + plugins + connection layer + GUI glue.
**Intended caller trust:** Hop is operated as a **single trust domain** — the
pipeline/workflow **author, the local operator, and anyone holding Hop Server
credentials are all trusted** (they direct what Hop does). There are **several
network-facing surfaces — Hop Server, Hop Web and the optional Arrow Flight
server** — and any of them is where an untrusted actor can appear; Hop Server
and Hop Web have **separate auth models** (§8).

## §3 Out of scope (explicit non-goals)

- `hop-website`, `docs/`, `integration-tests/`, `assemblies/` build mechanics,
  samples/demos — not the shipped runtime product. *(Note: the **default
  security posture** baked by `assemblies/` and `docker/` — the shipped
  `hop-server.xml`, `pwd/hop.pwd`, and the container `0.0.0.0` bind + default
  credentials — is an in-scope **fact** for the auth analysis in §8/§10, even
  though the assembly build itself is out of scope.)*
- **Bundled third-party JDBC drivers / libraries' own vulnerabilities** — a CVE
  in a packaged driver is a dependency/supply-chain matter, not a Hop defect
  (Hop's job is to call them correctly). `lib-jdbc/` bundles redshift,
  clickhouse, h2, mssql, vertica, crate, monetdb, jt400, snowflake, derby,
  duckdb, hsqldb, postgresql, sqlite, mysql.
- **Credential acquisition / storage / rotation** for the DBs/services Hop
  connects to — the operator supplies these via connections/variables (§8, §10).
- **Multi-tenant isolation** between pipelines/users on a shared engine or
  server — Hop runs as a single trust domain (§9).

## §4 Trust boundaries and data flow

1. **Pipeline/workflow author → engine.** The author is trusted; what they build
   (incl. scripting/exec/SQL steps) is *intended* execution, not an attack
   (§7, §9).
2. **Remote client → Hop Server (HTTP).** The high-value boundary: a network
   client requests pipeline/workflow execution. Authentication, transport
   security, and "who can run what" live here. The JSON API on `/hop/api/v1/`
   is served by the same process and behind the same auth (§8, §10).
3. **Metadata / project ingestion.** Pipeline/workflow + metadata files loaded
   from disk/VCS. **Loading** a file parses it (with hardened XML — §8) and
   instantiates only the no-arg *metadata* classes of referenced
   transforms/actions; no scripting/exec/DB/network/UI code runs from loading
   alone. **Running** it executes those steps — so running an **untrusted**
   `.hpl`/`.hwf` is equivalent to running untrusted instructions (§11).
   Verified: [`PipelineMeta.java`](engine/src/main/java/org/apache/hop/pipeline/PipelineMeta.java), [`TransformMeta.java:238`](engine/src/main/java/org/apache/hop/pipeline/transform/TransformMeta.java#L238).
4. **Connection layer → external DB/file/cloud.** Carries operator-configured
   credentials; responses are backend-controlled (§8).
5. **Variables / parameters.** Resolved into transform config at run time by
   **raw, unescaped string substitution** — an injection surface if a value is
   sourced from untrusted input (§9). A `Set Variables` transform/action can copy
   an upstream **row-stream value** (or an OS env / `-D` property) into a
   variable, so untrusted data can demonstrably reach a sink.

## §5 Assumptions about the environment

JVM; the OS the engine/server runs on (full file + process access for
exec/shell/scripting steps); network reachability to configured backends + the
Hop Server's listen interface; system trust store for outbound TLS (with the
caveats in §9). The operator is responsible for setting `javax.net.ssl.keyStore`
where strict outbound TLS verification is required (see §9).

## §6 Assumptions about inputs — per-parameter trust

| Input | Source | Trusted? | Notes |
|---|---|---|---|
| Pipeline/workflow definitions (`.hpl`/`.hwf`) | Operator/author | Trusted | Running an untrusted definition = running untrusted code (§11). Loading is hardened against XXE (§8). |
| Hop Server run requests | Network client | **Untrusted-capable** | Auth/authz is the boundary. The server enforces HTTP Basic auth by default — covering both the XML servlets and the JSON API — but ships a world-known default credential and has no per-endpoint authz (§8). |
| Connection configs + credentials | Operator | Trusted (secret) | Stored only **obfuscated** by default — *not* confidential at rest unless the operator opts into AES2 / a secrets resolver (§8). |
| Variables / parameters | Operator, OS env / `-D`, or upstream row data | Untrusted-capable | Resolved by raw substitution; injection into SQL/paths/commands if the value is untrusted (§9). |
| Metadata (from project files / VCS) | Operator/author | Trusted-by-default | Untrusted if loaded from an external source (§11). No signature/provenance check exists. |
| Row/stream data processed by a pipeline | Upstream sources | Pass-through | Hop moves it; semantic validation is the pipeline author's. |

## §7 Adversary model

- **Out of scope:** the **pipeline/workflow author, the local operator, and any
  holder of Hop Server credentials** — Hop is a single trust domain, and they
  already control execution (a scripting step that runs code is intent, not an
  attack).
- **In scope:** a **remote actor against an exposed Hop Server or Hop Web
  deployment** — submitting or running pipelines, reading results and mutating
  metadata over `/hop/*` or `/hop/api/v1/*`, or driving the RAP authoring UI on
  `/ui` — bounded by whatever auth fronts the surface (`enable_auth` for
  `hop-server`, the Hop Web auth mode for Hop Web, §8); and a **network MITM**
  between Hop and its backends or between client and server.
- **Conditional:** an attacker who can get an **untrusted pipeline/metadata
  file** loaded/run, or who controls a **variable/parameter** value (incl. an
  upstream row value picked up by `Set Variables`) that reaches a sink.

## §8 Security properties the project provides

- **Hop Server authentication (with a critical caveat).** *(This property is
  scoped to the standalone `hop-server` deployment only — Hop Web has no
  `enable_auth` and no `hop.pwd`; see the separate Hop Web property below.)*
  The servlet Hop Server
  enables HTTP Basic authentication **by default** (`enable_auth` defaults to
  true — [`HopServerMeta.java:229,251`](engine/src/main/java/org/apache/hop/server/HopServerMeta.java#L229), [`WebServer.java:197`](engine/src/main/java/org/apache/hop/www/WebServer.java#L197)), gating every endpoint, so a *fully unauthenticated* client is
  rejected. **Caveat:** the only shipped credential is the **publicly-known
  default `cluster`/`cluster`** ([`pwd/hop.pwd`](assemblies/static/src/main/resources/pwd/hop.pwd), [`HopServer.java:164`](engine/src/main/java/org/apache/hop/www/HopServer.java#L164)), there is **no per-endpoint
  authorization** (a single `/*` role gate — any authenticated user can run/stop
  pipelines), and the **Docker image binds `0.0.0.0:8080`** with those defaults.
  Changing the credential and restricting exposure is an operator responsibility
  (§10). — violation symptom: remote code execution using the unchanged default
  credential on an exposed deployment; severity: critical.
- **Hop Web authentication and authorization (separate model from `hop-server`).**
  Hop Web does not inherit the `hop-server` `enable_auth` / `cluster:cluster`
  posture described above; it has its own authentication mode in
  `security-config.json`
  ([`HopSecurityConfig.java`](core/src/main/java/org/apache/hop/core/security/HopSecurityConfig.java),
  `HOP_WEB_SECURITY_MODE`), and it **ships in mode `NONE`**. The boundary per
  mode:
  - **`NONE` (default).** The RAP UI on `/ui` / `/ui-dark` is **open by design**
    — this is the single-user / trusted-network install and the session is
    treated as *unrestricted*. The co-deployed Hop Server API on `/hop/*` is
    **default-denied** (`403`) by
    [`HopServerAuthorizationFilter`](rap/src/main/java/org/apache/hop/ui/hopgui/security/HopServerAuthorizationFilter.java),
    because mode `NONE` has no identity to authorize against and those endpoints
    deploy and execute pipelines/workflows. Opt-in flag
    `allowUnauthenticatedServerApi` /
    `HOP_WEB_ALLOW_UNAUTHENTICATED_SERVER_API` reopens it for operators running
    Hop Web purely as an execution server behind their own network controls —
    violation symptom: unauthenticated remote code execution via `/hop/*` on an
    exposed deployment that enabled the flag; severity: critical; mitigation is
    operator-side network control (§10).
  - **`BASIC` / `OAUTH2`.** Hop's own servlet filters
    ([`HopBasicAuthFilter`](rap/src/main/java/org/apache/hop/ui/hopgui/security/HopBasicAuthFilter.java),
    [`HopOidcAuthFilter`](rap/src/main/java/org/apache/hop/ui/hopgui/security/HopOidcAuthFilter.java))
    are mapped on `/*` — the whole application, UI and server API alike — and
    establish a principal plus roles. Unlike `hop-server`, `/hop/*` then gets
    **per-endpoint authorization**: each path maps to a required permission and
    unmapped paths are **default-denied**, so a *Read-only* user can read status
    but not `addPipeline` / `startPipeline` / `execWorkflow`.
  - **`EXTERNAL`.** Authentication is **delegated to the servlet container or a
    reverse proxy**; Hop only reads `request.getUserPrincipal()`. Nothing in
    `rap/` enforces it, and the shipped
    [`web.xml`](assemblies/web/src/main/resources/WEB-INF/web.xml) contains **no
    `<security-constraint>`** — so an operator who selects `EXTERNAL` without
    adding one gets an unauthenticated `/ui` while the General tab shows an
    authenticated mode (`EXTERNAL`) — the Security tab's own status line still
    reads *"Session is unrestricted (no AuthN)"*, so the tab surfaces the
    contradiction rather than claiming authentication is on. `/hop/*` still
    returns `401` (no principal) and `HopWebEntryPoint` now logs a warning for
    every principal-less session in a non-`NONE` mode, but the UI itself is
    open. — violation symptom: a fail-open UI on a deployment the operator
    believes is authenticated;
    severity: high; mitigation: a container `<security-constraint>` over `/*`,
    or a proxy that rejects unauthenticated requests (§10).
- **Cross-site browser requests to the Hop Server are rejected.** The servlets
  answer state-changing operations on `GET`, so a page the operator is visiting
  could otherwise drive them with the operator's browser and credentials
  (`Origin` is no defence: browsers omit it on cross-site `GET`). A single
  handler wrapped around every servlet context, the static resources and the
  JSON API ([`CrossSiteRequestHandler.java`](engine/src/main/java/org/apache/hop/www/CrossSiteRequestHandler.java), installed in [`WebServer.java`](engine/src/main/java/org/apache/hop/www/WebServer.java) ahead of
  authentication) rejects a request whose `Sec-Fetch-Site` the configured
  [`CrossSitePolicy`](core/src/main/java/org/apache/hop/core/security/CrossSitePolicy.java) disallows — by default `cross-site`, and under
  `--cross-site-policy same-origin` also `same-site`. **The same check covers Hop
  Web**, which co-deploys these servlets on `/hop/*` outside `WebServer`, via a
  servlet filter mapped ahead of the authentication filters
  ([`CrossSiteRequestFilter.java`](engine/src/main/java/org/apache/hop/www/CrossSiteRequestFilter.java), [`web.xml`](assemblies/web/src/main/resources/WEB-INF/web.xml)); there it is configured by
  `crossSitePolicy` in `security-config.json` (Configuration → Security →
  General) and matters more, because Hop Web's authenticated modes are
  session-backed and a browser attaches the session cookie to a cross-site
  request by itself. The Hop Web **UI** (`/ui`) is not covered. Requests carrying no
  `Sec-Fetch-*` headers (hop-run, the Hop GUI, curl, automation) are allowed, so
  non-browser clients are unaffected; `--cross-site-policy off` disables the
  check entirely. This narrows drive-by use of an authenticated operator's
  browser only — it is not per-endpoint authorization, and does nothing about a
  client that reaches the server directly (§9). — violation symptom: a visited
  web page starts or stops pipelines on a server the operator is logged in to;
  severity: high.
- **XML parsing is hardened against XXE.** Pipeline/workflow/metadata files are
  parsed through a secure `DocumentBuilderFactory`
  ([`XmlParserFactoryProducer.java`](core/src/main/java/org/apache/hop/core/xml/XmlParserFactoryProducer.java)) with external general/parameter entities and
  external DTD loading disabled and `FEATURE_SECURE_PROCESSING` on — XXE
  file-read/SSRF and entity-expansion are mitigated (verified empirically). The
  same secure parser is used by the Hop Server remote-add endpoints. StAX ingest
  (`XmlInputStream` and other `XMLInputFactory` sites) uses
  `createSecureXmlInputFactory()`, which disables DTD processing and external
  entities.
- **Credential storage — NOT confidential by default.** Connection passwords in
  metadata are by default only **reversibly obfuscated, not encrypted**: the
  built-in `Hop` encoder ([`HopTwoWayPasswordEncoder.java`](core/src/main/java/org/apache/hop/core/encryption/HopTwoWayPasswordEncoder.java)) XORs against a
  hardcoded, publicly-known seed and prefixes `Encrypted ` — the code itself
  states it is *"not really encryption … obfuscation."* Anyone with the file
  recovers the plaintext. Real confidentiality is **opt-in**: set
  `HOP_PASSWORD_ENCODER_PLUGIN=AES2` with an operator-held `HOP_AES_ENCODER_KEY`
  or `HOP_AES_ENCODER_KEY_FILE` (AES-GCM), and/or keep secrets out of metadata
  via a variable resolver (HashiCorp Vault, Azure Key Vault, Google Secret
  Manager). Encoder plugin and key can be re-bound per project/environment on
  enable (and via the Set password encoder action); the active encoder remains
  process-global. See §9, §10, §11a.
- **Credential leakage in logs.** No plaintext password is logged by default,
  and decrypted DB passwords travel in a JDBC `Properties` object, not the URL.
  However, resolved JDBC/REST/HTTP URLs are logged at debug/detailed level
  **without credential masking** ([`Database.java:488`](core/src/main/java/org/apache/hop/core/database/Database.java#L488), [`Rest.java:142,199`](plugins/transforms/rest/src/main/java/org/apache/hop/pipeline/transforms/rest/Rest.java#L142)), so credentials an
  author embeds in a URL can leak — operators should avoid in-URL credentials.
- **Transport security — available, off by default.** The Hop Server can serve
  over HTTPS via a Jetty `SslContextFactory` configured with a JKS keystore in
  the `<sslConfig>` block of `hop-server.xml`, but **TLS is off by default** (the
  shipped config starts a plain-HTTP listener; only auth is on). Outbound JDBC
  TLS is fully driver-delegated; HTTP/REST/cloud transforms use the JVM trust
  store unless given a per-transform truststore. The **Arrow Flight server**
  (`hop arrow`) is likewise plaintext and unauthenticated unless started with
  `--arrow-flight-tls-certificate`/`--arrow-flight-tls-key` (optionally
  `--arrow-flight-tls-client-ca` for mutual TLS) and
  `--arrow-flight-username`/`--arrow-flight-password`; it logs a warning when it
  starts without them. Enabling TLS is an operator responsibility (§10).

## §9 Security properties the project does *not* provide

- **Not a sandbox.** An authored pipeline/workflow runs scripting/shell/SQL and
  touches files/network with **full host/JVM capability — by design**. There is
  no `SecurityManager` anywhere. The `script` transform sets GraalJS
  `HostAccess.ALL` + `allowHostClassLookup(true)` ([`ScriptUtils.java:165-166`](plugins/transforms/script/src/main/java/org/apache/hop/pipeline/transforms/script/ScriptUtils.java#L165-L166)); the
  legacy `javascript` transform and `eval` action use Mozilla Rhino with no
  `ClassShutter`; `User Defined Java Class`/`Janino`/`Java Filter` compile
  arbitrary Java; `Exec process`/`Shell` run OS commands via
  `Runtime.exec`/`ProcessBuilder`. The **only** restriction mechanism is an
  opt-in, **default-empty** substring deny-list ([`JaninoCheckerUtil`](plugins/transforms/janino/src/main/java/org/apache/hop/pipeline/transforms/util/JaninoCheckerUtil.java) +
  `codeExclusions.xml`) that covers **only** the three Janino-based steps and
  blocks nothing out of the box (§11a).
- **No isolation between pipelines/tenants** running on the same engine/server
  beyond what the OS/JVM provides. Hop is a single trust domain (§7).
- **No per-endpoint authorization on the Hop Server.** Any authenticated user is
  fully privileged across both the XML servlets and the JSON API (§10).
- **Credential confidentiality at rest is not provided by default** — only
  reversible obfuscation; key-based protection is opt-in (§8).
- **No strict TLS verification on all client/internal paths.** The relaxation
  that is genuinely in effect is in [`ServerConnectionManager`](core/src/main/java/org/apache/hop/server/ServerConnectionManager.java), which installs a
  **process-global** default `SSLContext` that skips CA-chain/hostname
  validation (date-only) when `javax.net.ssl.keyStore` is unset. (`HopServerMeta.getHttpClient`
  also *constructs* a self-signed-accepting, no-hostname-verify socket factory in
  `sslMode`, but it is not actually wired into the returned client — latent.)
  Several transforms also offer opt-in `ignoreSsl`/`trustAllCertificates`
  switches (all default to verification on). The HashiCorp Vault and OpenBAO
  variable resolvers verify the secrets server's certificate by default too, but
  the setting is serialised per resolver, so a definition saved with it off keeps
  reaching the secrets server unverified until an operator changes it.
- **No automatic sanitization of variables.** `${VAR}`/`%%VAR%%`/`$[hex]` tokens
  are resolved by raw, unescaped string substitution
  ([`StringUtil.environmentSubstitute`](core/src/main/java/org/apache/hop/core/util/StringUtil.java)); the result is concatenated directly into
  SQL strings, file paths, and shell bodies. Parameterizing/escaping any variable
  that may carry untrusted data is the pipeline author's job. *(Note: the
  row-stream value path of SQL transforms — Table Input lookup params, Execute
  SQL "parameters" mode — IS bound via JDBC `PreparedStatement` `?` placeholders;
  that path is parameterized by design and is not the injection concern.)*
- **No protection if an untrusted `.hpl`/`.hwf` is loaded and run** — equivalent
  to executing untrusted instructions. There is **no signature, provenance, or
  integrity check** on these files; they are unsigned XML, and trust comes solely
  from their source.

## §10 Downstream (operator) responsibilities

- **Lock down the Hop Server:** change the default `cluster`/`cluster`
  credential, do not expose the remote-execution API to an untrusted network,
  and restrict who can submit runs. **Keep `enable_auth` on** — it is what
  protects the XML servlets and the JSON API alike —
  it has none of its own. Be aware the Docker image binds `0.0.0.0:8080` with
  the default credential.
- **Lock down Hop Web** (separate from `hop-server` above — `enable_auth` and
  `hop.pwd` do not apply): it ships in mode `NONE`, where `/ui` is open by
  design. For any shared deployment set `HOP_WEB_SECURITY_MODE` to `BASIC` or
  `OAUTH2`, or to `EXTERNAL` **together with** a container
  `<security-constraint>` over `/*` (or an authenticating reverse proxy) —
  `EXTERNAL` on its own fails open. Leave
  `HOP_WEB_ALLOW_UNAUTHENTICATED_SERVER_API` off unless you deliberately run Hop
  Web as an execution server behind network controls. Persist
  `HOP_CONFIG_FOLDER/security/` so users and role mappings survive upgrades.
- **Protect credentials at rest:** enable the AES2 encoder
  (`HOP_PASSWORD_ENCODER_PLUGIN=AES2` + `HOP_AES_ENCODER_KEY` or
  `HOP_AES_ENCODER_KEY_FILE`) and/or a secrets
  resolver (Vault / Azure Key Vault / Google Secret Manager); never commit
  default-obfuscated credentials to VCS; avoid embedding credentials in URLs.
- **Manage connection credentials**; don't source connection config or
  pipeline/metadata files from untrusted input — only run pipelines from trusted
  authors.
- **Treat scripting/exec/SQL steps as code**; parameterize variables that carry
  untrusted data. The Janino deny-list (`codeExclusions.xml`) can be populated to
  block named constructs in the three Janino-based steps if desired.
- **Use TLS** for the server + sensitive backends. Set `javax.net.ssl.keyStore`
  to avoid the relaxed process-global TLS trust manager (§9).

## §11 Known misuse patterns

- Exposing a **Hop Server with the unchanged default credential** (`cluster`/
  `cluster`), or with `enable_auth=false`, to an untrusted network → remote
  pipeline execution = remote code execution.
- Running Hop Server with `enable_auth=false` on an untrusted network →
  unauthenticated pipeline execution + metadata mutation over the JSON API.
- Exposing **Hop Web in mode `NONE`** to an untrusted network → `/ui` is an
  **unrestricted** GUI session, so anyone who can reach it can author *and* run
  pipelines/workflows **in-process** (the GUI executes locally through
  `PipelineEngineFactory` / `LocalPipelineEngine`, it does not go through
  `/hop/*`) = remote code execution, even though `/hop/*` still answers `403`.
  Enabling `HOP_WEB_ALLOW_UNAUTHENTICATED_SERVER_API` additionally opens the
  remote `/hop/*` and `/hop/api/v1/*` execution API.
- Selecting Hop Web mode **`EXTERNAL` without a container
  `<security-constraint>`** (or proxy) → an open `/ui` on a deployment the
  operator believes is authenticated.
- Running an **untrusted pipeline/workflow file** (or one fetched from an
  untrusted source).
- Building unparameterized SQL / shell / file paths from **untrusted variable
  values** — e.g. a value placed into a variable by a `Set Variables` transform
  from an untrusted upstream row, or from an attacker-influenced OS env / `-D`
  property. The resolver does not escape, so this is injection (SQL injection,
  path traversal, shell command construction).

### §11a Known non-findings (recurring false positives)

All of the following are **DISCLAIMED §9 non-guarantees** with the §7 adversary
being the **trusted author/operator** — they are intended functionality, not
vulnerabilities:

- "A pipeline can run JavaScript/Groovy/Python/shell/SQL" — by design. The
  `script` transform's GraalJS `HostAccess.ALL` + `allowHostClassLookup(true)`,
  the Rhino transforms/`eval` action without a `ClassShutter`, and the
  `ScriptValuesAddedFunctions` helpers (`execProcess`, `loadFileContent`,
  `appendToFile`) are deliberate full-host scripting affordances.
- "`User Defined Java Class` / `Janino` / `Java Filter` compile arbitrary Java
  with no `SecurityManager`" — by design; the empty `codeExclusions.xml`
  deny-list (which permits `System.exit`/`Runtime.exec` until an operator
  populates it) is the documented default, not a regression.
- "`Exec process` uses `Runtime.exec`; `Shell` uses `ProcessBuilder`
  (`cmd.exe /C` on Windows; the script file is run directly on other OSes);
  `sql`/`execsqlrow`/`dynamicsqlrow` execute
  operator-authored SQL; `ssh`/`as400command` run commands on a remote host" —
  running operator-authored commands/SQL is the step's purpose.
- "A connection password in a `.hpl`/`.hwf` is only obfuscated / the encoder
  seed is hardcoded" — by design; the default `Hop` encoder is obfuscation, not
  confidentiality. Key-based confidentiality (AES2) and secrets resolvers are
  opt-in (§8). An exposed default-obfuscated metadata password is recoverable by
  design.
- "Hop connects to a database with a configured password" — operator-supplied
  connection config, not a finding.
- "A bundled JDBC driver has CVE-XXXX" — dependency/supply-chain, handled via the
  driver/ASF process, not a Hop code defect.
- "Hop reads/writes the files a pipeline tells it to" — the operator's pipeline
  doing its job.
- *(Clarifications so triagers don't misfire):* the SQL **row-stream value path**
  uses `PreparedStatement` `?` binding and is **not** a SQL-injection bug; the
  `formula` transform (bounded POI/`FormulaParser` DSL) and the `simpleeval`
  action (field/variable comparison) are **not** code-execution steps and should
  not be conflated with the scripting steps above.

## §12 Conditions that would change this model

A change to the Hop Server auth or remote-exec model; a change to the Hop Web
authentication modes, its servlet filters, or to what the Hop Web web
application deploys; a new scripting/exec transform or action; a change to
credential/variable storage or resolution (e.g. a new default encoder); a new
metadata source format/location;
or a decision to support multi-tenant isolation (which today does not exist).

## §13 Triage dispositions

| Disposition | When | Section |
|---|---|---|
| **VALID** | A genuine implementation defect that violates a §8 property under the §7 adversary in in-scope code — e.g. an auth gate that can be bypassed (an endpoint reachable without the configured credential), an authorization hole beyond the documented single `/*` role, a credential leak in logs, or an XXE that bypasses the hardened parser | §7, §8 |
| **OUT-OF-MODEL** | Adversary is the trusted author/operator/credential-holder; or code in `hop-website`/tests/samples/assembly build | §3, §7 |
| **DOWNSTREAM-RESPONSIBILITY** | Default-credential / network exposure, credential mgmt, running untrusted pipelines, unparameterized untrusted variables, enabling TLS | §10 |
| **DISCLAIMED** | A §9 non-guarantee (authored pipeline runs code; default obfuscation; no pipeline isolation; no per-endpoint authz; relaxed internal TLS; bundled-driver CVE) | §9 |
| **MODEL-GAP** | Plausible, not covered → escalate to PMC | §12 |

## §14 Maintainer decisions (resolution of the v1 open questions)

The v1 draft's open questions have been resolved by the PMC (2026-06-22):

1. **Framing (Q1):** Confirmed — Hop runs operator-authored pipelines (incl.
   scripting/exec) by design; it is not a sandbox (§1, §9; code-verified).
2. **Trust model (Q2, Q7):** Confirmed — **single trust domain**: author,
   operator, and Hop Server credential-holder are trusted and out of the
   adversary model. Running an untrusted `.hpl`/`.hwf` is
   DOWNSTREAM-RESPONSIBILITY / OUT-OF-MODEL; the network-facing server
   submit-run path stays in scope (§7).
3. **Out of scope (Q3):** Confirmed — website, tests/samples, assembly build,
   bundled-driver CVEs (§3), with the caveat that the shipped default *posture*
   is an in-scope fact.
4. **Hop Server auth (Q5):** Resolved as code fact — Basic auth on by default
   but a world-known default credential, no per-endpoint authz, Docker `0.0.0.0`
   bind. Treated as DOWNSTREAM-RESPONSIBILITY: operators must change the
   credential and restrict exposure (§8, §10).
5. **TLS (Q6):** Resolved — available, off by default; internal relaxations are
   documented §9 non-guarantees with an operator mitigation (§9, §10).
6. **Credentials (Q8):** Resolved — default storage is obfuscation, not
   confidentiality (DISCLAIMED); AES2 / secrets resolvers are the supported
   confidentiality controls (§8, §11a). Unmasked URL logging noted in §8/§10.
7. **Variable injection (Q9):** Resolved — raw unescaped substitution is a
   DISCLAIMED §9 non-guarantee; sanitizing untrusted variables is the author's
   DOWNSTREAM responsibility (§9, §11).
8. **Scripting/exec (Q12):** Resolved — full host capability by design
   (DISCLAIMED), with the opt-in default-empty Janino deny-list as the only
   hardening hook (§9, §11a).
9. **Host environment / credential rotation (Q10/Q11):** Confirmed
   downstream/out-of-scope (§3, §5, §10).
10. **Recurring non-findings (Q18a):** Expanded with concrete per-step bullets
    (§11a).
11. **Discoverability chain (meta):** Confirmed — `AGENTS.md → SECURITY.md →
    THREAT_MODEL.md`.

## §15 Machine-readable companion

Not generated for v1.
