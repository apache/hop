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

# Apache Hop — Threat Model (v1 draft)

**Status:** v1 draft, authored by the ASF Security Team for the Hop PMC to
review. Drafted against `apache/hop` (default branch `main`), 2026-06-01.

**Provenance legend:** *(documented)* — from the project's README / repo
structure / docs; *(maintainer)* — ratified by a Hop PMC member; *(inferred)*
— reasoned from architecture / domain norms, **not yet confirmed** (each has a
matching §14 item).

**Draft confidence:** documented ~10 · maintainer 0 · inferred ~22. A
react-to-me draft — the Hop PMC (François Papon, Hans Van Akelyen)
confirms/corrects, especially the §14 items.

**Revision triggers:** a change to the Hop Server's auth/remote-execution
model; a new scripting/exec transform or action; a change to how connection
credentials / variables are stored or resolved; a new metadata source.

---

## §1 Header

- **Project:** Apache Hop (`apache/hop`) — the "Hop Orchestration Platform":
  visual **data and metadata orchestration** (pipelines + workflows) authored
  in a GUI and run on a local engine, a remote Hop Server, or Beam runners.
  *(documented — README, repo layout)*
- **Scope of this model:** the `apache/hop` repository only (`hop-website`
  excluded per the PMC). *(documented — PMC, 2026-05-15)*
- **What Hop is:** a tool that **executes operator-authored data pipelines
  and workflows** — by design those can read/write files, query databases,
  call services, and run scripting/exec steps. It is not a sandbox and not a
  multi-tenant isolation boundary. *(inferred — Q1)*

## §2 Scope and intended use

Hop is operated by a **data engineer / operator** who authors pipelines
(`.hpl`) and workflows (`.hwf`) and runs them — interactively in Hop GUI, via
the `hop-run` CLI, or submitted to a **Hop Server** for local/remote
execution. *(documented — README "Trying Apache Hop"; `engine/`, `rest/`,
`ui/` dirs)*

### Component families (distinct threat profiles)

| Family | Path | Role / trust notes |
|---|---|---|
| Execution engine | `engine/`, `engine-beam/` | Runs pipelines/workflows the operator authored — incl. transforms that touch files, DBs, network, and **scripting/exec** steps. *(documented)* |
| **Hop Server (REST / web)** | `rest/`, `rap/` | Remote-execution + web surface: accepts pipeline/workflow run requests over HTTP. The **network trust boundary** — who may submit/run, and with what auth. *(inferred — Q5)* |
| GUI (desktop / web) | `rcp/`, `rap/`, `ui/` | Authoring surface used by the trusted operator. *(documented)* |
| Plugins | `plugins/` | The large transform/action set, incl. scripting (JS/Groovy-style), shell/exec, SQL, and per-DB/per-cloud connectors. *(documented)* |
| Connection / driver layer | `lib-jdbc/`, metadata | DB/file/cloud credentials + JDBC drivers the operator configures. *(documented)* |

**In scope:** engine + Hop Server + plugins + connection layer + GUI glue.
**Intended caller trust:** the pipeline/workflow **author and the operator
running them are trusted** (they already direct what Hop does). The
**network-facing Hop Server** is where an untrusted actor can appear.
*(inferred — Q2)*

## §3 Out of scope (explicit non-goals)

- `hop-website`, `docs/`, `integration-tests/`, `assemblies/`, samples/demos
  — not the shipped runtime product. *(inferred — Q3)*
- **Bundled third-party JDBC drivers / libraries' own vulnerabilities** — a
  CVE in a packaged driver is a dependency/supply-chain matter, not a Hop
  defect (Hop's job is to call them correctly). *(inferred — Q3)*
- **Credential acquisition / storage / rotation** for the DBs/services Hop
  connects to — the operator supplies these via connections/variables.
  *(inferred — Q10)*

## §4 Trust boundaries and data flow

1. **Pipeline/workflow author → engine.** The author is trusted; what they
   build (incl. scripting/exec/SQL steps) is *intended* execution, not an
   attack (§7, §9). *(inferred — Q2)*
2. **Remote client → Hop Server (HTTP/REST).** The high-value boundary: a
   network client requests pipeline/workflow execution. Auth/authz, transport
   security, and "who can run what" live here. *(inferred — Q5,Q6)*
3. **Metadata / project ingestion.** Pipeline/workflow + metadata files
   loaded from disk/VCS. Loading an **untrusted** `.hpl`/`.hwf` is effectively
   running untrusted instructions (§11). *(inferred — Q7)*
4. **Connection layer → external DB/file/cloud.** Carries operator-configured
   credentials; responses are backend-controlled. *(inferred — Q6,Q8)*
5. **Variables / parameters.** Resolved into transform config at run time — an
   injection surface if sourced from untrusted input. *(inferred — Q9)*

## §5 Assumptions about the environment

JVM; the OS the engine/server runs on (file + process access for exec/shell
steps); network reachability to configured backends + the Hop Server's listen
interface; system trust store for TLS. *(inferred — Q11)*

## §6 Assumptions about inputs — per-parameter trust

| Input | Source | Trusted? | Notes |
|---|---|---|---|
| Pipeline/workflow definitions (`.hpl`/`.hwf`) | Operator/author | Trusted | Authoring is the operator's; running an untrusted definition = running untrusted code (Q7). |
| Hop Server run requests (REST) | Network client | **Untrusted-capable** | Auth/authz is the boundary — can an unauthenticated client run pipelines? (Q5) |
| Connection configs + credentials | Operator | Trusted (secret) | Must not be logged/leaked (§8, Q8). |
| Variables / parameters | Operator, or upstream input | Untrusted-capable | Injection into SQL/paths/commands if sourced from untrusted data (Q9). |
| Metadata (from project files / VCS) | Operator/author | Trusted-by-default | Untrusted if loaded from an external source (Q7). |
| Row/stream data processed by a pipeline | Upstream sources | Pass-through | Hop moves it; semantic validation is the pipeline author's. *(inferred)* |

## §7 Adversary model

- **Out of scope:** the **pipeline/workflow author and the operator** running
  Hop — they already control execution (a scripting step that runs code is
  their intent, not an attack). *(inferred — Q2)*
- **In scope:** a **remote actor against an exposed Hop Server** (submitting/
  running pipelines, reading results) — bounded by the server's auth/authz;
  and a **network MITM** between Hop and its backends / between client and
  server. *(inferred — Q5,Q6)*
- **Conditional:** an attacker who can get an **untrusted pipeline/metadata
  file** loaded/run, or who controls a **variable/parameter** value that
  reaches a sink. *(inferred — Q7,Q9)*

## §8 Security properties the project provides *(all pending PMC confirmation)*

- **Hop Server access control:** the remote-execution surface is expected to
  enforce authentication/authorization on run requests (default posture is the
  key question). *(inferred — Q5)* — violation symptom: unauthenticated remote
  pipeline execution on a default deployment; severity: critical.
- **Credential confidentiality:** connection passwords / secrets should not be
  written to logs or surfaced in error output. *(inferred — Q8)* — violation:
  a DB password appears in a log/exception; severity: high.
- **Transport security:** TLS available for the Hop Server and for backend
  connections that support it. *(inferred — Q6)*

## §9 Security properties the project does *not* provide

- **Not a sandbox.** A pipeline authored by a trusted operator can run
  scripting/shell/SQL and touch files/network — by design. Hop does not
  confine what an authored pipeline may do. *(inferred — Q1,Q12)*
- **No isolation between pipelines/tenants** running on the same engine/server
  beyond what the OS/JVM provides. *(inferred — Q5)*
- **No defense against a hostile backend** beyond protocol correctness.
  *(inferred — Q6)*
- **No protection if an untrusted `.hpl`/`.hwf` is loaded and run** — that is
  equivalent to executing untrusted instructions. *(inferred — Q7)*
- **No automatic sanitization of variables** flowing into SQL / file paths /
  shell — parameterization/escaping is the pipeline author's. *(inferred — Q9)*

## §10 Downstream (operator) responsibilities

- **Lock down the Hop Server** — authenticate it, don't expose the
  remote-execution API to an untrusted network, restrict who can submit runs.
  *(inferred — Q5)*
- Manage connection credentials; don't source connection config or
  pipeline/metadata files from untrusted input. *(inferred — Q7,Q10)*
- Treat scripting/exec/SQL steps as code; only run pipelines from trusted
  authors; parameterize variables that carry untrusted data. *(inferred — Q9)*
- Use TLS for the server + sensitive backends. *(inferred — Q6)*

## §11 Known misuse patterns

- Exposing a **Hop Server with weak/no auth** to an untrusted network →
  remote pipeline execution = remote code execution. *(inferred — Q5)*
- Running an **untrusted pipeline/workflow file** (or one fetched from an
  untrusted source). *(inferred — Q7)*
- Building unparameterized SQL / shell / file paths from **untrusted variable
  values**. *(inferred — Q9)*

### §11a Known non-findings (recurring false positives) *(seed — PMC to expand, Q18a)*

- "A pipeline can run JavaScript/Groovy/shell/SQL" — **by design**; authored by
  a trusted operator, that is intended functionality, not a vuln. *(inferred — Q1)*
- "Hop connects to a database with a configured password" — operator-supplied
  connection config, not a finding. *(inferred — Q10)*
- "A bundled JDBC driver has CVE-XXXX" — dependency/supply-chain, handled via
  the driver/ASF process, not a Hop code defect. *(inferred — Q3)*
- "Hop reads/writes the files a pipeline tells it to" — that is the operator's
  pipeline doing its job. *(inferred — Q1)*

## §12 Conditions that would change this model

A change to the Hop Server auth/remote-exec model; a new scripting/exec
transform; a change to credential/variable storage or resolution; a new
metadata source format/location. *(inferred)*

## §13 Triage dispositions

| Disposition | When | Section |
|---|---|---|
| **VALID** | Violates a §8 property under the §7 adversary (esp. unauth remote exec, credential leak) in in-scope code | §7, §8 |
| **OUT-OF-MODEL** | Adversary is the trusted author/operator; or code in `hop-website`/tests/samples | §3, §7 |
| **DOWNSTREAM-RESPONSIBILITY** | Hop Server exposure, credential mgmt, running untrusted pipelines, unparameterized untrusted variables | §10 |
| **DISCLAIMED** | A §9 non-guarantee (authored pipeline runs code; bundled-driver CVE; no pipeline isolation) | §9 |
| **MODEL-GAP** | Plausible, not covered → escalate to PMC | §12 |

## §14 Open questions for the maintainers

1. **(Q1)** Confirm framing: Hop runs operator-authored pipelines (incl. scripting/exec) by design; it's not a sandbox.
2. **(Q2)** Confirm the pipeline author + operator are trusted (out of the adversary model), like most automation/ETL tools.
3. **(Q3)** Confirm out-of-scope: website, tests/samples, and bundled-driver/dependency CVEs.
4. **(Q5)** **Hop Server is the crux:** what is its *default* auth posture? Can a default deployment accept unauthenticated remote run requests, or is auth required / is it bound to localhost by default? Any built-in authz on who can run what?
5. **(Q6)** TLS: available/default for the Hop Server and for backend connections? Trust store?
6. **(Q7)** Is loading/running an **untrusted** `.hpl`/`.hwf`/metadata in or out of scope (i.e. is "only run trusted pipelines" an operator responsibility)?
7. **(Q8)** Credential confidentiality: are connection secrets guaranteed not to be logged / surfaced in errors? How are stored connection passwords protected (encoding vs encryption)?
8. **(Q9)** Variable/parameter injection: is sanitizing untrusted variable values that reach SQL/paths/shell the pipeline author's responsibility?
9. **(Q12)** Scripting/exec steps: any sandboxing or restriction, or full host capability by design?
10. **(Q10/Q11)** Confirm credential acquisition/rotation + host environment are downstream/out-of-scope.
11. **(Q18a)** What do scanners most often report against Hop that you consider non-findings? (Expand §11a — e.g. the scripting/exec steps almost certainly get flagged.)
12. **(meta)** OK to land this as `THREAT_MODEL.md` with the `AGENTS.md → SECURITY.md → THREAT_MODEL.md` chain (this PR creates all three)?

## §15 Machine-readable companion

Not generated for v1.
