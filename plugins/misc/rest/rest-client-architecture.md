<!--
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
-->
# REST client architecture

How the REST connection metadata type (`hop-misc-rest`) and the REST transform
(`hop-transform-rest`) build and use HTTP clients.

## The problem this replaces

There used to be two client builders that had to be kept in step by hand:

| | REST transform, no connection | REST connection |
|---|---|---|
| built in | `Rest.setConfig()` | `RestConnection.getInvocationBuilder()` |
| connector | Apache 5 | JDK `HttpURLConnection` |
| SSL | `HttpClientManager` | its own `buildSslContext()` |
| auth | a feature on the client | a feature on the client *or* headers on the request |
| parameters | applied to the `WebTarget` | baked into the URL by the caller |
| lifetime | one client per **row**, closed after each | one client per **request**, never closed |

Every row of that table now has a single answer.

Neither side could see the other's decisions, so differences leaked outward: the
transform grew an `appendMatrixAndQueryParams` method purely to compensate for the
connection building its own target, and a `Content-Length` header had to be dropped
because one of the two connectors rejected it.

## The model

One settings object, two sources, one factory.

```
RestMeta fields ──┐
                  ├─► RestClientSettings ──► RestClientFactory ──► one Client
RestConnection  ──┘   (resolved POJO)                              per transform copy
```

* **`RestClientSettings`** — fully resolved: no variables, no encrypted passwords, no
  Hop metadata. Only what describes a *client*: connector, timeouts, proxy, SSL,
  credentials. Anything that varies per request (URL, method, headers, query and
  matrix parameters, body) is deliberately absent.
* **`RestClientFactory`** — the only place in these two plugins that creates a
  `CloseableHttpClient`. Apache HttpClient 5 is used directly; there is no JAX-RS layer
  above it.
* **`RestAuthenticator`** — the only place authentication is decided, for all four schemes
  (`RestAuthType`: none, Basic, Bearer, API key). Preemptive Basic, Bearer and API keys are
  request headers; challenge-response Basic is a credentials provider on the client.
* **`RestConnection.createClientSettings(...)`** and `Rest.createTransformClientSettings()`
  are the two adapters. Downstream code never asks which one ran.

"No connection selected" is not a separate code path — it is an implicit connection
built from the transform's own fields.

## Client lifetime

**One client per transform copy.** A client is bound to a *configuration*, not to a
URL, and it owns a connection pool. Building a request per row is cheap; building a
client per row is not — it means a fresh TCP and TLS handshake for every row, and on the
connection path the abandoned clients were never closed at all.

This holds even when the endpoint comes from an input field: HttpClient 5 pools by
route (scheme + host + port), so one client serving three hosts keeps three pools with
keep-alive on each.

**What must not be cached alongside it** is anything derived *from* the target URL,
because the URL changes per row while the client does not:

* **Proxy bypass.** Whether to use the proxy is a property of the target, not of the client.
  `HttpClientBuilder.setProxy()` installs a `DefaultProxyRoutePlanner`, which returns the
  proxy unconditionally — there is nowhere to express a bypass. Proxy selection therefore
  lives in `RestProxyRoutePlanner`, an `HttpRoutePlanner` set on the builder and consulted
  per route at request time. That is also what makes `nonProxyHosts` correct when the target
  host changes from row to row.
* **Credential scope.** Credentials are bound to an origin — `RestClientSettings.authOrigin`,
  the connection's base URL or the transform's static URL — and `RestAuthenticator` checks
  the request against it, so a row naming a different host gets no credentials. When there
  is no origin to check (a URL field with no base URL), they are sent regardless, which is
  the long-standing behaviour.

## Host names

Underscores are not legal in host names (RFC 1123), but Docker Compose service names allow
them and real deployments use them, so they have to work.

`java.net.URI` will not parse such a host as a server authority: `getHost()` returns `null`
and `getPort()` returns `-1`. Jersey's `Apache5Connector` derived its target host with
exactly those two calls and had no fallback, so the request died with
`NullPointerException: Host name` — in every Jersey version, and unfixable from outside
because the connector is package-private.

HttpClient 5 does not have that gap: `BasicHttpRequest.setUri` falls back to
`URIAuthority.create(uri.getRawAuthority())`, which parses the authority leniently. Going
direct to HttpClient 5 (phase 6) is what makes underscore hosts work, on both paths, with
no special-casing anywhere in Hop.

### Why hostname verification is HttpClient's, not the JDK's

`RestClientFactory` sets `HostnameVerificationPolicy.CLIENT`. Two things depend on it:

* **Underscore hosts over TLS.** The JDK's built-in endpoint identification runs *inside* the
  handshake and pushes the host through `java.net.IDN.toASCII`, which accepts only LDH ASCII.
  `mtls_test` fails there with `Illegal given domain name` before the certificate is compared,
  even when the certificate names it. HttpClient's `DefaultHostnameVerifier` matches against
  the SANs directly and has no such restriction.
* **"Ignore SSL" ignoring the host name.** Supplying a `HostnameVerifier` alone yields policy
  `BOTH`, not `CLIENT` — the built-in check still runs *first* and aborts the handshake, so a
  permissive verifier would never be consulted.

Two things this does **not** change: the trust manager still validates the certificate chain
(endpoint identification is only name matching), and a host name is still verified on every
request — `CLIENT` policy performs no check at all when the verifier is null, so
`DefaultHostnameVerifier` is always installed explicitly when the user has not asked for a
permissive one.

## Preemptive Basic authentication (issue #4196)

Basic credentials either go out on the first request, or wait for a 401 challenge and answer
that. `RestAuthenticator` decides: preemptive writes the `Authorization` header itself,
challenge-response leaves it to the credentials provider on the client.

Both sides now expose the choice, and both store it as `non_preemptive_basic_auth` — inverted,
for the reason in the notes below.

The transform's old `preemptive` key was **removed rather than read**. It was serialized and had
a checkbox, but nothing ever consumed it, so every pipeline ever saved carries
`<preemptive>N</preemptive>`: the default of a control that did nothing, not a decision anyone
made. Honouring that value on upgrade would have switched every existing pipeline to
challenge-response and broken every server that answers an unauthenticated request with anything
other than a 401 — silently, at runtime. Dropping the key instead means an old file loses a value
that never meant anything and keeps the behaviour it has always had.

## Precedence

When a REST connection is selected it supplies **everything** about the client. The
transform's own connection-related fields are ignored, and the dialog disables them.
This replaced an undocumented split where the transform won for proxy and timeouts
while the connection won for SSL and authentication.

For that rule not to lose features, the connection carries everything the transform
offers: proxy (scheme, host, port, credentials, bypass list), connect and read timeouts,
SSL, and every authentication scheme.

Values already stored in old pipelines are never rewritten — a user may deselect the
connection later — they are simply not read, and `init()` logs which ones it ignored.

## Backwards compatibility

* One `@HopMetadataProperty` key has been removed: `RestMeta.preemptive`, in phase 7. Nothing
  ever read it, so no pipeline loses behaviour — see *Preemptive Basic authentication* above.
  Deserialization ignores an element with no matching property, so files still carrying it load
  without complaint. No other key is renamed, removed or given new semantics.
* New connection properties are additive with empty defaults, so old XML loads unchanged.
* `RestConnection.getInvocationBuilder` (all three overloads) is **gone**, along with
  `buildInvocationBuilder`, `getResponseFromUrl` and `disconnect`. They exposed JAX-RS types
  in their signatures, so they could not survive the move off Jersey; each also built and
  owned a client per call, which is the thing this design exists to stop.
  `RestConnection.getResponse(String url)` and `testConnection()` remain and now run on
  HttpClient 5.

## Status

| phase | scope | state |
|---|---|---|
| 1 | shared settings + factory; one client per transform copy, closed on dispose | **done** |
| 2 | Apache 5 for both paths; drop `SET_METHOD_WORKAROUND`; single target builder; TLS context; fix the `baseUrl` + `url` join | **done** |
| 3 | one authenticator, credentials scoped to an origin | **done** |
| 4 | proxy, timeouts and preemptive on the connection; route planner instead of `PROXY_URI` | **done** |
| 5 | delete `RestData.config` / `sslContext`; disable superseded dialog fields | **done** |
| 6 | drop Jersey entirely; HttpClient 5 directly on both paths | **done** |
| 7 | wire preemptive Basic on the transform too; drop the dead `preemptive` key (#4196) | **done** |

`RestData` no longer carries client configuration at all: `config`, `sslContext` and
`basicAuthentication` are gone. What it holds now is per-row request state and the client
itself.

## What phase 3 changed for users

* **Credentials no longer follow a URL field to another host.** A REST transform whose URL
  comes from an input field used to hand the connection's credentials to whatever host a
  row named. They are now sent only to the connection's own origin. A pipeline that
  deliberately relied on the old behaviour — one connection, rows pointing at several hosts
  — will start getting 401s from the other hosts.
* **Every scheme goes out the same way**, so a green connection test now means the same
  thing a real request does. `testConnection` previously replayed its own header sequence.

## What phase 2 changed for users

* **Custom HTTP verbs and PATCH through a REST connection** no longer depend on
  reflecting into `java.net.HttpURLConnection`, so they no longer depend on
  `--add-opens java.base/java.net=ALL-UNNAMED` either. The flag is still passed by the
  launch scripts for other reasons.
* **A REST connection now talks to a proxy through Apache HttpClient 5**, which is what
  makes proxy authentication and an `https://` proxy reachable at all (phase 4).
* **`SSLContext.getInstance("SSL")` became `"TLS"`.** On a modern JVM both resolve to the
  same implementation; the old value merely read as a request for a protocol family that
  has been insecure for a decade.
* **The base URL and the transform URL are joined properly.** Previously raw string
  concatenation: `https://host/` + `/v1` gave `//v1`, `https://host` + `v1` gave
  `hostv1`, and an absolute URL arriving in a URL field was glued onto the base. A value
  carrying its own `scheme://` is now treated as absolute and the base is ignored.

## What phase 4 changed for users

* **A REST connection carries its own proxy**: scheme (so the proxy itself can be reached
  over TLS), host, port, credentials, and a bypass list in JDK `http.nonProxyHosts` syntax.
  The bypass list is evaluated per request, so it works on a transform whose target host
  changes from row to row.
* **A REST connection carries its own timeouts and preemptive-auth setting**, on a new
  Advanced tab in the connection editor.
* **A selected connection now supplies the whole client.** The transform's own proxy,
  authentication, SSL and timeout fields are no longer read; `init()` logs which of them it
  ignored. Previously the transform won for proxy and timeouts while the connection won for
  SSL and authentication, which nothing documented.
* **An empty timeout field on the transform** now leaves the timeout unset rather than
  passing `-1` through. Both mean no timeout; they are simply the same thing now.

## What phase 7 changed for users

* **The transform's "use preemptive authentication" checkbox works.** It was drawn and saved but
  never read, so Basic credentials always went out on the first request whatever it said
  (issue #4196). Unticking it now gives genuine challenge-response: an unauthenticated request
  first, then the credentials in answer to the 401.
* **Existing pipelines are unaffected.** The checkbox now defaults to ticked, which is what every
  pipeline has always done; the old stored value is discarded rather than honoured, because it
  was never a choice anyone made.

## What phase 6 changed for users

* **Underscore host names work everywhere**, on both paths — over TLS as well, which needed
  the hostname-verification change above. Hop's own mTLS integration environment keeps its
  `mtls_test` service name because of it.
* **Host names are matched by HttpClient's verifier** rather than the JDK's. For a certificate
  that was already valid for its host, nothing changes; the two implementations differ only at
  the edges (non-LDH names, and wildcard handling, where HttpClient consults a public-suffix
  list).
* **Nothing else, deliberately.** Phase 6 is a change of HTTP library, not of behaviour:
  the same settings object, the same authenticator, the same precedence rules. What it buys
  is one request path instead of a JAX-RS layer wrapping the client that did the work, and
  roughly 800 lines of adapter and fallback code that no longer exist.

## What phase 5 changed for users

* **The transform dialog greys out what a selected connection supersedes**: proxy, HTTP
  authentication, preemptive, SSL and the two timeouts. The values are kept, so deselecting
  the connection brings them back. Until now they looked editable while being ignored.

## Notes found along the way

* **Basic auth was never non-preemptive.** `HttpAuthenticationFeature.basic()` and
  `basicBuilder()` both resolve to `Mode.BASIC_PREEMPTIVE`, so the two paths always behaved
  identically here.
* **An absent boolean does not keep its field initializer.** Metadata deserialization sets a
  boolean whose key is missing to `false`, so `private boolean x = true;` does not survive a
  load. Both `RestConnection.nonPreemptiveBasicAuth` and `RestMeta.nonPreemptiveBasicAuth` are
  therefore stored inverted, so that anything saved before the option existed loads as
  preemptive — what it always did. Anything added later that must default to true needs the
  same treatment.
* **The `Content-Length` skip is still there.** A user-supplied `Content-Length` is
  dropped rather than sent, because the client computes it from the buffered entity and
  rejects a second one.
* **Matrix parameters are percent-encoded, not form-encoded.** They live in a path segment,
  where `+` means a literal plus rather than a space — so `Rest.encodePathValue` exists
  alongside `encodeQueryValue`. `URIBuilder` has no `matrixParam`, so the segment is
  assembled by hand ahead of any query string.
* **`httpclient` 4.x stays in `lib/core`.** Nothing in the REST plugins uses it any more —
  only `plugins/tech/webdav` does directly — but Beam, Google Cloud and the AWS SDK pull it
  in transitively, which is what the version pin in `lib/pom.xml` is for.
