#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Download an Apache Hop client and the marketplace plugin zips that go with it,
# so tools/check-plugin-classpath.sh can check them the way a user's install would
# see them.
#
# The client zip is the right baseline and the container image is not: the image is
# built from a client that already had every marketplace plugin installed into it
# (Apache Hop's Jenkinsfile unpacks the client, runs install-wave1-plugins.sh, then
# builds the image from that directory). Checking against the image would let each
# plugin borrow the jars of all the others, which is exactly the bug being hunted.
# The published client zip is what a user downloads, and it is plugin-free.
#
# The plugin list is read from the client's own full-client-env.yaml rather than
# from Apache Hop's optional-plugins.yaml over the network, so the list and the
# baseline are always the same version.
#
# Releases and snapshots are both supported. A release client comes from the ASF
# dist archive; a -SNAPSHOT client comes from the snapshot repository, where the
# assemblies are published under the artifactId hop-client (the deploy-snapshots
# excludes drop hop-assemblies*, which that name does not match). Snapshot files are
# timestamped, so their real names have to be resolved per artifact from
# maven-metadata.xml — the plain <artifactId>-<version>-SNAPSHOT.zip name is a 404.
#
# Usage:
#   tools/fetch-apache-marketplace.sh --version 2.19.0 --dest <dir> [--only <id>]...
#   tools/fetch-apache-marketplace.sh --version 2.20.0-SNAPSHOT --dest <dir>
#   tools/fetch-apache-marketplace.sh --version 2.19.0 --dest <dir> --client-only
#
# Writes <dir>/client.zip, <dir>/zips/*.zip and <dir>/plugins.txt (the
# "<artifactId> <path>" list that check-plugin-classpath.sh --plugins expects).
#
# --client-only stops after the client, for callers that just need a baseline to
# install something else onto.
#
set -euo pipefail

VERSION=""
DEST=""
CLIENT_ONLY=false
ONLY=()

die() { echo "ERROR: $*" >&2; exit 2; }

while [[ $# -gt 0 ]]; do
  case "$1" in
  --version) shift; VERSION="${1:-}"; [[ -n "${VERSION}" ]] || die "--version needs a value" ;;
  --dest) shift; DEST="${1:-}"; [[ -n "${DEST}" ]] || die "--dest needs a value" ;;
  --only) shift; [[ -n "${1:-}" ]] || die "--only needs an artifactId"; ONLY+=("$1") ;;
  --client-only) CLIENT_ONLY=true ;;
  -h | --help) sed -n '17,50p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
  *) die "unknown argument: $1" ;;
  esac
  shift
done

[[ -n "${VERSION}" ]] || die "--version is required"
[[ -n "${DEST}" ]] || die "--dest is required"
command -v curl >/dev/null 2>&1 || die "curl not on PATH"
command -v unzip >/dev/null 2>&1 || die "unzip not on PATH"

# archive.apache.org keeps every release; downloads.apache.org only the current one,
# so a pinned older version resolves on the archive and not on the mirror.
DIST_URLS=(
  "https://archive.apache.org/dist/hop/${VERSION}/apache-hop-client-${VERSION}.zip"
  "https://downloads.apache.org/hop/${VERSION}/apache-hop-client-${VERSION}.zip"
)
# The marketplace itself resolves plugins from the ASF group first and Central as a
# fallback (see MarketplaceConfig in Apache Hop); mirror that order here so this
# check fails on the same artifacts a user's install would get.
REPO_URLS=(
  "https://repository.apache.org/content/groups/public"
  "https://repo1.maven.org/maven2"
)

case "${VERSION}" in
*-SNAPSHOT) IS_SNAPSHOT=true ;;
*) IS_SNAPSHOT=false ;;
esac

mkdir -p "${DEST}/zips"
CLIENT_ZIP="${DEST}/client.zip"

fetch() {
  local out="$1" url
  shift
  for url in "$@"; do
    if curl -fsSL --retry 3 --retry-delay 2 -o "${out}.part" "${url}"; then
      mv "${out}.part" "${out}"
      printf '%s\n' "${url}"
      return 0
    fi
  done
  rm -f "${out}.part"
  return 1
}

# A snapshot version directory holds timestamped files only; maven-metadata.xml
# names the current one. Each artifact is resolved separately: they are usually from
# the same deploy run, but nothing guarantees it, and reusing one artifact's
# timestamp for another silently 404s.
snapshot_name() {
  local repo="$1" art="$2" value
  value="$(curl -fsSL --retry 2 --max-time 60 \
    "${repo}/org/apache/hop/${art}/${VERSION}/maven-metadata.xml" 2>/dev/null |
    tr '<' '\n' | sed -n 's:^value>::p' | tail -1 || true)"
  [[ -n "${value}" ]] || return 1
  printf '%s-%s.zip\n' "${art}" "${value}"
}

# Candidate URLs for one artifact's zip, across both repositories.
zip_urls() {
  local art="$1" repo name
  for repo in "${REPO_URLS[@]}"; do
    if [[ "${IS_SNAPSHOT}" == true ]]; then
      name="$(snapshot_name "${repo}" "${art}")" || continue
    else
      name="${art}-${VERSION}.zip"
    fi
    printf '%s\n' "${repo}/org/apache/hop/${art}/${VERSION}/${name}"
  done
}

if [[ -f "${CLIENT_ZIP}" ]]; then
  echo "==> Client already downloaded: ${CLIENT_ZIP}"
elif [[ "${IS_SNAPSHOT}" == true ]]; then
  echo "==> Downloading Apache Hop ${VERSION} client from the snapshot repository"
  urls=()
  while IFS= read -r u; do urls+=("$u"); done < <(zip_urls hop-client)
  [[ ${#urls[@]} -gt 0 ]] || die "no snapshot client published for ${VERSION}"
  from="$(fetch "${CLIENT_ZIP}" "${urls[@]}")" ||
    die "could not download the snapshot client for ${VERSION}"
  echo "    from ${from}"
else
  echo "==> Downloading Apache Hop ${VERSION} client"
  from="$(fetch "${CLIENT_ZIP}" "${DIST_URLS[@]}")" ||
    die "could not download the client zip for ${VERSION} (tried ${DIST_URLS[*]})"
  echo "    from ${from}"
fi

if [[ "${CLIENT_ONLY}" == true ]]; then
  echo "    client:  ${CLIENT_ZIP}"
  exit 0
fi

# full-client-env.yaml is generated from optional-plugins.yaml and ships in the
# client, so the list matches the baseline by construction.
ENV_FILE="$(unzip -Z1 "${CLIENT_ZIP}" '*full-client-env.yaml' 2>/dev/null | head -1 || true)"
[[ -n "${ENV_FILE}" ]] ||
  die "full-client-env.yaml not found in the client zip; this Hop version predates the marketplace registry"
unzip -p "${CLIENT_ZIP}" "${ENV_FILE}" >"${DEST}/full-client-env.yaml"

ARTIFACTS="$(awk '/^[[:space:]]*-[[:space:]]*artifactId:/ { print $3 }' "${DEST}/full-client-env.yaml")"
[[ -n "${ARTIFACTS}" ]] || die "no artifactIds in full-client-env.yaml"

wanted() {
  [[ ${#ONLY[@]} -eq 0 ]] && return 0
  local a
  for a in "${ONLY[@]}"; do [[ "$a" == "$1" ]] && return 0; done
  return 1
}

: >"${DEST}/plugins.txt"
count=0
failed=0
for art in ${ARTIFACTS}; do
  wanted "${art}" || continue
  zip="${DEST}/zips/${art}-${VERSION}.zip"
  if [[ ! -f "${zip}" ]]; then
    urls=()
    while IFS= read -r u; do urls+=("$u"); done < <(zip_urls "${art}")
    if [[ ${#urls[@]} -eq 0 ]] || ! fetch "${zip}" "${urls[@]}" >/dev/null; then
      # Not fatal on its own: the check reports it as a missing zip, which is a
      # more useful failure than aborting the whole fetch here.
      echo "    MISSING ${art}"
      failed=$((failed + 1))
      continue
    fi
  fi
  printf '%s %s\n' "${art}" "${zip}" >>"${DEST}/plugins.txt"
  count=$((count + 1))
done

echo "==> ${count} plugin zip(s) ready in ${DEST}/zips"
[[ ${failed} -gt 0 ]] && echo "    ${failed} could not be downloaded"
echo "    client:  ${CLIENT_ZIP}"
echo "    plugins: ${DEST}/plugins.txt"
exit 0
