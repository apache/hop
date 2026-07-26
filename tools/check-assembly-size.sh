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
# Fail if published mega-artifacts exceed the ASF Artifactory package limit.
set -euo pipefail

# 850 MiB ASF limit; use a safer default threshold of 800 MiB
MAX_BYTES=${MAX_ASSEMBLY_BYTES:-838860800}
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FAILED=0

check() {
  local label="$1"
  local file="$2"
  if [[ ! -f "$file" ]]; then
    echo "SKIP (missing): $label — $file"
    return 0
  fi
  local size
  size=$(stat -c%s "$file" 2>/dev/null || stat -f%z "$file")
  local mb
  mb=$(awk -v s="$size" 'BEGIN { printf "%.1f", s/1024/1024 }')
  if (( size > MAX_BYTES )); then
    echo "FAIL: $label is ${mb} MB ($(printf '%s' "$size") bytes) > limit $(awk -v s="$MAX_BYTES" 'BEGIN { printf "%.0f", s/1024/1024 }') MB"
    FAILED=1
  else
    echo "OK:   $label is ${mb} MB"
  fi
}

shopt -s nullglob
for f in "$ROOT"/assemblies/client/target/hop-client-*.zip; do
  check "hop-client" "$f"
done
for f in "$ROOT"/assemblies/plugins/target/hop-assemblies-plugins-*.zip; do
  # plugins aggregate should also stay under the limit
  check "hop-assemblies-plugins" "$f"
done

if (( FAILED )); then
  echo "Assembly size check failed (limit ${MAX_BYTES} bytes)."
  exit 1
fi
echo "Assembly size check passed."
