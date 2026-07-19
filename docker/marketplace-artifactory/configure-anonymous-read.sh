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
# Artifactory OSS (JFrog Platform UI, 7.x):
#   - Repository create / many admin REST APIs are Pro-only (HTTP 400).
#   - This script prints deep links into the *current* Platform UI and verifies
#     anonymous read afterward.
#
# Usage:
#   # After admin password is set, open the links and complete A–C, then:
#   export ARTIFACTORY_PASSWORD='…'
#   ./docker/marketplace-artifactory/configure-anonymous-read.sh
#
set -euo pipefail

BASE_URL="${ARTIFACTORY_URL_BASE:-http://localhost:8082}"
USER="${ARTIFACTORY_USER:-admin}"
PASS="${ARTIFACTORY_PASSWORD:-}"
REPO_KEY="${ARTIFACTORY_REPO_KEY:-hop-plugins-local}"
REPO_URL="${BASE_URL}/artifactory/${REPO_KEY}/"

# JFrog Platform SPA deep links (7.x). All load the same app; path selects the page.
UI_REPOS="${BASE_URL}/ui/admin/repositories"
UI_ANON="${BASE_URL}/ui/admin/security/general"
UI_PERMS="${BASE_URL}/ui/admin/user_management/permissions"
UI_HOME="${BASE_URL}/ui/"

echo "============================================================"
echo " Hop marketplace + Artifactory OSS (JFrog Platform UI)"
echo "============================================================"
echo
echo "You are on Artifactory OSS. Repo-create REST is Pro-only — use the UI."
echo "Open ${UI_HOME} and log in as admin, then use the links below."
echo
echo "If the left menu looks empty: top of the page switch"
echo "  Application  |  Administration   ← click Administration"
echo "  (or the gear / 'Administration' control in the top bar)"
echo

cat <<EOF
────────────────────────────────────────────────────────────────
 A) Create local Maven repository: ${REPO_KEY}
────────────────────────────────────────────────────────────────
 Open:  ${UI_REPOS}

 1. Ensure you are in the Administration module (see above).
 2. Left nav: Artifactory → Repositories
    (or search the admin menu for "Repositories")
 3. Click  + Add Repositories  /  Create a Repository  /  New Repository
 4. Choose  Local Repository
 5. Package type:  Maven
 6. Repository Key:  ${REPO_KEY}
 7. Repository Layout:  maven-2-default  (usually default for Maven)
 8. Create / Save

────────────────────────────────────────────────────────────────
 B) Allow Anonymous Access (global switch)
────────────────────────────────────────────────────────────────
 Open:  ${UI_ANON}

 1. Administration module
 2. Left nav:  Security → General
    (older docs said "User Management → Settings" — same toggle, new place)
 3. Enable  Allow Anonymous Access
 4. Save

────────────────────────────────────────────────────────────────
 C) Anonymous READ only on ${REPO_KEY}  (no Deploy)
────────────────────────────────────────────────────────────────
 Open:  ${UI_PERMS}

 Why: since Artifactory 7.84+, anonymous is NOT on "Anything" by default.

 1. Administration → User Management → Permissions
 2. + New Permission  /  Add Permission
 3. Name:  hop-plugins-anonymous-read
 4. Resources / Repositories tab:
      - Select ONLY "${REPO_KEY}"
      - Avoid "Any Local" / "Anything" unless you intend global read
 5. Users tab:
      - Add user:  anonymous
      - Actions:   ☑ Read
                   ☐ Annotate  ☐ Deploy / Cache  ☐ Delete / Overwrite  ☐ Manage
 6. Save / Create

 Security model we want:
   • hop marketplace install  → anonymous GET (no password in Hop)
   • publish-wave1-plugins.sh → admin (or a dedicated deploy user)

EOF

if [[ -z "${PASS}" ]]; then
  echo "────────────────────────────────────────────────────────────────"
  echo " After finishing A–C in the UI, verify with:"
  echo "   export ARTIFACTORY_PASSWORD='your-admin-password'"
  echo "   $0"
  echo "────────────────────────────────────────────────────────────────"
  exit 0
fi

AUTH=(-u "${USER}:${PASS}")

echo "==> Checking Artifactory"
if ! curl -sf "${BASE_URL}/artifactory/api/system/ping" >/dev/null; then
  echo "ERROR: ping failed — is the stack up? (${BASE_URL})" >&2
  exit 1
fi
echo "    ping OK"

LICENSE=$(curl -sf "${AUTH[@]}" "${BASE_URL}/artifactory/api/system/version" 2>/dev/null \
  | sed -n 's/.*"license"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -1 || true)
echo "    license: ${LICENSE:-unknown}"

echo "==> Admin probe of ${REPO_URL}"
HTTP_AUTH=$(curl -s -o /dev/null -w '%{http_code}' "${AUTH[@]}" "${REPO_URL}" || true)
case "${HTTP_AUTH}" in
  200|404)
    echo "    OK as admin (HTTP ${HTTP_AUTH}) — repo path exists"
    ;;
  401|403)
    echo "ERROR: admin auth failed (HTTP ${HTTP_AUTH}). Check ARTIFACTORY_PASSWORD." >&2
    exit 1
    ;;
  *)
    echo "    WARNING: HTTP ${HTTP_AUTH}. If the repo is missing, finish step A."
    ;;
esac

echo "==> Anonymous probe of ${REPO_URL}"
HTTP_ANON=$(curl -s -o /dev/null -w '%{http_code}' "${REPO_URL}" || true)
case "${HTTP_ANON}" in
  200|404)
    echo "    OK — anonymous read works (HTTP ${HTTP_ANON})"
    echo
    echo "Hop hop-config.json marketplace repo (no username/password):"
    echo "  \"url\": \"${REPO_URL}\""
    echo
    echo "  unset HOP_MARKETPLACE_PASSWORD ARTIFACTORY_PASSWORD"
    echo "  ./hop marketplace install hop-tech-parquet"
    exit 0
    ;;
  401|403)
    echo "    STILL DENIED (HTTP ${HTTP_ANON})"
    echo
    echo "Usually one of:"
    echo "  • Step B not saved (Allow Anonymous Access still off)"
    echo "  • Step C missing — anonymous has no Read on ${REPO_KEY}"
    echo "  • Step A — repo key typo (must be exactly ${REPO_KEY})"
    echo
    echo "Re-open:"
    echo "  B) ${UI_ANON}"
    echo "  C) ${UI_PERMS}"
    exit 1
    ;;
  *)
    echo "    Unexpected HTTP ${HTTP_ANON}. Finish A–C and re-run."
    exit 1
    ;;
esac
