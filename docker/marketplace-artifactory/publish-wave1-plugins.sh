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
# Deploy Wave 1 marketplace plugin zips to a local Maven repository
# (Artifactory, Nexus, or any HTTP Maven layout endpoint that accepts deploy-file).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPO_URL="${ARTIFACTORY_URL:-http://localhost:8082/artifactory/hop-plugins-local}"
REPO_ID="${ARTIFACTORY_REPO_ID:-hop-plugins-local}"
USER="${ARTIFACTORY_USER:-admin}"
PASS="${ARTIFACTORY_PASSWORD:-}"
VERSION="${HOP_VERSION:-2.19.0-SNAPSHOT}"
GROUP_ID="org.apache.hop"

# artifactId → path to built zip (relative to repo root)
declare -A ZIPS=(
  [hop-engines-spark]="plugins/engines/spark/target/hop-engines-spark-${VERSION}.zip"
  [hop-engines-beam]="plugins/engines/beam/target/hop-engines-beam-${VERSION}.zip"
  [hop-transform-script]="plugins/transforms/script/target/hop-transform-script-${VERSION}.zip"
  [hop-tech-cassandra]="plugins/tech/cassandra/target/hop-tech-cassandra-${VERSION}.zip"
  [hop-transform-tika]="plugins/transforms/tika/target/hop-transform-tika-${VERSION}.zip"
  [hop-transform-drools]="plugins/transforms/drools/target/hop-transform-drools-${VERSION}.zip"
  [hop-tech-parquet]="plugins/tech/parquet/target/hop-tech-parquet-${VERSION}.zip"
  [hop-transform-stanfordnlp]="plugins/transforms/stanfordnlp/target/hop-transform-stanfordnlp-${VERSION}.zip"
  [hop-tech-arrow]="plugins/tech/arrow/target/hop-tech-arrow-${VERSION}.zip"
  [hop-tech-dropbox]="plugins/tech/dropbox/target/hop-tech-dropbox-${VERSION}.zip"
  [hop-transform-edi2xml]="plugins/transforms/edi2xml/target/hop-transform-edi2xml-${VERSION}.zip"
)

if [[ -z "$PASS" ]]; then
  echo "Set ARTIFACTORY_PASSWORD (and optionally ARTIFACTORY_USER / ARTIFACTORY_URL)" >&2
  exit 1
fi

MVN="${ROOT}/mvnw"
if [[ ! -x "$MVN" ]]; then
  MVN=mvn
fi

deploy_one() {
  local artifactId="$1"
  local rel="$2"
  local file="$ROOT/$rel"
  if [[ ! -f "$file" ]]; then
    echo "SKIP (not built): $artifactId — missing $rel" >&2
    return 0
  fi
  echo "Deploying $artifactId → $REPO_URL"
  "$MVN" -q org.apache.maven.plugins:maven-deploy-plugin:3.1.3:deploy-file \
    -DgroupId="$GROUP_ID" \
    -DartifactId="$artifactId" \
    -Dversion="$VERSION" \
    -Dpackaging=zip \
    -Dfile="$file" \
    -DrepositoryId="$REPO_ID" \
    -Durl="$REPO_URL" \
    -DgeneratePom=true \
    -DretryFailedDeploymentCount=2
}

# Allow password via settings or wagon; deploy-file uses server credentials from settings.xml
# when repositoryId matches. Also support inline for local-only:
export MAVEN_OPTS="${MAVEN_OPTS:-}"

for artifactId in "${!ZIPS[@]}"; do
  deploy_one "$artifactId" "${ZIPS[$artifactId]}"
done

echo "Done. Configure hop marketplace repositories[0].url to: ${REPO_URL%/}/"
echo "Example: hop marketplace install hop-tech-parquet"
