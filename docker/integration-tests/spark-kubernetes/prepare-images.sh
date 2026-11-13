#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Images for the spark-native-kubernetes integration test. k3s cannot pull an image that only
# exists in the local Docker daemon, but imports every tarball in its agent images folder at
# startup: build the Spark on Kubernetes image, pull SeaweedFS, and save both there.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGES="${HERE}/images"
SPARK_VERSION="${SPARK_NATIVE_VERSION:-4.1.4}"
SEAWEEDFS_IMAGE="chrislusf/seaweedfs:4.48"

mkdir -p "${IMAGES}"
docker build -q -f "${HERE}/../spark/Dockerfile.kubernetes" \
  --build-arg SPARK_VERSION="${SPARK_VERSION}" \
  -t "hop-spark-kubernetes:${SPARK_VERSION}" "${HERE}/../spark"
docker pull -q "${SEAWEEDFS_IMAGE}"
docker save -o "${IMAGES}/hop-it-images.tar" "hop-spark-kubernetes:${SPARK_VERSION}" "${SEAWEEDFS_IMAGE}"
echo "Saved the Spark on Kubernetes and SeaweedFS images to ${IMAGES}/hop-it-images.tar"
