#!/bin/bash

#
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

################################################################################
# Apache Hop Unified Docker Build Script
#
# This script builds all Apache Hop Docker images using a multi-stage build
# approach. It supports building from local source or from a GitHub tag.
#
# Usage:
#   ./build-hop-images.sh [OPTIONS]
#
# Options:
#   -s, --source <local|github>   Build from local source or GitHub (default: local)
#   -t, --tag <tag>               Git tag/branch to build from (default: main)
#   -r, --repo <url>              GitHub repository URL (default: https://github.com/apache/hop.git)
#   -i, --images <list>           Comma-separated list of images to build (default: all)
#                                 Options: see ALL_STAGES variable, or 'all'
#   -v, --version <version>       Version string for image tagging (auto-detected if not provided)
#   -p, --push                    Push images to registry after build
#   -x, --registry <registry>     Docker registry prefix (default: none/local, e.g., apache, myregistry.io)
#   --platforms <platforms>       Build platforms (default: current platform, e.g., linux/amd64,linux/arm64 for multi)
#   --maven-threads <threads>     Maven build threads (default: 1C, e.g., 2C, 4, 8)
#   --progress <mode>             Docker build output mode: auto (default), plain (verbose), tty (compact)
#   --builder <type>              Builder type: full (Maven, default) or fast (pre-built artifacts)
#   --skip-fat-jar                Skip building the fat jar (auto-detected based on images, only needed for dataflow/web-beam)
#   --no-cache                    Build without using cache
#   -h, --help                    Show this help message
#
# Examples:
#   # Build all images from local source
#   ./build-hop-images.sh
#
#   # Build from GitHub tag 2.9.0
#   ./build-hop-images.sh --source github --tag 2.9.0
#
#   # Fast build using pre-built artifacts (for local dev)
#   ./build-hop-images.sh --builder fast --images web
#
#   # Build web image with beam variant (includes fat jar for Dataflow)
#   ./build-hop-images.sh --images web-beam
#
#   # Build web image only (fat jar automatically skipped)
#   ./build-hop-images.sh --images web
#
#   # Build specific variants
#   ./build-hop-images.sh --images client-minimal,web-beam
#
#   # Build for multiple platforms and push to Docker Hub
#   ./build-hop-images.sh --platforms linux/amd64,linux/arm64 --push --registry apache
#
#   # Manually skip fat jar generation (saves time if not building dataflow/web-beam)
#   ./build-hop-images.sh --images web,client --skip-fat-jar
#
################################################################################

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Available image stages (add new stages here)
# Format: "baseImage" or "baseImage-variant"
ALL_STAGES=("client" "web" "web-beam" "dataflow")

# Detect current platform architecture
detect_platform() {
    local arch=$(uname -m)
    case "$arch" in
        x86_64|amd64)
            echo "linux/amd64"
            ;;
        aarch64|arm64)
            echo "linux/arm64"
            ;;
        *)
            echo "linux/amd64"  # fallback to amd64
            ;;
    esac
}

# Default values
SOURCE_TYPE="local"
GIT_TAG="main"
GIT_REPO="https://github.com/apache/hop.git"
IMAGES="all"
VERSION=""
PUSH=false
REGISTRY=""  # Empty = local registry (no prefix)
PLATFORMS="$(detect_platform)"  # Default to current platform only
USE_CACHE="true"
MAVEN_THREADS="1C"  # Maven thread count (1C, 2C, or specific number like 4)
BUILD_PROGRESS="auto"  # Docker build progress output (auto, plain, tty)
BUILDER_TYPE="full"  # Builder type: full (Maven build) or fast (pre-built artifacts)
SKIP_FAT_JAR="auto"  # Whether to skip fat jar generation (auto, true, false)
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Stage names follow pattern: <base>-<variant> or just <base>
# Examples:
#   client         → hop
#   client-minimal → hop
#   web            → hop-web
#   web-beam       → hop-web
get_image_name() {
    local stage_name="$1"
    
    # Check if stage has a variant suffix
    case "$stage_name" in
        client*)   echo "hop" ;;
        web*)      echo "hop-web" ;;

        dataflow*) echo "hop-dataflow-template" ;;
        *)         echo "" ;;
    esac
}

# Extract variant suffix from stage name
# Examples:
#   client         → ""
#   client-minimal → "minimal"
#   web-beam       → "beam"
get_variant_suffix() {
    local stage_name="$1"
    
    # Check if there's a dash in the name
    if [[ "$stage_name" == *"-"* ]]; then
        # Extract everything after the first dash
        echo "${stage_name#*-}"
    else
        echo ""
    fi
}

# Function to print colored messages
print_info() {
    echo -e "${BLUE}ℹ${NC} $1"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_header() {
    echo -e "\n${BLUE}═══════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}\n"
}

# Function to show help
show_help() {
    sed -n '/^# Usage:/,/^################################################################################$/p' "$0" | grep -v "^#####" | sed 's/^# //'
    exit 0
}

# Function to determine if fat jar is needed based on images being built
needs_fat_jar() {
    local images_list="$1"
    
    # Fat jar is only needed for dataflow and web-beam images
    if [[ "$images_list" == "all" ]]; then
        # Building all images includes dataflow and web-beam
        return 0
    fi
    
    # Check if any of the requested images needs the fat jar
    IFS=',' read -ra images_array <<< "$images_list"
    for img in "${images_array[@]}"; do
        img=$(echo "$img" | xargs)  # trim whitespace
        if [[ "$img" == "dataflow"* ]] || [[ "$img" == "web-beam"* ]]; then
            return 0
        fi
    done
    
    # No images that need fat jar
    return 1
}

# Function to detect version from pom.xml
detect_version() {
    if [ -f "$PROJECT_ROOT/pom.xml" ]; then
        # Get the project version (not parent version)
        # Remove parent block, then get first version tag
        VERSION=$(sed -n '/<parent>/,/<\/parent>/d; s/.*<version>\(.*\)<\/version>.*/\1/p' "$PROJECT_ROOT/pom.xml" | head -1 | xargs)
        
        if [ -z "$VERSION" ]; then
            # Fallback to maven command if available
            if command -v mvn &> /dev/null; then
                VERSION=$(cd "$PROJECT_ROOT" && mvn help:evaluate -Dexpression=project.version -q -DforceStdout 2>/dev/null || echo "")
            fi
        fi
        
        if [ -z "$VERSION" ]; then
            VERSION="latest"
            print_warning "Could not detect version, using: $VERSION"
        else
            print_info "Detected version from pom.xml: $VERSION"
        fi
    else
        VERSION="latest"
        print_warning "Could not detect version, using: $VERSION"
    fi
}

# Function to validate prerequisites
check_prerequisites() {
    print_header "Checking Prerequisites"
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed or not in PATH"
        exit 1
    fi
    print_success "Docker: $(docker --version)"
    
    # Check if Docker daemon is running
    if ! docker info &> /dev/null; then
        print_error "Docker daemon is not running"
        exit 1
    fi
    print_success "Docker daemon is running"
    
    # Check buildx for multi-platform builds
    if echo "$PLATFORMS" | grep -q ","; then
        if ! docker buildx version &> /dev/null; then
            print_error "Docker buildx is required for multi-platform builds"
            exit 1
        fi
        print_success "Docker buildx is available"
    fi
}

# Function to load environment file if exists
load_env_file() {
    local env_file="$SCRIPT_DIR/build.env"
    if [[ -f "$env_file" ]]; then
        print_info "Loading configuration from build.env"
        # shellcheck disable=SC1090
        source "$env_file"
    fi
}

# Function to parse command line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -s|--source)
                SOURCE_TYPE="$2"
                shift 2
                ;;
            -t|--tag)
                GIT_TAG="$2"
                shift 2
                ;;
            -r|--repo)
                GIT_REPO="$2"
                shift 2
                ;;
            -i|--images)
                IMAGES="$2"
                shift 2
                ;;
            -v|--version)
                VERSION="$2"
                shift 2
                ;;
            -p|--push)
                PUSH=true
                shift
                ;;
            -x|--registry)
                REGISTRY="$2"
                shift 2
                ;;
            --platforms)
                PLATFORMS="$2"
                shift 2
                ;;
            --maven-threads)
                MAVEN_THREADS="$2"
                shift 2
                ;;
            --progress)
                BUILD_PROGRESS="$2"
                shift 2
                ;;
            --builder)
                BUILDER_TYPE="$2"
                shift 2
                ;;
            --skip-fat-jar)
                SKIP_FAT_JAR="true"
                shift
                ;;
            --no-cache)
                USE_CACHE="false"
                shift
                ;;
            -h|--help)
                show_help
                ;;
            *)
                print_error "Unknown option: $1"
                echo "Use --help for usage information"
                exit 1
                ;;
        esac
    done
}

# Function to validate arguments
validate_args() {
    # Validate source type
    if [[ "$SOURCE_TYPE" != "local" && "$SOURCE_TYPE" != "github" ]]; then
        print_error "Invalid source type: $SOURCE_TYPE (must be 'local' or 'github')"
        exit 1
    fi
    
    # If building from local, check if we're in the right directory
    if [[ "$SOURCE_TYPE" == "local" && ! -f "$PROJECT_ROOT/pom.xml" ]]; then
        print_error "pom.xml not found in $PROJECT_ROOT"
        print_error "Make sure you're running this script from the Hop project directory"
        exit 1
    fi
}

# Function to build an image
build_image() {
    local stage_name=$1
    local image_name=$(get_image_name "$stage_name")
    local variant_suffix=$(get_variant_suffix "$stage_name")
    
    if [ -z "$image_name" ]; then
        print_warning "Unknown stage name: $stage_name (skipping)"
        return 1
    fi
    
    # Build full image name with optional registry prefix
    local full_image_name
    if [ -n "$REGISTRY" ]; then
        full_image_name="$REGISTRY/$image_name"
    else
        full_image_name="$image_name"
    fi
    
    # Display what we're building
    if [ -n "$variant_suffix" ]; then
        print_header "Building $image_name (variant: $variant_suffix)"
    else
        print_header "Building $image_name"
    fi
    
    # Detect CPU cores for Maven threading
    local cpu_cores
    if [[ "$OSTYPE" == "darwin"* ]]; then
        cpu_cores=$(sysctl -n hw.ncpu 2>/dev/null || echo "4")
    else
        cpu_cores=$(nproc 2>/dev/null || echo "4")
    fi
    local maven_threads="${MAVEN_THREADS:-1C}"
    
    # Determine the primary version tag (include variant suffix if applicable)
    local version_tag="$VERSION"
    if [ -n "$variant_suffix" ]; then
        version_tag="${VERSION}-${variant_suffix}"
    fi
    
    # Determine if we should skip fat jar for this specific image
    local skip_fat_jar_value="false"
    if [[ "$SKIP_FAT_JAR" == "true" ]]; then
        skip_fat_jar_value="true"
    elif [[ "$SKIP_FAT_JAR" == "auto" ]]; then
        # Auto-detect: skip if this image doesn't need it
        if [[ "$stage_name" != "dataflow"* ]] && [[ "$stage_name" != "web-beam"* ]]; then
            skip_fat_jar_value="true"
        fi
    fi
    
    if [[ "$skip_fat_jar_value" == "true" ]]; then
        print_info "Skipping fat jar generation (not needed for $stage_name)"
    fi
    
    # Build arguments
    local build_args=(
        "--build-arg" "HOP_BUILD_FROM_SOURCE=$SOURCE_TYPE"
        "--build-arg" "TARGET_IMAGE=$stage_name"
        "--build-arg" "MAVEN_THREADS=$maven_threads"
        "--build-arg" "BUILDER_TYPE=$BUILDER_TYPE"
        "--build-arg" "SKIP_FAT_JAR=$skip_fat_jar_value"
        "--progress" "$BUILD_PROGRESS"
        "--file" "$SCRIPT_DIR/unified.Dockerfile"
        "--target" "$stage_name"
        "--tag" "$full_image_name:$version_tag"
    )
    
    # Add git arguments if building from GitHub
    if [[ "$SOURCE_TYPE" == "github" ]]; then
        build_args+=(
            "--build-arg" "HOP_GIT_REPO=$GIT_REPO"
            "--build-arg" "HOP_GIT_TAG=$GIT_TAG"
        )
    fi
    
    # Add cache flag
    if [[ "$USE_CACHE" == "false" ]]; then
        build_args+=("--no-cache")
    fi
    
    # Add additional tags based on version and variant
    # Tag format examples:
    #   Standard: hop-web:2.9.0, hop-web:Development, hop-web:latest
    #   Variant:  hop-web:2.9.0-beam, hop-web:Development-beam, hop-web:beam-latest
    if [[ "$VERSION" =~ SNAPSHOT ]]; then
        # SNAPSHOT versions get "Development" tag
        if [ -n "$variant_suffix" ]; then
            build_args+=("--tag" "$full_image_name:Development-${variant_suffix}")
        else
            build_args+=("--tag" "$full_image_name:Development")
        fi
    else
        # Release versions get "latest" tag
        if [ -n "$variant_suffix" ]; then
            build_args+=("--tag" "$full_image_name:${variant_suffix}-latest")
        else
            build_args+=("--tag" "$full_image_name:latest")
        fi
    fi
    
    # Build command (primary_tag already set as version_tag above)
    if echo "$PLATFORMS" | grep -q ","; then
        # Multi-platform build requires buildx
        build_args+=(
            "--platform" "$PLATFORMS"
        )
        
        if [[ "$PUSH" == "true" ]]; then
            build_args+=("--push")
        else
            build_args+=("--load")
        fi
        
        print_info "Building multi-platform image: $PLATFORMS"
        # Use default buildx builder (no need to create/destroy)
        docker buildx build "${build_args[@]}" "$PROJECT_ROOT"
    else
        # Single platform build (faster, smaller, can use regular docker build)
        print_info "Building for platform: $PLATFORMS"
        
        # Use buildx only if pushing, otherwise use regular docker build (faster)
        if [[ "$PUSH" == "true" ]]; then
            build_args+=(
                "--platform" "$PLATFORMS"
                "--push"
            )
            
            # Use default buildx builder
            docker buildx build "${build_args[@]}" "$PROJECT_ROOT"
        else
            # Regular docker build for single platform (no buildx overhead)
            build_args+=(
                "--platform" "$PLATFORMS"
            )
            docker build "${build_args[@]}" "$PROJECT_ROOT"
        fi
    fi
    
    print_success "Successfully built $full_image_name:$version_tag"
}

# Function to build all requested images
build_images() {
    print_header "Build Configuration"
    echo "Source:        $SOURCE_TYPE"
    if [[ "$SOURCE_TYPE" == "github" ]]; then
        echo "Repository:    $GIT_REPO"
        echo "Tag:           $GIT_TAG"
    fi
    echo "Images:        $IMAGES"
    echo "Version:       $VERSION"
    if [ -n "$REGISTRY" ]; then
        echo "Registry:      $REGISTRY"
    else
        echo "Registry:      local (no prefix)"
    fi
    echo "Platforms:     $PLATFORMS"
    echo "Builder type:  $BUILDER_TYPE"
    if [[ "$BUILDER_TYPE" == "full" ]]; then
        echo "Maven threads: $MAVEN_THREADS"
    fi
    
    # Show fat jar status
    if [[ "$SKIP_FAT_JAR" == "auto" ]]; then
        if needs_fat_jar "$IMAGES"; then
            echo "Fat jar:       enabled (auto-detected, needed for dataflow/web-beam)"
        else
            echo "Fat jar:       disabled (auto-detected, not needed for selected images)"
        fi
    elif [[ "$SKIP_FAT_JAR" == "true" ]]; then
        echo "Fat jar:       disabled (manual)"
    else
        echo "Fat jar:       enabled"
    fi
    
    echo "Push:          $PUSH"
    echo "Cache:         $USE_CACHE"
    echo ""
    
    # Determine which images to build
    local images_to_build=()
    if [[ "$IMAGES" == "all" ]]; then
        # Build all images including variants
        images_to_build=("${ALL_STAGES[@]}")
    else
        IFS=',' read -ra images_to_build <<< "$IMAGES"
    fi
    
    # Build each image
    for image in "${images_to_build[@]}"; do
        image=$(echo "$image" | xargs) # trim whitespace
        build_image "$image"
    done
}

# Function to print summary
print_summary() {
    print_header "Build Summary"
    
    local images_to_check=()
    if [[ "$IMAGES" == "all" ]]; then
        # Check all images including variants
        images_to_check=("${ALL_STAGES[@]}")
    else
        IFS=',' read -ra images_to_check <<< "$IMAGES"
    fi
    
    echo "Built images:"
    for image in "${images_to_check[@]}"; do
        image=$(echo "$image" | xargs)
        local image_name=$(get_image_name "$image")
        local variant_suffix=$(get_variant_suffix "$image")
        
        if [[ -n "$image_name" ]]; then
            # Build the full image name with registry
            local full_image_name
            if [ -n "$REGISTRY" ]; then
                full_image_name="$REGISTRY/$image_name"
            else
                full_image_name="$image_name"
            fi
            
            # Primary version tag (with variant suffix if applicable)
            local version_tag="$VERSION"
            if [ -n "$variant_suffix" ]; then
                version_tag="${VERSION}-${variant_suffix}"
            fi
            echo "  - $full_image_name:$version_tag"
            
            # Additional alias tag (Development or latest)
            if [[ "$VERSION" =~ SNAPSHOT ]]; then
                if [ -n "$variant_suffix" ]; then
                    echo "  - $full_image_name:Development-${variant_suffix}"
                else
                    echo "  - $full_image_name:Development"
                fi
            else
                if [ -n "$variant_suffix" ]; then
                    echo "  - $full_image_name:${variant_suffix}-latest"
                else
                    echo "  - $full_image_name:latest"
                fi
            fi
        fi
    done
    
    if [[ "$PUSH" == "true" ]]; then
        print_success "Images pushed to registry"
    else
        print_info "Images available locally (not pushed)"
    fi
    
    echo ""
    print_success "Build completed successfully!"
}

################################################################################
# Main execution
################################################################################

main() {
    print_header "Apache Hop Docker Build System"
    
    # Load environment file if exists (before parsing args so args can override)
    load_env_file
    
    # Parse command line arguments
    parse_args "$@"
    
    # Check prerequisites
    check_prerequisites
    
    # Validate arguments
    validate_args
    
    # Detect version if not provided
    if [[ -z "$VERSION" ]]; then
        detect_version
    fi
    
    # Build images
    build_images
    
    # Print summary
    print_summary
}

# Run main function
main "$@"

