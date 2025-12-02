#!/usr/bin/env bash
# Build & push a multi-arch image for cloud/fog/edge with interactive prompts.
set -euo pipefail

# Defaults
BUILDER="${BUILDER:-multiarch-insecure}"
BUILD_REGISTRY_DEFAULT="${BUILD_REGISTRY_DEFAULT:-localhost:5000}"
PUSH_REGISTRY_DEFAULT="${PUSH_REGISTRY_DEFAULT:-192.168.2.100:5000}"
PIP_INDEX_URL="${PIP_INDEX_URL:-https://pypi.org/simple}"
PIP_TRUSTED_HOST="${PIP_TRUSTED_HOST:-pypi.org}"
INSECURE_FLAG="${INSECURE_FLAG:---insecure}"  # for HTTP registries

PROJECT_BASENAME="$(basename "$(pwd)")"
case "${PROJECT_BASENAME}" in
  federated-grid)   PREFIX_DEFAULT="fedgrid" ;;
  federated-agents) PREFIX_DEFAULT="fedagents" ;;
  *)                PREFIX_DEFAULT="" ;;
esac

read -rp "Component to build (cloud/fog/edge): " COMPONENT
COMPONENT="${COMPONENT,,}"
if [[ -z "${COMPONENT}" ]]; then
  echo "Component is required (cloud|fog|edge)." >&2
  exit 1
fi

DOCKERFILE="${COMPONENT}/Dockerfile"
if [[ ! -f "${DOCKERFILE}" ]]; then
  echo "Dockerfile not found at ${DOCKERFILE}. Run this from repo root." >&2
  exit 1
fi

read -rp "Image name prefix (default: ${PREFIX_DEFAULT:-<required>}): " PREFIX
PREFIX="${PREFIX:-${PREFIX_DEFAULT}}"
if [[ -z "${PREFIX}" ]]; then
  echo "Image prefix is required (e.g., fedgrid or fedagents)." >&2
  exit 1
fi

read -rp "Build registry (default: ${BUILD_REGISTRY_DEFAULT}): " BUILD_REGISTRY
BUILD_REGISTRY="${BUILD_REGISTRY:-${BUILD_REGISTRY_DEFAULT}}"
read -rp "Push registry  (default: ${PUSH_REGISTRY_DEFAULT}): " PUSH_REGISTRY
PUSH_REGISTRY="${PUSH_REGISTRY:-${PUSH_REGISTRY_DEFAULT}}"

BUILD_REPO="${BUILD_REGISTRY}/${PREFIX}-${COMPONENT}"
PUSH_REPO="${PUSH_REGISTRY}/${PREFIX}-${COMPONENT}"
BUILD_REG_HOST="${BUILD_REGISTRY#http://}"
PUSH_REG_HOST="${PUSH_REGISTRY#http://}"

echo "Listing existing tags for build repo ${BUILD_REPO} (if reachable)..."
BUILD_TAGS_JSON="$(curl -fs "http://${BUILD_REG_HOST}/v2/${PREFIX}-${COMPONENT}/tags/list" 2>/dev/null || true)"
if [[ -n "${BUILD_TAGS_JSON}" ]]; then
  mapfile -t BUILD_TAGS < <(echo "${BUILD_TAGS_JSON}" | jq -r '.tags[]?' 2>/dev/null)
  if [[ ${#BUILD_TAGS[@]} -gt 0 ]]; then
    echo "Available tags:"
    idx=1
    for t in "${BUILD_TAGS[@]}"; do
      echo "  [${idx}] ${t}"
      idx=$((idx+1))
    done
  else
    echo "  (no tags returned from registry)"
  fi
else
  echo "  (unable to list tags from http://${BUILD_REG_HOST}/v2/${PREFIX}-${COMPONENT}/tags/list)"
fi

read -rp "Base image tag number or full ref (e.g., ${BUILD_REPO}:v2025-11-11-1800) [leave blank to skip]: " BASE_INPUT
BASE_IMAGE=""
if [[ -n "${BASE_INPUT}" ]]; then
  # if numeric and in range, pick from tag list
  if [[ "${BASE_INPUT}" =~ ^[0-9]+$ ]] && [[ -n "${BUILD_TAGS_JSON}" ]]; then
    sel=$((BASE_INPUT))
    if (( sel >= 1 && sel <= ${#BUILD_TAGS[@]} )); then
      BASE_IMAGE="${BUILD_REPO}:${BUILD_TAGS[$((sel-1))]}"
    else
      echo "  (index out of range; using raw input)"
      BASE_IMAGE="${BASE_INPUT}"
    fi
  else
    BASE_IMAGE="${BASE_INPUT}"
  fi
fi
read -rp "New tag (e.g., v2025-11-28-1300): " NEW_TAG
if [[ -z "${NEW_TAG}" ]]; then
  echo "A new tag is required." >&2
  exit 1
fi

echo "Select build target: [1] both (amd64+arm64) [2] amd64 only [3] arm64 only"
read -rp "Choice (1/2/3, default 1): " ARCH_CHOICE
ARCH_CHOICE="${ARCH_CHOICE:-1}"

PLATFORMS=()
ARCH_SUFFIXES=()
case "${ARCH_CHOICE}" in
  2)
    PLATFORMS=("linux/amd64")
    ARCH_SUFFIXES=("amd64")
    ;;
  3)
    PLATFORMS=("linux/arm64")
    ARCH_SUFFIXES=("arm64")
    ;;
  *)
    PLATFORMS=("linux/amd64" "linux/arm64")
    ARCH_SUFFIXES=("amd64" "arm64")
    ;;
esac

MANIFEST_TAG="${PUSH_REPO}:${NEW_TAG}"
LOCAL_TAGS=()
PUSH_TAGS=()
for arch in "${ARCH_SUFFIXES[@]}"; do
  LOCAL_TAGS+=("${PREFIX}-${COMPONENT}:${arch}-local")
  PUSH_TAGS+=("${PUSH_REPO}:${NEW_TAG}-${arch}")
done

cat <<EOF
Summary
-------
Builder        : ${BUILDER}
Build registry : ${BUILD_REPO}
Push registry  : ${PUSH_REPO}
Tag            : ${NEW_TAG}
Platforms      : ${PLATFORMS[*]}
Base image     : ${BASE_IMAGE:-<none>}
Dockerfile     : ${DOCKERFILE}
EOF

read -rp "Ready to build (local load) for ${PLATFORMS[*]}? [y/N]: " CONT
if [[ "${CONT,,}" != "y" ]]; then
  echo "Aborting."
  exit 0
fi

for i in "${!PLATFORMS[@]}"; do
  plat="${PLATFORMS[$i]}"
  local_tag="${LOCAL_TAGS[$i]}"
  CMD=(docker buildx build
    --builder "${BUILDER}"
    --platform "${plat}"
    --build-arg "PIP_INDEX_URL=${PIP_INDEX_URL}"
    --build-arg "PIP_TRUSTED_HOST=${PIP_TRUSTED_HOST}"
    -t "${local_tag}"
    -f "${DOCKERFILE}"
    .
    --load
  )
  if [[ -n "${BASE_IMAGE}" ]]; then
    CMD+=(--build-arg "BASE_IMAGE=${BASE_IMAGE}")
  fi
  echo "Running: ${CMD[*]}"
  "${CMD[@]}"
done

echo "Tagging for push registry (${PUSH_REPO})..."
for i in "${!LOCAL_TAGS[@]}"; do
  docker tag "${LOCAL_TAGS[$i]}" "${PUSH_TAGS[$i]}"
done

read -rp "Ready to push arch images to ${PUSH_REPO}? [y/N]: " PUSH_CONT
if [[ "${PUSH_CONT,,}" != "y" ]]; then
  echo "Skipping push."
  exit 0
fi

for tag in "${PUSH_TAGS[@]}"; do
  echo "Pushing ${tag}..."
  docker push "${tag}"
done

echo "Creating and pushing manifest ${MANIFEST_TAG}..."
docker manifest create ${INSECURE_FLAG} "${MANIFEST_TAG}" "${PUSH_TAGS[@]}"
docker manifest push ${INSECURE_FLAG} "${MANIFEST_TAG}"

echo "Inspecting final manifest:"
docker buildx imagetools inspect "${MANIFEST_TAG}" || docker manifest inspect ${INSECURE_FLAG} "${MANIFEST_TAG}" || true
echo "Latest tags for ${PUSH_REPO}:"
curl -s "http://${PUSH_REG_HOST}/v2/${PREFIX}-${COMPONENT}/tags/list" | jq || true

read -rp "Cleanup local images and prune dangling layers? [y/N]: " CLEAN
if [[ "${CLEAN,,}" == "y" ]]; then
  echo "Removing local image references (if present)..."
  for t in "${LOCAL_TAGS[@]}"; do docker image rm -f "${t}" 2>/dev/null || true; done
  for t in "${PUSH_TAGS[@]}"; do docker image rm -f "${t}" 2>/dev/null || true; done
  docker image rm -f "${MANIFEST_TAG}" 2>/dev/null || true
  echo "Pruning dangling images..."
  docker image prune -f >/dev/null || true
  echo "Cleanup complete."
fi
