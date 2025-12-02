#!/usr/bin/env bash
# List tags for a component and delete selected ones from a registry by index.
set -euo pipefail

REGISTRY_DEFAULT="${REGISTRY_DEFAULT:-192.168.2.100:5000}"
INSECURE_FLAG="${INSECURE_FLAG:---insecure}" # for HTTP registries

PROJECT_BASENAME="$(basename "$(pwd)")"
case "${PROJECT_BASENAME}" in
  federated-grid)   PREFIX_DEFAULT="fedgrid" ;;
  federated-agents) PREFIX_DEFAULT="fedagents" ;;
  *)                PREFIX_DEFAULT="" ;;
esac

read -rp "Component to clean (cloud/fog/edge): " COMPONENT
COMPONENT="${COMPONENT,,}"
if [[ -z "${COMPONENT}" ]]; then
  echo "Component is required (cloud|fog|edge)." >&2
  exit 1
fi

read -rp "Image name prefix (default: ${PREFIX_DEFAULT:-<required>}): " PREFIX
PREFIX="${PREFIX:-${PREFIX_DEFAULT}}"
if [[ -z "${PREFIX}" ]]; then
  echo "Image prefix is required (e.g., fedgrid or fedagents)." >&2
  exit 1
fi

read -rp "Registry (default: ${REGISTRY_DEFAULT}): " REGISTRY
REGISTRY="${REGISTRY:-${REGISTRY_DEFAULT}}"
REG_HOST="${REGISTRY#http://}"
REPO="${REGISTRY}/${PREFIX}-${COMPONENT}"

TAGS_JSON="$(curl -fs "http://${REG_HOST}/v2/${PREFIX}-${COMPONENT}/tags/list" 2>/dev/null || true)"
if [[ -z "${TAGS_JSON}" ]]; then
  echo "Could not fetch tags from http://${REG_HOST}/v2/${PREFIX}-${COMPONENT}/tags/list" >&2
  exit 1
fi

mapfile -t TAGS < <(echo "${TAGS_JSON}" | jq -r '.tags[]?' 2>/dev/null)
if [[ ${#TAGS[@]} -eq 0 ]]; then
  echo "No tags found for ${REPO}."
  exit 0
fi

echo "Available tags for ${REPO}:"
idx=1
for t in "${TAGS[@]}"; do
  echo "  [${idx}] ${t}"
  idx=$((idx+1))
done

read -rp "Enter tag indexes to delete (space-separated): " SELECTIONS
if [[ -z "${SELECTIONS}" ]]; then
  echo "No selection made. Exiting."
  exit 0
fi

SELECTED_TAGS=()
for sel in ${SELECTIONS}; do
  if [[ "${sel}" =~ ^[0-9]+$ ]] && (( sel >= 1 && sel <= ${#TAGS[@]} )); then
    SELECTED_TAGS+=("${TAGS[$((sel-1))]}")
  else
    echo "Skipping invalid index: ${sel}"
  fi
done

if [[ ${#SELECTED_TAGS[@]} -eq 0 ]]; then
  echo "No valid tags selected. Exiting."
  exit 0
fi

echo "Tags selected for deletion:"
for t in "${SELECTED_TAGS[@]}"; do
  echo "  - ${t}"
done

read -rp "Proceed to delete these tags from ${REPO}? [y/N]: " CONFIRM
if [[ "${CONFIRM,,}" != "y" ]]; then
  echo "Aborting."
  exit 0
fi

for tag in "${SELECTED_TAGS[@]}"; do
  echo "Processing ${tag}..."
  DIGEST=""
  # prefer schema v2 first, then manifest list
  for ACCEPT in "application/vnd.docker.distribution.manifest.v2+json" \
                "application/vnd.docker.distribution.manifest.list.v2+json" \
                "application/vnd.docker.distribution.manifest.v1+json"; do
    DIGEST=$(curl -fsI -H "Accept: ${ACCEPT}" \
      "http://${REG_HOST}/v2/${PREFIX}-${COMPONENT}/manifests/${tag}" 2>/dev/null | \
      grep -i Docker-Content-Digest | awk '{print $2}' | tr -d $'\r')
    [[ -n "${DIGEST}" ]] && break
  done
  if [[ -z "${DIGEST}" ]]; then
    echo "  Unable to resolve digest for ${tag}; skipping."
    continue
  fi
  echo "  Deleting manifest digest ${DIGEST}..."
  STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE "http://${REG_HOST}/v2/${PREFIX}-${COMPONENT}/manifests/${DIGEST}")
  if [[ "${STATUS}" == "202" || "${STATUS}" == "200" ]]; then
    echo "  Deleted ${tag} (${DIGEST})"
  else
    echo "  Failed to delete ${tag} (${DIGEST}) (HTTP ${STATUS})"
  fi
done

echo "Updated tag list:"
curl -s "http://${REG_HOST}/v2/${PREFIX}-${COMPONENT}/tags/list" | jq || true
