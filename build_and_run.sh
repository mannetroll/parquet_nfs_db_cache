#!/bin/bash
# build_and_run.sh
set -euo pipefail

IMAGE="oracle-scn-test"
CONTAINER="oracle-scn"
VOLUME="oracle_scn_data"
ORACLE_PASSWORD="${ORACLE_PASSWORD:-AdminPassword123}"
APP_USER="SOMEUSER"
APP_PASSWORD="cache"
WIPE=0

usage() {
  echo "Usage: $0 [--wipe]"
  echo
  echo "  --wipe  Remove the existing $VOLUME Docker volume before starting Oracle."
  echo
  echo "Environment:"
  echo "  ORACLE_PLATFORM=linux/amd64  Optional Docker platform override."
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --wipe)
      WIPE=1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

if [[ -n "${ORACLE_PLATFORM:-}" ]]; then
  docker build --platform="$ORACLE_PLATFORM" -t "$IMAGE" .
else
  docker build -t "$IMAGE" .
fi

# If container already exists, remove it (keep volume unless you wipe it explicitly)
if docker ps -a --format '{{.Names}}' | grep -qx "$CONTAINER"; then
  docker rm -f "$CONTAINER"
fi

if [[ "$WIPE" == "1" ]] && docker volume inspect "$VOLUME" >/dev/null 2>&1; then
  docker volume rm "$VOLUME"
fi

if [[ -n "${ORACLE_PLATFORM:-}" ]]; then
  docker run -d --name "$CONTAINER" \
    --platform="$ORACLE_PLATFORM" \
    -p 1521:1521 \
    --shm-size=1g \
    -e ORACLE_PASSWORD="$ORACLE_PASSWORD" \
    -v "$VOLUME":/opt/oracle/oradata \
    "$IMAGE"
else
  docker run -d --name "$CONTAINER" \
    -p 1521:1521 \
    --shm-size=1g \
    -e ORACLE_PASSWORD="$ORACLE_PASSWORD" \
    -v "$VOLUME":/opt/oracle/oradata \
    "$IMAGE"
fi

echo "Oracle container started:"
echo "  name:     $CONTAINER"
echo "  image:    $IMAGE"
echo "  port:     1521"
echo "  service:  FREEPDB1"
echo "  volume:   $VOLUME"
echo "  platform: ${ORACLE_PLATFORM:-native}"
echo "  user:     $APP_USER"
echo "  pass:     $APP_PASSWORD"
