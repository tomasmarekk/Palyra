#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PROTO_DIR="$ROOT_DIR/schemas/proto"

to_windows_path() {
  local path="$1"
  if [[ "$path" =~ ^/mnt/([a-zA-Z])/(.*)$ ]]; then
    local drive_letter="${BASH_REMATCH[1]}"
    local rest_path="${BASH_REMATCH[2]}"
    rest_path="${rest_path//\//\\}"
    printf "%s:\\%s" "${drive_letter^^}" "$rest_path"
    return
  fi
  printf "%s" "$path"
}

if command -v protoc >/dev/null 2>&1; then
  protoc_bin="protoc"
elif command -v protoc.exe >/dev/null 2>&1; then
  protoc_bin="protoc.exe"
else
  echo "protoc is required to validate protocol schemas." >&2
  exit 1
fi

tmp_dir="$(mktemp -d "$ROOT_DIR/.tmp-proto-validate.XXXXXX")"
trap 'rm -rf "$tmp_dir"' EXIT

proto_files=()
while IFS= read -r file; do
  proto_files+=("$file")
done < <(find "$PROTO_DIR" -type f -name '*.proto' | sort)

if [ "${#proto_files[@]}" -eq 0 ]; then
  echo "No .proto files found under $PROTO_DIR" >&2
  exit 1
fi

if [[ "$protoc_bin" == *.exe ]]; then
  proto_dir_for_protoc="$(to_windows_path "$PROTO_DIR")"
  descriptor_path="$(to_windows_path "$tmp_dir/palyra-protocol.pb")"
  proto_files_for_protoc=()
  for proto_file in "${proto_files[@]}"; do
    proto_files_for_protoc+=("$(to_windows_path "$proto_file")")
  done
  "$protoc_bin" \
    -I "$proto_dir_for_protoc" \
    --include_imports \
    --descriptor_set_out "$descriptor_path" \
    "${proto_files_for_protoc[@]}"
else
  "$protoc_bin" \
    -I "$PROTO_DIR" \
    --include_imports \
    --descriptor_set_out "$tmp_dir/palyra-protocol.pb" \
    "${proto_files[@]}"
fi

echo "Protocol schema validation passed (${#proto_files[@]} files)."
