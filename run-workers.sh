#!/usr/bin/env bash
# bash run-workers.sh [MASTER_IP [PORT]]
set -u
fail() { printf '[ERROR] %s\n' "$*" >&2; exit 1; }
script_parent=${BASH_SOURCE[0]%/*}
[[ "$script_parent" != "${BASH_SOURCE[0]}" ]] || script_parent=.
script_dir=$(CDPATH= cd -- "$script_parent" && pwd) || exit 1
(( $# <= 2 )) || fail 'Usage: bash run-workers.sh [MASTER_IP [PORT]]'
candidates=()
[[ -z "${JAVA_HOME:-}" ]] || candidates+=("$JAVA_HOME/bin/java")
path_java=$(type -P java || true)
[[ -z "$path_java" ]] || candidates+=("$path_java")
java_path=''
version_pattern='version[[:space:]]+"?([0-9]+)'
for candidate in "${candidates[@]}"; do
    [[ -x "$candidate" ]] || continue
    if version=$("$candidate" -version 2>&1); then
        if [[ "$version" =~ $version_pattern ]] && (( BASH_REMATCH[1] >= 17 )); then
            java_path=$candidate
            break
        fi
    fi
done
[[ -n "$java_path" ]] || fail 'Java 17+ required. Check installation, JAVA_HOME and PATH. javac is not required to run.'
[[ -f "$script_dir/worker-launcher.jar" ]] || fail 'worker-launcher.jar missing. See README build instructions.'
printf 'Java: %s\n' "$java_path"
"$java_path" -Dfile.encoding=UTF-8 -jar "$script_dir/worker-launcher.jar" "${1:-32.236.94.251}" "${2:-5000}"
result=$?
if [[ -t 0 ]]; then read -r -p 'Press Enter to close...' ignored || true; fi
exit "$result"
