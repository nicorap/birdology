#!/usr/bin/env bash
# Move every birdology artifact off the shared ML folders into the rapnic home.
#
#   /faic/weka/ML/<item>    -> /srv/home/rapnic/birdology/data/<item>
#   /shared/apps/ML/<item>  -> /srv/home/rapnic/birdology/apps/<item>
#
# The layout mirrors the old roots 1:1, so every path in the repo moved by a
# plain prefix swap. Four steps, each run by hand, in order:
#
#   move_to_home.sh plan      sizes, file counts, destination space, running
#                             jobs, and anything in the ML folders that is NOT
#                             on the move list (decide those by hand)
#   move_to_home.sh copy      rsync each item (resumable: re-run after an
#                             interruption, it picks up where it stopped)
#   move_to_home.sh verify    per-item file list + byte sizes, source vs
#                             destination, plus extra files in the destination;
#                             any difference is printed and fails.
#                             `verify --checksum` also compares content (slow).
#   move_to_home.sh delete --yes
#                             re-verifies, refuses while any of your Slurm jobs
#                             is queued or running, then removes the SOURCES.
#                             Without --yes it only prints what it would remove.
#
# Run it on a node that mounts BOTH /faic/weka and /srv/home, inside an
# allocation, not on the login node -- birdset alone is large:
#   srun --cpus-per-task=8 --mem=16G --time=24:00:00 --pty \
#       bash scripts/cluster/move_to_home.sh copy
#
# venvs/ is NOT copied: a venv bakes its absolute path into its scripts'
# shebangs, so a copied venv runs the wrong interpreter or none. Re-create it
# at the new path with scripts/cluster/hf_setup.sh after the copy.
set -euo pipefail

# Overridable only so the script can be rehearsed on scratch dirs.
OLD_DATA=${MOVE_OLD_DATA:-/faic/weka/ML}
OLD_APPS=${MOVE_OLD_APPS:-/shared/apps/ML}
BIRDOLOGY_ROOT=${MOVE_BIRDOLOGY_ROOT:-/srv/home/rapnic/birdology}
NEW_DATA=$BIRDOLOGY_ROOT/data
NEW_APPS=$BIRDOLOGY_ROOT/apps

# The move list: what the repo's scripts read or write. Anything else found in
# the ML folders is reported by `plan` and left alone.
DATA_ITEMS=(birdset phase_a phase_b phase_c prior)
APPS_ITEMS=(birdology containers models secrets logs priors)
SKIPPED_APPS_ITEMS=(venvs)  # re-created by hf_setup.sh, never copied

PARALLEL=${PARALLEL:-4}
LOG_DIR=$BIRDOLOGY_ROOT/move_logs

usage() { sed -n '2,/^set -euo/p' "$0" | sed '$d'; exit 2; }

# "src<TAB>dst" for every listed item whose source exists.
pairs() {
    local item
    for item in "${DATA_ITEMS[@]}"; do
        [ -e "$OLD_DATA/$item" ] && printf '%s\t%s\n' "$OLD_DATA/$item" "$NEW_DATA/$item"
    done
    for item in "${APPS_ITEMS[@]}"; do
        [ -e "$OLD_APPS/$item" ] && printf '%s\t%s\n' "$OLD_APPS/$item" "$NEW_APPS/$item"
    done
    return 0
}

my_jobs() { squeue -h -u "$USER" -o '%i %j %T' 2>/dev/null || true; }

require_mounts() {
    local d
    for d in "$OLD_DATA" "$OLD_APPS"; do
        [ -d "$d" ] || { echo "FATAL: $d is not visible on $(hostname -s)"; exit 2; }
    done
    [ -d "$(dirname "$BIRDOLOGY_ROOT")" ] || {
        echo "FATAL: $(dirname "$BIRDOLOGY_ROOT") is not visible on $(hostname -s)"; exit 2; }
}

cmd_plan() {
    require_mounts
    echo "== move list (source -> destination, size, files)"
    local src dst
    while IFS=$'\t' read -r src dst; do
        printf '%-40s -> %s\n    %s, %s files\n' "$src" "$dst" \
            "$(du -sh "$src" 2>/dev/null | cut -f1)" \
            "$(find "$src" -type f 2>/dev/null | wc -l | tr -d ' ')"
    done < <(pairs)
    local item
    for item in "${DATA_ITEMS[@]}"; do
        [ -e "$OLD_DATA/$item" ] || echo "    (absent, nothing to move: $OLD_DATA/$item)"
    done
    for item in "${APPS_ITEMS[@]}"; do
        [ -e "$OLD_APPS/$item" ] || echo "    (absent, nothing to move: $OLD_APPS/$item)"
    done

    echo; echo "== NOT on the move list -- decide by hand (may belong to others)"
    local root entry name listed
    for root in "$OLD_DATA" "$OLD_APPS"; do
        for entry in "$root"/* "$root"/.[!.]*; do
            [ -e "$entry" ] || continue
            name=$(basename "$entry"); listed=no
            for item in "${DATA_ITEMS[@]}" "${APPS_ITEMS[@]}" "${SKIPPED_APPS_ITEMS[@]}"; do
                [ "$name" = "$item" ] && listed=yes
            done
            [ "$listed" = no ] && echo "    $entry ($(du -sh "$entry" 2>/dev/null | cut -f1))"
        done
    done
    echo "    venvs/ is skipped on purpose: re-create with scripts/cluster/hf_setup.sh"

    echo; echo "== destination space"
    df -h "$(dirname "$BIRDOLOGY_ROOT")"
    command -v quota >/dev/null && { quota -s 2>/dev/null || true; }

    echo; echo "== your Slurm jobs (copy warns, delete refuses, while any exist)"
    my_jobs | sed 's/^/    /'
}

copy_one() {
    local src=$1 dst=$2 log
    log=$LOG_DIR/copy_$(basename "$(dirname "$dst")")_$(basename "$dst").log
    mkdir -p "$(dirname "$dst")"
    # Trailing slashes: copy the directory's CONTENTS into dst, not dst/<name>.
    if [ -d "$src" ]; then
        rsync -aH --partial --stats "$src/" "$dst/" >"$log" 2>&1
    else
        rsync -aH --partial --stats "$src" "$dst" >"$log" 2>&1
    fi
    echo "copied $src -> $dst (log: $log)"
}
export -f copy_one
export LOG_DIR

cmd_copy() {
    require_mounts
    mkdir -p "$NEW_DATA" "$NEW_APPS" "$LOG_DIR"
    if [ -n "$(my_jobs)" ]; then
        echo "WARNING: you have Slurm jobs queued/running; files they write after"
        echo "their rsync pass finishes will be caught by re-running copy before verify:"
        my_jobs | sed 's/^/    /'
    fi
    pairs | while IFS=$'\t' read -r src dst; do printf '%s\0%s\0' "$src" "$dst"; done \
        | xargs -0 -n 2 -P "$PARALLEL" bash -c 'copy_one "$1" "$2"' _
    [ -d "$NEW_APPS/secrets" ] && chmod -R go-rwx "$NEW_APPS/secrets"
    echo "copy done. Next: $0 verify"
}

# Dry-run rsync, itemised, with --delete so EXTRA destination files show up
# too. Any line other than a directory-attribute-only change (".d...") is a
# difference. Sizes + mtimes by default; content checksums with --checksum.
# Permissions and owners are NOT compared (-rlHt, not -a): copy tightens
# secrets/ on purpose, and a home filesystem may map owners differently.
differences() {
    local src=$1 dst=$2 flags=$3
    [ -d "$src" ] && { src=$src/; dst=$dst/; }
    rsync -rlHtn --delete --itemize-changes $flags "$src" "$dst" 2>&1 | grep -v '^\.d' || true
}

cmd_verify() {
    require_mounts
    local flags=""
    [ "${1:-}" = "--checksum" ] && flags="-c"
    local src dst bad=0 out
    while IFS=$'\t' read -r src dst; do
        if [ ! -e "$dst" ]; then
            echo "MISSING  $dst"; bad=1; continue
        fi
        out=$(differences "$src" "$dst" "$flags")
        if [ -n "$out" ]; then
            echo "DIFFERS  $src vs $dst (first differences):"
            echo "$out" | head -20 | sed 's/^/    /'
            bad=1; continue
        fi
        echo "OK       $src -> $dst ($(find "$src" -type f | wc -l | tr -d ' ') files)"
    done < <(pairs)
    [ "$bad" = 0 ] || { echo "VERIFY FAILED -- re-run copy, then verify"; return 1; }
    echo "VERIFY OK"
}

cmd_delete() {
    local yes=${1:-}
    require_mounts
    if [ -n "$(my_jobs)" ]; then
        echo "REFUSED: you have Slurm jobs queued/running; they may still read the old paths:"
        my_jobs | sed 's/^/    /'
        exit 1
    fi
    cmd_verify || { echo "REFUSED: verify failed, nothing deleted"; exit 1; }
    local src dst
    if [ "$yes" != "--yes" ]; then
        echo; echo "Would remove (re-run with --yes to do it):"
        pairs | cut -f1 | sed 's/^/    /'
        [ -d "$OLD_APPS/venvs" ] && echo "    (venvs/ is left alone -- remove $OLD_APPS/venvs by hand once hf_setup.sh has rebuilt it)"
        return 0
    fi
    while IFS=$'\t' read -r src dst; do
        echo "removing $src"
        rm -rf -- "$src"
    done < <(pairs)
    echo "delete done."
}

case "${1:-}" in
    plan) cmd_plan ;;
    copy) cmd_copy ;;
    verify) shift; cmd_verify "${1:-}" ;;
    delete) shift; cmd_delete "${1:-}" ;;
    *) usage ;;
esac
