#!/bin/bash
#SBATCH --job-name=gbif-fetch
#SBATCH --partition=gpu
#SBATCH --time=24:00:00
#SBATCH --mem=16G
#SBATCH --cpus-per-task=8
#SBATCH --exclude=dgx177
#SBATCH --output=/srv/home/rapnic/birdology/data/prior/logs/gbif_fetch_%j.out
# Usage: sbatch gbif_fetch.sbatch <download_key> <name>
set -uo pipefail
KEY=$1; NAME=$2
BIRDOLOGY_ROOT=/srv/home/rapnic/birdology
# Fail loudly if this node cannot see the birdology home: an unmounted
# /srv/home leaves a missing or empty dir here -- 0 files, exit 0.
[ -d "$BIRDOLOGY_ROOT/data/phase_a" ] || { echo "FATAL: $BIRDOLOGY_ROOT/data/phase_a is not visible on $(hostname -s)"; exit 2; }
mkdir -p /srv/home/rapnic/birdology/data/prior/logs /srv/home/rapnic/birdology/data/prior/archives
DEST=/srv/home/rapnic/birdology/data/prior/archives/${NAME}.zip
URL="https://api.gbif.org/v1/occurrence/download/request/${KEY}.zip"
echo "host=$(hostname -s) key=$KEY name=$NAME $(date -Is)"
curl -fSL --retry 5 --retry-delay 30 -o "${DEST}.part" "$URL" || { echo CURL_FAILED; exit 3; }
mv "${DEST}.part" "$DEST"
ls -la "$DEST"
python3 -c "import zipfile; z=zipfile.ZipFile(\"$DEST\"); print(\"zip ok:\", z.namelist()[:3])"
echo "FETCH_OK $NAME $(date -Is)"
