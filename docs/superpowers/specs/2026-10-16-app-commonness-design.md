# Commonness chip and rare-bird warning in Fugle DK

Date: 2026-10-16. Repos: birdology (export) and fugle-dk (app, branch `danish`).

## Purpose

Fugle DK should tell the user, on every detection, how common that bird is
where they stand at this time of year, and warn when it would be rare. The
signal comes from the birdology DOF occurrence prior, so the prior work is used
by the app. Two uses, both wanted:

- a quiet commonness chip on every detection;
- a visible warning on rare ones: unusual here and now, so double-check the ID,
  and worth reporting to DOFbasen if it holds up.

Success: a blackbird in Copenhagen in May reads "Common" or "Abundant"; a swift
in January, or a species that needs documentation by the Danish rarities
committee (SU), reads "Rare" with the warning; outside Denmark the app behaves
as it does today.

## Decisions taken

| decision | choice |
|---|---|
| what the indicator does | chip on every detection + warning on the rare tier |
| data inside Denmark | DOF prior, H3 resolution 5 (`data/prior/dof/prior_dof_r5_full`, GBIF download 10.15468/dl.3j4czw, CC BY 4.0) |
| where tiers are computed | offline in birdology, shipped as a precomputed asset (approach A) |
| outside Denmark / species not in the prior | the app's existing geo-model commonness (`geoCommonnessProvider`) |

Rejected: computing tiers in Dart from shipped probabilities (calibration would
live twice, the Dart copy untested against data); feeding the DOF prior into the
app's rank-based `ExploreTierScale` (it labels a fixed share of species "rare"
at every place and week, and its absolute floors are tuned to geo-model scores,
not to shares of records).

## 1. Export (birdology)

Code: `src/birdology/prior/app_tiers.py` (logic) and `scripts/prior_app_export.py`
(CLI). Runs on the fleet, where the prior lives; the asset is copied into the
fork.

### Inputs

- The DOF r5 prior: 532 GBIF species keys x 515 cells x 52 weeks, every context
  stored, float64. A value is the smoothed share of the cell-week's records that
  are that species (floor 1e-06).
- GBIF species key -> scientific name, read from the DOF archive's `speciesKey`
  and `species` columns.
- The app's audio labels (`BirdNET+_V3.0-preview3.1_Global_10K-pruned_Labels.csv`,
  `sci_name` column). Only species the classifier can name are exported.

Names match exactly, or through the synonym groups of
`birdology.phase_d.taxonomy.SYNONYMS` (two names in one group are the same
species, e.g. Accipiter gentilis / Astur gentilis). Unmatched prior species are
listed in the export log; unmatched app species simply get no entry.

### Tiers

Six tiers, the app's existing vocabulary, stored as the `CommonnessBin` index:
`rare=0, scarce=1, uncommon=2, frequent=3, common=4, abundant=5`; `255` = no
value. A species' tier in a cell-week is set by absolute thresholds on its share
of records `p`:

| tier | starting threshold on p |
|---|---|
| abundant | p >= 1e-2 |
| common | 3e-3 <= p < 1e-2 |
| frequent | 1e-3 <= p < 3e-3 |
| uncommon | 3e-4 <= p < 1e-3 |
| scarce | 5e-5 <= p < 3e-4 |
| rare | p < 5e-5 |

These are starting values. The export calibrates them against three checks and
records the final thresholds and check results in the sidecar; it exits
non-zero if a check fails:

1. SU species: the species on DOF's published SU list (saved in the repo as
   `src/birdology/prior/data/su_species.csv` with its URL and download date) are rare in at
   least 95% of their cell-weeks.
2. Everyday breeders: Turdus merula, Parus major, Fringilla coelebs are common
   or abundant in at least 90% of cell-weeks in weeks 14-30, over all
   515 cells.
3. Seasonality: Apus apus is rare in at least 90% of cell-weeks in weeks 1-10
   and 45-52.

Calibration only moves the thresholds; it never edits individual values.

### Asset

Three files in `assets/prior/` of the fork:

- `dk_tiers.json`, the sidecar: `version` (1), `source` ("dof"), `resolution`
  (5), `weeks` (52), `species` (scientific names in column order), `cells` (H3
  strings in block order), `tier_names`, `thresholds`, `calibration` (check
  results), `gbif_doi`, `licence`, `attribution`, `export_date`, and the
  layouts of the two binary files below. The export refuses an empty DOI.
- `dk_tiers.bin`: one zlib stream per cell, concatenated; `offsets` and
  `lengths` in the sidecar. Each stream inflates to `52 x len(species)` bytes,
  row-major `[week][species]`, week 1 first.
- `dk_raster.bin`: uint16 little-endian, `nlat x nlon` row-major starting at the
  south-west corner, step 0.02 degrees over Denmark's bounding box (`lat0`,
  `lon0`, `step`, `nlat`, `nlon` in the sidecar). Each value is the index of the
  r5 cell containing the pixel centre, computed with `h3.latlng_to_cell`, or
  `0xFFFF` for a pixel outside every prior cell.

Expected size: about 10 MB of tiers before compression, 1-2 MB after; about
120 KB of raster.

### Amendment (2026-10-16, after the first export run)

The first export failed the seasonality check: Apus apus was rare in only 22.7%
of winter cell-weeks (need 90%). The DOF prior is smoothed for classification:
in sparse winter cell-weeks its backoff pulls each species toward the cell's
year-round share, so the swift's national winter value is a flat 0.17% (Hirundo
rustica 0.12%, Phylloscopus trochilus 0.06%). The user chose to compute
commonness from the raw DOF counts instead:

- Input: the guarded DOF counts at r5 (`species_key, cell, week, n`), built from
  `data/prior/parquet/dof` by `scripts/prior_occurrence_counts.py --no-test-split`
  (Xeno-canto rows removed).
- p for (cell, week, species) = the species' share of ALL records in that cell,
  pooled over weeks w-2..w+2 (circular). When the pooled cell total is under 500
  records, p is the species' national share for the same pooled weeks.
- The tiers, thresholds, three checks and asset layout are unchanged. The
  sidecar's `calibration` records `window` and `min_records`; provenance is the
  DOF download 10.15468/dl.3j4czw, CC BY 4.0.

The first run also left 29 prior species unmatched, mostly genus moves and
spellings (Sylvia communis -> Curruca communis, Phylloscopus sibillatrix ->
sibilatrix, Porzana parva -> Zapornia parva). Name matching gains an explicit
alias table (`APP_ALIASES`) for these; only aliases present in the app's labels
are used.

Export result (2026-10-16, raw counts, birdology b064936): 532 species in the
counts, 526 matched to the app, 6 unmatched (Acanthis hornemanni, Anas
carolinensis, Charadrius asiaticus, Melanitta stejnegeri, Pelecanus crispus,
Pelecanus rufescens). Checks: su_rare 0.972 (need 0.95), breeders_common 0.958
(need 0.9), swift_winter_rare 0.995 (need 0.9); thresholds [5e-05, 0.0003,
0.001, 0.003, 0.01] at factor 1.000. 515 cells; dk_tiers.bin 1,977,184 bytes,
dk_raster.bin 209,066 bytes, dk_tiers.json 34,715 bytes.

## 2. Lookup (fugle-dk)

- `lib/features/inference/dk_commonness.dart`: `DkCommonness`, loading the
  sidecar and raster lazily from the bundle and inflating only the current
  cell's block (about 20 KB). `tierFor(scientificName, lat, lon, date)` returns
  an `ExploreTier`, or null outside the raster, for an unknown species or a 255
  value.
- Week: ISO week of the date, week 53 folded into 52, as in the prior. This is
  not the geo model's 48-week scheme; the code says so at the conversion.
- `detectionCommonnessProvider`: scientific name -> `{tier, source}` for the
  current location (`currentLocationProvider`) and week. Inside Denmark the
  source is `dof`; otherwise, and for species the prior lacks, the entry comes
  from `geoCommonnessProvider` with source `geo`. Recomputed when the cell or
  the week changes.
- Spoken announcements read `detectionCommonnessProvider` instead of
  `geoCommonnessProvider` (one change in the announcements alert sink), so the
  voice and the chip never disagree. The out-of-season flag stays geo-derived.
- Failure: a missing or unreadable asset is logged once and the provider falls
  back to the geo model. A detection is never blocked by the prior.

## 3. UI (fugle-dk)

- `DetectionTile` (`lib/features/live/widgets/detection_list_widget.dart`) gets
  a tier chip on the name row, beside the existing count chip: a coloured dot
  and the localized tier name, using `exploreTierColor` and `exploreTierLabel`.
  `DetectionTile` serves the Live, Survey, Point count and ARU screens, so all
  four get it.
- Rare tier only: a warning icon and one line under the name. Confidence below
  0.8: "Rare here this week. Double-check the ID." Confidence 0.8 or higher:
  "Rare here this week. Worth reporting to DOFbasen if confirmed." Scarce gets
  the chip only.
- Tapping the chip opens a sheet naming the source: "Based on DOFbasen records
  for this area and time of year (GBIF 10.15468/dl.3j4czw, CC BY 4.0)" or
  "Based on BirdNET's range model" outside Denmark.
- Setting "Show how common birds are", on by default, next to the species
  filter setting. Off hides chip and warning.
- Screen readers get "Common here this week" / "Rare here this week. Double-check
  the ID." rather than a colour.
- Strings are added in English (`app_en.arb`); other locales fall back to
  English. The About credits gain one line citing the GBIF download and licence.

## 4. Testing and delivery

- birdology, pytest on a synthetic prior: threshold boundaries, name matching
  with synonyms, the raster against `h3.latlng_to_cell`, the per-cell zlib round
  trip, sidecar fields, refusal of an empty DOI.
- Calibration checks on the real DOF prior inside the export (section 1).
- A shared fixture: the exporter writes a small asset from the synthetic prior
  into the fork's `test/fixtures/dk_tiers/`, and the Dart tests read that same
  asset, so a layout change on either side fails a test.
- fugle-dk unit tests: fixture decoding, ISO week (1 January edge cases, week 53
  -> 52), a point outside the raster -> null, provider fallback outside Denmark
  and with no asset.
- fugle-dk widget tests: chip label per tier, warning wording by confidence,
  setting off hides both.
- On the user's iPhone 15 Pro: an integration test with the location set to
  Copenhagen and known detections fed in, a manual look at the live screen, then
  a reinstall.

## Out of scope

- A Danish translation of the app (it has no Danish locale today).
- File analysis results and the session review screen, which do not render
  through `DetectionTile` (follow-up).
- eBird data, and changing the Explore screen's tiers (they keep the geo model).
