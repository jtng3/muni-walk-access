# Residential Filter Spike — DataSF Parcel Dataset Selection

**Status: 🚫 BLOCKED — awaiting Luke Armbruster (SF MUNI) confirmation**
**Date:** 2026-04-13
**Author:** Dev agent (Story 1.6)
**For:** Luke Armbruster, SF MUNI

---

## Context

FR5 requires filtering the ~220k EAS addresses to residential properties only.
The pipeline config has `residential_filter.parcel_dataset_id: "TBD_FROM_LUKE"` as a
placeholder pending your confirmation of the correct DataSF parcel dataset.

This spike investigates 3 candidate datasets, implements an interim filter using the
best default, and asks a few targeted questions before we lock in the dataset.

---

## Candidate Datasets

| # | Dataset | DataSF ID | Join Key (to EAS) | Use-Code Field | Residential Codes | Row Count | Freshness |
|---|---------|-----------|-------------------|----------------|-------------------|-----------|-----------|
| 1 | Assessor Historical Secured Property Tax Rolls | `wv5m-vpq2` | `parcel_number` | `use_code` | `SRES`, `MRES` | ~3.7M (multi-year) | Annual (2024 current) |
| 2 | Map of San Francisco Land Use 2023 | `k8rg-ihdq` (table: `fdfd-xptc`) | `mapblklot` | `landuse` | `RESIDENT`, `MIXRES` | ~125k | Static 2023 snapshot |
| 3 | Parcels – Active and Retired | `acdm-wktn` | `blklot` | `zoning_code` | (indirect — Planning zoning, not property use) | ~236k | Daily |

### Candidate 1: Assessor Historical Tax Rolls (`wv5m-vpq2`) ⭐ Recommended

**Pros:**
- Most authoritative residential classification — straight from the Assessor's Office
- `parcel_number` join key matches EAS directly (verified via SODA API metadata — both datasets expose `parcel_number` as text)
- Only 2 distinct residential codes in 2024 data: `SRES` (Single Family) and `MRES` (Multi-Family)
- Rich schema: bedrooms, bathrooms, units, year built — useful for future stories

**Cons:**
- Large dataset: ~3.7M rows across all years; must filter to `closed_roll_year = 2024` (most recent)
- First fetch is slow (~74 paginated SODA calls at 50k rows/page); cached after first run
- Annual update cycle — fine for this use case, not for real-time

**Verified use codes** (via SODA group query, 2026-04-13):
```
SRES = "Single Family Residential"
MRES = "Multi-Family Residential"
COMH = "Commercial Hotel"
COMM = "Commercial Misc"
COMO = "Commercial Office"
COMR = "Commercial Retail"
GOVT = "Government"
IND  = "Industrial"
MISC = "Miscellaneous/Mixed-Use"
```

**⚠️ Config code mismatch found:** `config.yaml` previously had `use_codes_residential: ["SFR", "MFR", "CONDO", "RESIDENTIAL"]` — none of these match wv5m-vpq2. Updated to `["SRES", "MRES"]` as part of this spike. See Change Log.

**⚠️ Condominiums:** No separate `CONDO` code found in this dataset (2024 roll). Condos are likely classified under `SRES`. Confirm with Luke.

### Candidate 2: Land Use 2023 (`k8rg-ihdq` / `fdfd-xptc`)

**Pros:**
- Smaller dataset (~125k rows), faster fetch
- Clean, purpose-built land-use classification
- `MIXRES` code captures mixed-use buildings with residential floors

**Cons:**
- Join key `mapblklot` may not match EAS `parcel_number` format without normalization
- Static 2023 snapshot — not updated
- Requires joining through the geospatial table (underlying table ID `fdfd-xptc` needed for SODA CSV access)

### Candidate 3: Parcels Active and Retired (`acdm-wktn`)

**Pros:**
- Daily updates, most current data

**Cons:**
- `zoning_code` is Planning Department zoning (e.g., "RH-1", "RM-2") — not the same as property use/assessment classification
- Residential filtering requires knowing zoning codes, which are less directly correlated with actual use
- `blklot` format may differ from EAS `parcel_number`

---

## Recommended Interim Default

**Dataset ID:** `wv5m-vpq2` (Assessor Historical Secured Property Tax Rolls)

**Rationale:** Shares the `parcel_number` join key with EAS (no format normalization needed), has a direct `use_code` field with verified residential codes (`SRES`, `MRES`), and is the most authoritative source for property classification.

**Limitation to flag:** First pipeline run fetches 3.7M rows (cached thereafter). If Luke prefers a lighter dataset, Candidate 2 is viable with a join-key normalization step.

---

## ⚠️ Known Implementation Risk: `parcel_number` Format

Both EAS (`3mea-di5p`) and Tax Rolls (`wv5m-vpq2`) expose a `parcel_number` text field. However, the format encoding (e.g., `"3745/025"` vs `"3745025"`) has not been verified across datasets. The spot-check (AC-3) will reveal whether the join produces results. If the spot-check YAML is empty, the join key formats differ and normalization is needed.

---

## Questions for Luke

1. **Is `wv5m-vpq2` (Assessor Tax Rolls) the dataset you'd use for residential classification?**
   If not, which DataSF parcel dataset should we use?

2. **Are SF condominiums classified as `SRES` in the assessor data, or is there a separate code?**
   We found no `CONDO` code in the 2024 roll. Using `["SRES", "MRES"]` — is that correct?

3. **Is the `parcel_number` field format the same between EAS and the Tax Rolls?**
   If not, do you know the normalization needed to make the join work?

4. **Annual freshness OK?** The 2024 Assessor roll is current. Annual update is fine for this project's scope?

---

## Action Required

Once Luke answers:

1. Update `config.yaml::residential_filter.parcel_dataset_id` from `"TBD_FROM_LUKE"` to the confirmed dataset ID
2. Update `use_codes_residential` if needed (currently `["SRES", "MRES"]`)
3. The WARNING log in `fetch_residential_addresses()` will disappear automatically
4. Re-run the spot-check script (`uv run python tests/generate_residential_spot_check.py`) to verify ≥95% accuracy

---

## Implementation Notes (for codebase maintainers)

- Interim dataset constant: `pipeline/src/muni_walk_access/ingest/datasf.py::_INTERIM_PARCEL_DATASET_ID = "wv5m-vpq2"`
- Sentinel constant: `_TBD_SENTINEL = "TBD_FROM_LUKE"`
- EAS dataset ID: `_EAS_DATASET_ID = "3mea-di5p"`
- Join key: `parcel_number` (inner join — EAS addresses without a parcel record are excluded)
- Year filter: automatic when `closed_roll_year` column present (keeps `max()` year)
- Spot-check generation: `pipeline/tests/generate_residential_spot_check.py`
