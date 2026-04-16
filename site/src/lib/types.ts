/**
 * Data contract TypeScript interfaces for muni-walk-access.
 * Field names mirror the Python Pydantic models exactly (snake_case).
 * All properties are readonly — this data is build-time-imported, never mutated.
 */

export interface GridAxes {
  readonly frequency_minutes: readonly number[];
  readonly walking_minutes: readonly number[];
}

export interface GridDefaults {
  readonly frequency_idx: number;
  readonly walking_idx: number;
}

export interface LensFlags {
  readonly analysis_neighborhoods: boolean;
  readonly ej_communities: boolean;
  readonly equity_strategy: boolean;
}

export interface CityWide {
  readonly pct_within: readonly (readonly number[])[];
}

export interface NeighborhoodGrid {
  readonly id: string;
  readonly name: string;
  readonly population: number;
  readonly lens_flags: LensFlags;
  readonly pct_within: readonly (readonly number[])[];
}

export interface GridSchema {
  readonly version: string;
  readonly run_id: string;
  readonly config_snapshot_url: string;
  readonly axes: GridAxes;
  readonly defaults: GridDefaults;
  readonly city_wide: CityWide;
  readonly neighborhoods: readonly NeighborhoodGrid[];
}

export interface CodeVersion {
  readonly git_sha: string;
  readonly git_tag: string;
}

export interface DataVersions {
  readonly gtfs_feed_sha256: string;
  readonly osm_extract_date: string;
  readonly datasf_timestamps: Readonly<Record<string, string>>;
}

export interface ConfigSnapshot {
  readonly run_id: string;
  readonly code_version: CodeVersion;
  readonly config_hash: string;
  readonly data_versions: DataVersions;
  readonly config_values: Readonly<Record<string, unknown>>;
  readonly upstream_fallback: boolean;
}

export interface GroundTruth {
  readonly sample_size: number;
  readonly within_10pct: number;
  readonly within_20pct: number;
  readonly median_error_pct: number;
  readonly worst_case_pct: number;
}

export interface ComparisonTool {
  readonly name: string;
  readonly pct_agreement: number;
}

export interface ValidationResults {
  readonly run_id: string;
  readonly ground_truth: GroundTruth;
  readonly comparison_tool: ComparisonTool | null;
}

export interface NeighborhoodFeatureProperties {
  readonly id: string;
  readonly name: string;
  readonly population: number;
  readonly lens_flags: LensFlags;
  readonly pct_at_defaults: number;
}

export interface HexCell {
  readonly id: string;
  readonly center_lat: number;
  readonly center_lon: number;
  readonly population: number;
  readonly pct_within: readonly (readonly number[])[];
}

export interface HexGridSchema {
  readonly version: string;
  readonly h3_resolution: number;
  readonly run_id: string;
  readonly config_snapshot_url: string;
  readonly time_window?: string;
  readonly axes: GridAxes;
  readonly defaults: GridDefaults;
  readonly cells: readonly HexCell[];
}

export const TIME_WINDOWS = [
  { key: "am_peak", label: "AM", long: "AM Rush", range: "6–9a" },
  { key: "midday", label: "Midday", long: "Midday", range: "9a–3p" },
  { key: "pm_peak", label: "PM", long: "PM Rush", range: "3–7p" },
  { key: "evening", label: "Eve", long: "Evening", range: "7p–12a" },
  { key: "overnight", label: "Night", long: "Overnight", range: "12–6a" },
] as const;

export type TimeWindowKey = (typeof TIME_WINDOWS)[number]["key"];

export type RouteMode = "aggregate" | "headway";
