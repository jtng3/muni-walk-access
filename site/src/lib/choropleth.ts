/** 7-stop viridis palette for the neighborhood choropleth.
 * Index 0 = best-served (yellow-green), index 6 = worst-served (deep purple).
 * Colorblind-safe by design (perceptually uniform). */
export const VIRIDIS_STOPS = [
  "#FDE725", // stop 0 — best-served (light yellow-green)
  "#A0DA39", // stop 1
  "#4AC16D", // stop 2
  "#1FA187", // stop 3 — teal-green
  "#277F8E", // stop 4
  "#365C8D", // stop 5
  "#46327E", // stop 6 — worst-served (deep purple)
] as const;

export type ViridisStop = (typeof VIRIDIS_STOPS)[number];
