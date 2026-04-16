import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import { Map as MapGL, Source, Layer } from "react-map-gl/maplibre";
import { Protocol } from "pmtiles";
import { layers as pmLayers, LIGHT, DARK } from "@protomaps/basemaps";
import { VIRIDIS_STOPS } from "@/lib/choropleth";
import type { GridSchema, HexGridSchema } from "@/lib/types";
import type {
  FillLayerSpecification,
  LineLayerSpecification,
  StyleSpecification,
  SymbolLayerSpecification,
  GeoJSONSourceSpecification,
} from "maplibre-gl";
import type { DevFlags } from "./DevOverlay";

// Self-hosted PMTiles extract (23 MB, zoom 0–14, SF + bay + Oakland waterfront).
// Regenerate with:
//   pmtiles extract https://build.protomaps.com/20251201.pmtiles \
//     site/public/tiles/sf.pmtiles --bbox="-122.55,37.69,-122.20,37.85"
const TILE_URL = "pmtiles:///tiles/sf.pmtiles";

// AWS Open Data terrain tiles — free, no API key, CORS-enabled.
const TERRAIN_URL =
  "https://s3.amazonaws.com/elevation-tiles-prod/terrarium/{z}/{x}/{y}.png";

function buildMapStyle(isDark: boolean, flags: DevFlags): StyleSpecification {
  const baseLayers = pmLayers("protomaps", isDark ? DARK : LIGHT).filter((l) =>
    flags.buildings3d ? l.id !== "buildings" : true,
  );

  const extraLayers: any[] = [];

  if (flags.buildings3d) {
    extraLayers.push({
      id: "buildings-3d",
      type: "fill-extrusion",
      source: "protomaps",
      "source-layer": "buildings",
      filter: ["in", "kind", "building", "building_part"],
      minzoom: 13,
      paint: {
        "fill-extrusion-color": isDark ? "#1e2430" : "#d9d5cf",
        "fill-extrusion-height": ["coalesce", ["get", "height"], 8],
        "fill-extrusion-base": ["coalesce", ["get", "min_height"], 0],
        "fill-extrusion-opacity": isDark ? 0.7 : 0.5,
        "fill-extrusion-vertical-gradient": true, // lighter at top, darker at base
      },
    });
  }

  const style: any = {
    version: 8,
    sources: {
      protomaps: { type: "vector", url: TILE_URL },
      ...(flags.terrain && {
        terrain: {
          type: "raster-dem",
          tiles: [TERRAIN_URL],
          tileSize: 256,
          encoding: "terrarium",
        },
      }),
    },
    layers: [...baseLayers, ...extraLayers],
  };

  if (flags.terrain) {
    style.terrain = { source: "terrain", exaggeration: 1.5 };
  }

  // Directional light — boost intensity to counteract terrain shadows
  style.light = {
    anchor: "map",
    color: isDark ? "#d0d8f0" : "#ffffff",
    intensity: isDark ? 0.5 : 0.6,
    position: [1.2, 210, 40],
  };

  if (flags.fog) {
    style.fog = {
      range: [0.3, 6],
      color: isDark ? "#0a0c10" : "#e8e5e0",
      "horizon-blend": 0.15,
    };
  }

  return style as StyleSpecification;
}

// Viridis interpolation expression — reused across fill and line layers
const VIRIDIS_COLOR_EXPR = [
  "interpolate",
  ["linear"],
  ["get", "pct_at_defaults"],
  0,
  VIRIDIS_STOPS[6], // worst-served — deep purple
  0.167,
  VIRIDIS_STOPS[5],
  0.333,
  VIRIDIS_STOPS[4],
  0.5,
  VIRIDIS_STOPS[3],
  0.667,
  VIRIDIS_STOPS[2],
  0.833,
  VIRIDIS_STOPS[1],
  1.0,
  VIRIDIS_STOPS[0], // best-served — yellow-green
] as any; // eslint-disable-line @typescript-eslint/no-explicit-any -- MapLibre expression type

function buildChoroplethFill(
  isDark: boolean,
  buildingGlow?: boolean,
  viewMode: "summary" | "detailed" = "summary",
): FillLayerSpecification {
  // Detailed mode: hex carries the color, neighbourhood fill hidden entirely
  if (viewMode === "detailed") {
    return {
      id: "neighborhoods-fill",
      type: "fill",
      source: "neighborhoods",
      layout: { visibility: "none" },
      paint: { "fill-color": VIRIDIS_COLOR_EXPR, "fill-opacity": 0 },
    };
  }
  // Summary mode: traditional choropleth for board presentations
  const opacity = buildingGlow ? (isDark ? 0.35 : 0.5) : isDark ? 0.55 : 0.65;
  return {
    id: "neighborhoods-fill",
    type: "fill",
    source: "neighborhoods",
    paint: {
      "fill-color": VIRIDIS_COLOR_EXPR,
      "fill-opacity": opacity,
    },
  };
}

// Glow border — wide, blurred, same viridis color. Visible in both modes, stronger in dark.
function buildGlowBorder(isDark: boolean): LineLayerSpecification {
  return {
    id: "neighborhoods-glow",
    type: "line",
    source: "neighborhoods",
    paint: {
      "line-color": VIRIDIS_COLOR_EXPR,
      "line-width": isDark ? 8 : 5,
      "line-opacity": isDark ? 0.35 : 0.15,
      "line-blur": isDark ? 4 : 2,
    },
  };
}

// Core border line
function buildChoroplethLine(isDark: boolean): LineLayerSpecification {
  return {
    id: "neighborhoods-line",
    type: "line",
    source: "neighborhoods",
    paint: {
      "line-color": isDark ? (VIRIDIS_COLOR_EXPR as any) : "#334155",
      "line-width": isDark ? 1.5 : 1,
      "line-opacity": isDark ? 0.7 : 0.6,
    },
  };
}

// Neighbourhood name + percentage labels at polygon centroids.
// text-allow-overlap + text-ignore-placement: all 41 labels must be visible.
function buildLabelsLayer(isDark: boolean): SymbolLayerSpecification {
  return {
    id: "neighborhood-labels",
    type: "symbol",
    source: "neighborhoods",
    layout: {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any -- MapLibre expression type
      "text-field": [
        "concat",
        ["get", "name"],
        "\n",
        ["to-string", ["round", ["*", 100, ["get", "pct_at_defaults"]]]],
        "%",
      ] as any,
      "text-size": 12,
      "text-allow-overlap": true,
      "text-ignore-placement": true,
    },
    paint: {
      "text-color": isDark ? "#e2e8f0" : "#1e293b",
      "text-halo-color": isDark ? "rgba(0,0,0,0.7)" : "white",
      "text-halo-width": 2,
    },
  };
}

interface MapInnerProps {
  data: GridSchema;
  freqIdx: number;
  walkIdx: number;
  isDark: boolean;
  devFlags: DevFlags;
  onError?: () => void;
  viewMode: "summary" | "detailed";
  hexBaseFC: GeoJSON.FeatureCollection | null;
  hexData: HexGridSchema | null;
}

export default function MapInner({
  data,
  freqIdx,
  walkIdx,
  isDark,
  devFlags,
  onError,
  viewMode,
  hexBaseFC,
  hexData,
}: MapInnerProps) {
  const loadedRef = useRef(false);

  const mapStyle = useMemo(
    () => buildMapStyle(isDark, devFlags),
    [isDark, devFlags],
  );
  const fillSpec = useMemo(
    () => buildChoroplethFill(isDark, devFlags.buildingGlow, viewMode),
    [isDark, devFlags.buildingGlow, viewMode],
  );
  const glowSpec = useMemo(() => buildGlowBorder(isDark), [isDark]);
  const lineSpec = useMemo(() => buildChoroplethLine(isDark), [isDark]);
  const labelsSpec = useMemo(() => buildLabelsLayer(isDark), [isDark]);

  const [baseGeoJSON, setBaseGeoJSON] =
    useState<GeoJSON.FeatureCollection | null>(null);

  // Build id→index map once for O(1) neighborhood lookups
  const idxById = useMemo(
    () => new Map(data.neighborhoods.map((n, i) => [n.id, i])),
    [data.neighborhoods],
  );

  // Fetch GeoJSON once on mount, cache the geometry
  useEffect(() => {
    let cancelled = false;
    fetch("/data/neighborhoods.geojson")
      .then((r) => {
        if (!r.ok) throw new Error(`GeoJSON fetch failed: ${r.status}`);
        return r.json();
      })
      .then((fc: GeoJSON.FeatureCollection) => {
        if (!cancelled) setBaseGeoJSON(fc);
      })
      .catch((err) => {
        console.error("Failed to fetch GeoJSON:", err);
        if (!cancelled) onError?.();
      });
    return () => {
      cancelled = true;
    };
  }, []); // eslint-disable-line react-hooks/exhaustive-deps -- onError is stable via useCallback in parent

  // rAF-throttled indices — only apply the latest values on each animation frame
  const latestRef = useRef({ freqIdx, walkIdx });
  latestRef.current = { freqIdx, walkIdx };
  const [rendered, setRendered] = useState({ freqIdx, walkIdx });
  const rafRef = useRef(0);

  useEffect(() => {
    rafRef.current = requestAnimationFrame(() => {
      setRendered({ ...latestRef.current });
    });
    return () => cancelAnimationFrame(rafRef.current);
  }, [freqIdx, walkIdx]);

  // Neighbourhood choropleth — recolour pct_at_defaults on slider drag (rAF-throttled)
  const sourceData = useMemo<
    GeoJSON.FeatureCollection | GeoJSONSourceSpecification["data"]
  >(() => {
    if (!baseGeoJSON) return "/data/neighborhoods.geojson";
    return {
      ...baseGeoJSON,
      features: baseGeoJSON.features.map((f) => {
        const idx = idxById.get(f.properties?.id);
        if (idx === undefined) return f;
        return {
          ...f,
          properties: {
            ...f.properties,
            pct_at_defaults:
              data.neighborhoods[idx].pct_within[rendered.freqIdx][
                rendered.walkIdx
              ],
          },
        };
      }),
    };
  }, [
    baseGeoJSON,
    rendered.freqIdx,
    rendered.walkIdx,
    data.neighborhoods,
    idxById,
  ]);

  // Hex fill — recolour pct_at_defaults on slider drag (rAF-throttled, same path as above)
  // feature[i] corresponds to hexData.cells[i] — guaranteed by InteractiveView build order.
  const hexSourceData = useMemo<GeoJSON.FeatureCollection | null>(() => {
    if (viewMode !== "detailed" || !hexBaseFC || !hexData) return null;
    return {
      ...hexBaseFC,
      features: hexBaseFC.features.map((f, i) => {
        const cell = hexData.cells[i];
        if (!cell) return f;
        return {
          ...f,
          properties: {
            ...f.properties,
            pct_at_defaults:
              cell.pct_within[rendered.freqIdx][rendered.walkIdx],
          },
        };
      }),
    };
  }, [viewMode, hexBaseFC, hexData, rendered.freqIdx, rendered.walkIdx]);

  useEffect(() => {
    const protocol = new Protocol();
    maplibregl.addProtocol("pmtiles", protocol.tile);
    return () => {
      maplibregl.removeProtocol("pmtiles");
    };
  }, []);

  const handleLoad = useCallback(() => {
    loadedRef.current = true;
  }, []);

  const handleError = useCallback(
    (e: { error: Error }) => {
      console.error("MapLibre error:", e.error);
      if (!loadedRef.current) {
        onError?.();
      }
    },
    [onError],
  );

  return (
    <MapGL
      onLoad={handleLoad}
      onError={handleError}
      initialViewState={{
        bounds: [
          [-122.54, 37.695],
          [-122.3, 37.845],
        ],
        fitBoundsOptions: { padding: 40 },
      }}
      mapStyle={mapStyle}
      style={{ width: "100%", height: "100%" }}
    >
      {/* Hex fill — bottom layer; only rendered when data is ready and in Detailed mode.
          beforeId ensures it sits below the neighbourhood fill across sources. */}
      {hexSourceData !== null && (
        <Source id="hex" type="geojson" data={hexSourceData}>
          <Layer
            id="hex-fill"
            type="fill"
            beforeId="neighborhoods-fill"
            paint={{
              "fill-color": VIRIDIS_COLOR_EXPR,
              "fill-opacity": isDark ? 0.18 : 0.38,
            }}
          />
          {/* Hex glow border — soft halo per cell, dark mode only */}
          {isDark && (
            <Layer
              id="hex-glow"
              type="line"
              paint={{
                "line-color": VIRIDIS_COLOR_EXPR,
                "line-width": 3.5,
                "line-opacity": 0.12,
                "line-blur": 2,
              }}
            />
          )}
          {/* Hex core border — subtle honeycomb grid */}
          <Layer
            id="hex-border"
            type="line"
            paint={{
              "line-color": VIRIDIS_COLOR_EXPR,
              "line-width": 0.5,
              "line-opacity": isDark ? 0.4 : 0.3,
            }}
          />
        </Source>
      )}

      {/* Neighbourhood layers (fill → glow → line → labels) */}
      <Source id="neighborhoods" type="geojson" data={sourceData}>
        <Layer {...fillSpec} />
        {devFlags.buildingGlow && (
          <Layer
            id="neighborhoods-glow-base"
            type="fill-extrusion"
            source="neighborhoods"
            paint={{
              "fill-extrusion-color": VIRIDIS_COLOR_EXPR,
              "fill-extrusion-height": 18,
              "fill-extrusion-base": 0,
              "fill-extrusion-opacity": isDark ? 0.18 : 0.12,
            }}
          />
        )}
        {devFlags.glowBorders && <Layer {...glowSpec} />}
        <Layer {...lineSpec} />
        <Layer {...labelsSpec} />
      </Source>
    </MapGL>
  );
}
