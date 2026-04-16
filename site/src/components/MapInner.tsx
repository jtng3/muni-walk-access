import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import { Map as MapGL, Source, Layer, Marker } from "react-map-gl/maplibre";
import { Protocol } from "pmtiles";
import { layers as pmLayers, LIGHT, DARK } from "@protomaps/basemaps";
import { VIRIDIS_STOPS } from "@/lib/choropleth";
import type { GridSchema, HexGridSchema } from "@/lib/types";
import type {
  FillLayerSpecification,
  LineLayerSpecification,
  StyleSpecification,
  GeoJSONSourceSpecification,
} from "maplibre-gl";
import type { DevFlags } from "./DevOverlay";
import polylabel from "@/lib/polylabel";

// Manual label position overrides for neighborhoods where polylabel doesn't look right
const LABEL_OVERRIDES: Record<string, [number, number]> = {
  "outer-richmond": [-122.4968, 37.7785], // shift left — very wide polygon
  "golden-gate-park": [-122.48, 37.7692], // shift left
  "financial-district-south-beach": [-122.392, 37.7904], // shift right + south
  chinatown: [-122.4065, 37.7981], // shift north (adjusted south 0.001)
};

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

// Fill opacity is controlled by devFlags.fillOpacity slider

function buildChoroplethFill(
  opacity: number,
  viewMode: "summary" | "detailed" = "summary",
): FillLayerSpecification {
  // Detailed mode: hex carries the color, neighbourhood fill transparent but
  // still queryable for hover tooltips (visibility must remain "visible").
  if (viewMode === "detailed") {
    return {
      id: "neighborhoods-fill",
      type: "fill",
      source: "neighborhoods",
      paint: { "fill-color": VIRIDIS_COLOR_EXPR, "fill-opacity": 0 },
    };
  }
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

// Glow border — white glow in dark mode (same treatment as hex borders),
// viridis-colored in light mode.
function buildGlowBorder(isDark: boolean): LineLayerSpecification {
  return {
    id: "neighborhoods-glow",
    type: "line",
    source: "neighborhoods",
    paint: {
      "line-color": isDark ? "#ffffff" : VIRIDIS_COLOR_EXPR,
      "line-width": isDark ? 3 : 5,
      "line-opacity": isDark ? 0.15 : 0.15,
      "line-blur": isDark ? 2.5 : 2,
    } as any,
  };
}

// Core border line
function buildChoroplethLine(isDark: boolean): LineLayerSpecification {
  return {
    id: "neighborhoods-line",
    type: "line",
    source: "neighborhoods",
    paint: {
      "line-color": isDark ? "#ffffff" : "#334155",
      "line-width": isDark ? 0.5 : 1,
      "line-opacity": isDark ? 0.2 : 0.6,
    },
  };
}

// Label styles — CSS text-shadow produces a smooth glow (no WebGL halo artifacts)
const LABEL_STYLE_LIGHT: React.CSSProperties = {
  color: "rgba(30,41,59,0.95)",
  textShadow: "0 0 3px rgba(255,255,255,0.6), 0 0 1px rgba(255,255,255,0.7)",
  fontSize: 11,
  fontFamily: "'Inter', system-ui, sans-serif",
  fontWeight: 500,
  lineHeight: 1.2,
  textAlign: "center",
  pointerEvents: "none",
  userSelect: "none",
  whiteSpace: "nowrap",
};
const LABEL_STYLE_DARK: React.CSSProperties = {
  ...LABEL_STYLE_LIGHT,
  color: "rgba(203,213,225,0.95)",
  textShadow: "0 0 3px rgba(0,0,0,0.5), 0 0 1px rgba(0,0,0,0.6)",
};

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
  showLabels: boolean;
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
  showLabels,
}: MapInnerProps) {
  const loadedRef = useRef(false);
  const [hover, setHover] = useState<{
    name: string;
    pct: number;
    x: number;
    y: number;
  } | null>(null);

  const mapStyle = useMemo(
    () => buildMapStyle(isDark, devFlags),
    [isDark, devFlags],
  );
  const fillSpec = useMemo(
    () => buildChoroplethFill(devFlags.fillOpacity, viewMode),
    [devFlags.fillOpacity, viewMode],
  );
  const glowSpec = useMemo(() => buildGlowBorder(isDark), [isDark]);
  const lineSpec = useMemo(() => buildChoroplethLine(isDark), [isDark]);

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
        if (!cancelled) {
          setBaseGeoJSON(fc);
          if (import.meta.env.DEV) {
            const geoIds = new Set(
              fc.features.map((f: any) => f.properties?.id),
            );
            const gridIds = new Set(data.neighborhoods.map((n) => n.id));
            const missing = [...geoIds].filter((id) => !gridIds.has(id));
            const extra = [...gridIds].filter((id) => !geoIds.has(id));
            if (missing.length || extra.length) {
              console.warn("[MapInner] ID mismatch!", {
                inGeoJSONNotGrid: missing,
                inGridNotGeoJSON: extra,
              });
            }
          }
        }
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
        if (idx === undefined) {
          if (import.meta.env.DEV) {
            console.warn(
              `[MapInner] sourceData: no grid entry for id="${f.properties?.id}" — using baked-in pct=${f.properties?.pct_at_defaults}`,
            );
          }
          return f;
        }
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

  // Polylabel visual centers — computed once from polygon geometry.
  // For MultiPolygons, pick the largest polygon by area (shoelace) to avoid
  // tiny slivers like piers or waterfront strips pulling the label off-center.
  const labelCenters = useMemo(() => {
    if (!baseGeoJSON) return null;

    // Shoelace area — unsigned, works for any simple polygon ring
    const ringArea = (ring: number[][]) => {
      let a = 0;
      for (let i = 0, j = ring.length - 1; i < ring.length; j = i++)
        a += ring[j][0] * ring[i][1] - ring[i][0] * ring[j][1];
      return Math.abs(a / 2);
    };

    return baseGeoJSON.features.map((f) => {
      let coords: number[][][];
      if (f.geometry.type === "MultiPolygon") {
        const mp = f.geometry as GeoJSON.MultiPolygon;
        coords = mp.coordinates.reduce((best, poly) =>
          ringArea(poly[0]) > ringArea(best[0]) ? poly : best,
        );
      } else {
        coords = (f.geometry as GeoJSON.Polygon).coordinates;
      }
      const id = f.properties?.id as string;
      return LABEL_OVERRIDES[id] ?? polylabel(coords);
    });
  }, [baseGeoJSON]);

  // Label point source — visual centers with live pct values
  const labelSourceData = useMemo<GeoJSON.FeatureCollection | null>(() => {
    if (!baseGeoJSON || !labelCenters) return null;
    return {
      type: "FeatureCollection",
      features: baseGeoJSON.features.map((f, i) => {
        const idx = idxById.get(f.properties?.id);
        const pct =
          idx !== undefined
            ? data.neighborhoods[idx].pct_within[rendered.freqIdx][
                rendered.walkIdx
              ]
            : 0;
        if (idx === undefined && import.meta.env.DEV) {
          console.warn(
            `[MapInner] labelSourceData: no grid entry for id="${f.properties?.id}" — using baked-in pct=${pct}`,
          );
        }
        return {
          type: "Feature" as const,
          geometry: {
            type: "Point" as const,
            coordinates: labelCenters[i],
          },
          properties: {
            name: f.properties?.name,
            pct_at_defaults: pct,
          },
        };
      }),
    };
  }, [
    baseGeoJSON,
    labelCenters,
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
      onMouseMove={(e) => {
        const hits = e.target.queryRenderedFeatures(e.point, {
          layers: ["neighborhoods-fill"],
        });
        if (hits.length > 0 && hits[0].properties?.name) {
          setHover({
            name: hits[0].properties.name as string,
            pct: Math.round(
              ((hits[0].properties.pct_at_defaults as number) ?? 0) * 100,
            ),
            x: e.originalEvent.clientX,
            y: e.originalEvent.clientY,
          });
          e.target.getCanvas().style.cursor = "pointer";
        } else {
          setHover(null);
          e.target.getCanvas().style.cursor = "";
        }
      }}
      onMouseLeave={() => setHover(null)}
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
              "fill-opacity": devFlags.fillOpacity,
            }}
          />
          {/* Hex border — white/gray line with subtle glow.
              Lightens the fill color at edges, creating a clean honeycomb grid. */}
          {/* Opacity halved so shared edges (drawn 2x) look correct;
              outer boundary edges will be subtler. */}
          <Layer
            id="hex-glow"
            type="line"
            paint={{
              "line-color": isDark ? "#ffffff" : "#000000",
              "line-width": isDark ? 2.5 : 1.5,
              "line-opacity": isDark ? 0.075 : 0.05,
              "line-blur": isDark ? 2 : 1,
            }}
          />
          <Layer
            id="hex-border"
            type="line"
            paint={{
              "line-color": isDark ? "#ffffff" : "#000000",
              "line-width": 0.5,
              "line-opacity": isDark ? 0.075 : 0.06,
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
      </Source>

      {/* Neighbourhood labels — HTML markers with CSS text-shadow glow */}
      {showLabels &&
        labelSourceData?.features.map((f) => {
          const [lng, lat] = (f.geometry as GeoJSON.Point).coordinates as [
            number,
            number,
          ];
          const name = f.properties?.name as string;
          const pct = Math.round(
            ((f.properties?.pct_at_defaults as number) ?? 0) * 100,
          );
          return (
            <Marker key={name} longitude={lng} latitude={lat} anchor="center">
              <div style={isDark ? LABEL_STYLE_DARK : LABEL_STYLE_LIGHT}>
                {name}
                <br />
                <span style={{ fontSize: 12, fontWeight: 600 }}>{pct}%</span>
              </div>
            </Marker>
          );
        })}

      {/* Hover tooltip */}
      {hover && (
        <div
          style={{
            position: "fixed",
            left: hover.x + 14,
            top: hover.y - 32,
            background: isDark ? "rgba(15,23,42,0.6)" : "rgba(255,255,255,0.6)",
            backdropFilter: "blur(12px)",
            WebkitBackdropFilter: "blur(12px)",
            color: isDark ? "#e2e8f0" : "#1e293b",
            borderRadius: 8,
            padding: "6px 12px",
            fontSize: 13,
            fontFamily: "'Inter', system-ui, sans-serif",
            pointerEvents: "none",
            boxShadow: "0 2px 10px rgba(0,0,0,0.18)",
            zIndex: 50,
            whiteSpace: "nowrap",
          }}
        >
          {hover.name}
          <strong style={{ marginLeft: 8 }}>{hover.pct}%</strong>
        </div>
      )}
    </MapGL>
  );
}
