import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import { Map as MapGL, Source, Layer } from "react-map-gl/maplibre";
import { Protocol } from "pmtiles";
import { layers as pmLayers, LIGHT, DARK } from "@protomaps/basemaps";
import { VIRIDIS_STOPS } from "@/lib/choropleth";
import type { GridSchema } from "@/lib/types";
import type {
  FillLayerSpecification,
  LineLayerSpecification,
  StyleSpecification,
  GeoJSONSourceSpecification,
} from "maplibre-gl";

// Self-hosted PMTiles extract (23 MB, zoom 0–14, SF + bay + Oakland waterfront).
// Regenerate with:
//   pmtiles extract https://build.protomaps.com/20251201.pmtiles \
//     site/public/tiles/sf.pmtiles --bbox="-122.55,37.69,-122.20,37.85"
const TILE_URL = "pmtiles:///tiles/sf.pmtiles";

function buildMapStyle(isDark: boolean): StyleSpecification {
  return {
    version: 8,
    sources: {
      protomaps: {
        type: "vector",
        url: TILE_URL,
      },
    },
    layers: pmLayers("protomaps", isDark ? DARK : LIGHT),
  };
}

const choroplethFill: FillLayerSpecification = {
  id: "neighborhoods-fill",
  type: "fill",
  source: "neighborhoods",
  paint: {
    "fill-color": [
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
    ],
    "fill-opacity": 0.75,
  },
};

function buildChoroplethLine(isDark: boolean): LineLayerSpecification {
  return {
    id: "neighborhoods-line",
    type: "line",
    source: "neighborhoods",
    paint: {
      "line-color": isDark ? "#8b949e" : "#334155",
      "line-width": 1,
      "line-opacity": isDark ? 0.4 : 0.6,
    },
  };
}

interface MapInnerProps {
  data: GridSchema;
  freqIdx: number;
  walkIdx: number;
  isDark: boolean;
  onError?: () => void;
}

export default function MapInner({
  data,
  freqIdx,
  walkIdx,
  isDark,
  onError,
}: MapInnerProps) {
  const loadedRef = useRef(false);

  const mapStyle = useMemo(() => buildMapStyle(isDark), [isDark]);
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

  // Produce updated FeatureCollection with recolored pct_at_defaults
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
      <Source id="neighborhoods" type="geojson" data={sourceData}>
        <Layer {...choroplethFill} />
        <Layer {...lineSpec} />
      </Source>
    </MapGL>
  );
}
