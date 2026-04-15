import { useEffect } from "react";
import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import { Map, Source, Layer } from "react-map-gl/maplibre";
import { Protocol } from "pmtiles";
import { layers as pmLayers, LIGHT } from "@protomaps/basemaps";
import { VIRIDIS_STOPS } from "@/lib/choropleth";
import type { GridSchema } from "@/lib/types";
import type {
  FillLayerSpecification,
  LineLayerSpecification,
  StyleSpecification,
} from "maplibre-gl";

// Self-hosted SF PMTiles extract (13 MB, zoom 0–14, bbox -122.525,37.705,-122.355,37.835).
// Regenerate with:
//   pmtiles extract https://build.protomaps.com/20251201.pmtiles \
//     site/public/tiles/sf.pmtiles --bbox="-122.525,37.705,-122.355,37.835"
const TILE_URL = "pmtiles:///tiles/sf.pmtiles";

const MAP_STYLE: StyleSpecification = {
  version: 8,
  sources: {
    protomaps: {
      type: "vector",
      url: TILE_URL,
    },
  },
  layers: pmLayers("protomaps", LIGHT),
};

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

const choroplethLine: LineLayerSpecification = {
  id: "neighborhoods-line",
  type: "line",
  source: "neighborhoods",
  paint: {
    "line-color": "#334155", // slate-700 (--primary)
    "line-width": 1,
    "line-opacity": 0.6,
  },
};

interface MapInnerProps {
  data: GridSchema;
}

export default function MapInner(_props: MapInnerProps) {
  useEffect(() => {
    const protocol = new Protocol();
    maplibregl.addProtocol("pmtiles", protocol.tile);
    return () => {
      maplibregl.removeProtocol("pmtiles");
    };
  }, []);

  return (
    <Map
      initialViewState={{
        bounds: [
          [-122.525, 37.705],
          [-122.355, 37.835],
        ],
        fitBoundsOptions: { padding: 20 },
      }}
      mapStyle={MAP_STYLE}
      style={{ width: "100%", height: "100%" }}
    >
      <Source
        id="neighborhoods"
        type="geojson"
        data="/data/neighborhoods.geojson"
      >
        <Layer {...choroplethFill} />
        <Layer {...choroplethLine} />
      </Source>
    </Map>
  );
}
