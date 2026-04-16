import { useMemo, useState, useCallback, useRef } from "react";
import { useUrlState } from "@/lib/useUrlState";
import { useTheme } from "@/lib/useTheme";
import type { GridSchema, HexGridSchema } from "@/lib/types";
import Controls from "./Controls";
import MapView from "./MapView";
import DevOverlay, { DEFAULT_DEV_FLAGS } from "./DevOverlay";
import type { DevFlags } from "./DevOverlay";

// Minimal interface for the lazily-loaded h3-js module — only cellToBoundary needed.
interface H3Module {
  cellToBoundary(
    h3Index: string,
    formatAsGeoJson?: boolean,
  ): [number, number][];
}

interface InteractiveViewProps {
  data: GridSchema;
}

export default function InteractiveView({ data }: InteractiveViewProps) {
  const totalAddresses = useMemo(
    () => data.neighborhoods.reduce((sum, n) => sum + n.population, 0),
    [data.neighborhoods],
  );
  const [freqIdx, setFreqIdx] = useUrlState(
    "freq",
    data.defaults.frequency_idx,
    0,
    data.axes.frequency_minutes.length - 1,
  );
  const [walkIdx, setWalkIdx] = useUrlState(
    "walk",
    data.defaults.walking_idx,
    0,
    data.axes.walking_minutes.length - 1,
  );
  const [theme, toggleTheme] = useTheme();
  const isDark = theme === "dark";
  const [devFlags, setDevFlags] = useState<DevFlags>(DEFAULT_DEV_FLAGS);

  // View mode and resolution — local state (not URL-shared; display preferences only)
  const [viewMode, setViewMode] = useState<"summary" | "detailed">("summary");
  const [hexRes, setHexRes] = useState(8);

  // Hex data state
  const [hexBaseFC, setHexBaseFC] = useState<GeoJSON.FeatureCollection | null>(
    null,
  );
  const [hexData, setHexData] = useState<HexGridSchema | null>(null);
  const [hexLoading, setHexLoading] = useState(false);
  const [failedResolutions, setFailedResolutions] = useState<Set<number>>(
    new Set(),
  );

  // Stable refs — not re-initialised on render
  const hexCacheRef = useRef<
    Map<number, { fc: GeoJSON.FeatureCollection; data: HexGridSchema }>
  >(new Map());
  const h3Ref = useRef<H3Module | null>(null);
  // cancelRef holds a function that marks the current in-flight load as cancelled
  const cancelRef = useRef<(() => void) | null>(null);

  const loadHexResolution = useCallback(async (res: number) => {
    // Cancel any previous in-flight load
    cancelRef.current?.();
    let cancelled = false;
    cancelRef.current = () => {
      cancelled = true;
    };

    // Fast path: cache hit (clear loading state in case a cancelled load left it true)
    const cached = hexCacheRef.current.get(res);
    if (cached) {
      setHexBaseFC(cached.fc);
      setHexData(cached.data);
      setHexLoading(false);
      return;
    }

    setHexLoading(true);
    try {
      const resp = await fetch(`/data/grid_hex_r${res}.json`);
      if (cancelled) return;
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const hexGridData: HexGridSchema = await resp.json();
      if (cancelled) return;

      // Lazy-load h3-js on first use; cache the module ref
      if (!h3Ref.current) {
        const mod = await import("h3-js");
        h3Ref.current = mod as unknown as H3Module;
      }
      if (cancelled) return;
      const h3 = h3Ref.current;

      // Build base FeatureCollection (geometry + initial pct_at_defaults).
      // Feature order matches hexGridData.cells order — MapInner relies on this.
      // cellToBoundary(id, true) returns [lng, lat][] — GeoJSON coordinate order.
      const fc: GeoJSON.FeatureCollection = {
        type: "FeatureCollection",
        features: hexGridData.cells.map((cell) => ({
          type: "Feature" as const,
          geometry: {
            type: "Polygon" as const,
            coordinates: [h3.cellToBoundary(cell.id, true)],
          },
          properties: {
            id: cell.id,
            pct_at_defaults:
              cell.pct_within[hexGridData.defaults.frequency_idx][
                hexGridData.defaults.walking_idx
              ],
          },
        })),
      };

      hexCacheRef.current.set(res, { fc, data: hexGridData });
      setHexBaseFC(fc);
      setHexData(hexGridData);
    } catch (err) {
      if (!cancelled) {
        console.error(`[hex] Failed to load r${res}:`, err);
        setFailedResolutions((prev) => new Set([...prev, res]));
      }
    } finally {
      if (!cancelled) setHexLoading(false);
    }
  }, []); // stable — only touches refs and setters

  const handleViewModeChange = useCallback(
    (mode: "summary" | "detailed") => {
      setViewMode(mode);
      if (mode === "detailed") {
        void loadHexResolution(hexRes);
      }
    },
    [hexRes, loadHexResolution],
  );

  const handleHexResChange = useCallback(
    (res: number) => {
      setHexRes(res);
      void loadHexResolution(res);
    },
    [loadHexResolution],
  );

  const pct = data.city_wide.pct_within[freqIdx][walkIdx];
  const freqMin = data.axes.frequency_minutes[freqIdx];
  const walkMin = data.axes.walking_minutes[walkIdx];

  return (
    <div className="relative" style={{ height: "calc(100dvh - 3rem)" }}>
      <div className="absolute inset-0">
        <MapView
          data={data}
          freqIdx={freqIdx}
          walkIdx={walkIdx}
          isDark={isDark}
          devFlags={devFlags}
          viewMode={viewMode}
          hexBaseFC={hexBaseFC}
          hexData={hexData}
        />
      </div>
      <Controls
        axes={data.axes}
        defaults={data.defaults}
        freqIdx={freqIdx}
        walkIdx={walkIdx}
        onFreqChange={setFreqIdx}
        onWalkChange={setWalkIdx}
        isDark={isDark}
        onThemeToggle={toggleTheme}
        pct={pct}
        freqMin={freqMin}
        walkMin={walkMin}
        totalAddresses={totalAddresses}
        viewMode={viewMode}
        onViewModeChange={handleViewModeChange}
        hexRes={hexRes}
        onHexResChange={handleHexResChange}
        hexLoading={hexLoading}
        failedResolutions={failedResolutions}
      />
      <DevOverlay flags={devFlags} onChange={setDevFlags} />
    </div>
  );
}
