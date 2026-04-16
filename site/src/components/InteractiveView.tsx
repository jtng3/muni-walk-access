import { useMemo, useState, useCallback, useRef, useEffect } from "react";
import { useUrlState } from "@/lib/useUrlState";
import { useTheme } from "@/lib/useTheme";
import type { GridSchema, HexGridSchema, TimeWindowKey } from "@/lib/types";
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
    [data],
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
  const [showLabels, setShowLabels] = useState(true);
  const [timeWindow, setTimeWindow] = useState<TimeWindowKey>("am_peak");

  // Per-window grid data (neighborhoods + city_wide pct_within)
  const [windowGrid, setWindowGrid] = useState<GridSchema | null>(null);
  const gridCacheRef = useRef<Map<string, GridSchema>>(new Map());

  useEffect(() => {
    const cached = gridCacheRef.current.get(timeWindow);
    if (cached) {
      setWindowGrid(cached);
      return;
    }
    let cancelled = false;
    fetch(`/data/grid_${timeWindow}.json`)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((d: GridSchema) => {
        if (!cancelled) {
          gridCacheRef.current.set(timeWindow, d);
          setWindowGrid(d);
        }
      })
      .catch((err) => {
        console.error(`[grid] Failed to load grid_${timeWindow}.json:`, err);
        if (!cancelled) setWindowGrid(null);
      });
    return () => {
      cancelled = true;
    };
  }, [timeWindow]);

  // Use per-window grid data when available, fall back to static build-time data
  const activeGrid = windowGrid ?? data;

  // Hex data state
  const [hexBaseFC, setHexBaseFC] = useState<GeoJSON.FeatureCollection | null>(
    null,
  );
  const [hexData, setHexData] = useState<HexGridSchema | null>(null);
  const [hexLoading, setHexLoading] = useState(false);
  const [failedResolutions, setFailedResolutions] = useState<Set<number>>(
    new Set(),
  );
  const [h3Failed, setH3Failed] = useState(false);

  // Stable refs — not re-initialised on render
  const hexCacheRef = useRef<
    Map<string, { fc: GeoJSON.FeatureCollection; data: HexGridSchema }>
  >(new Map());
  const h3Ref = useRef<H3Module | null>(null);
  // cancelRef holds a function that marks the current in-flight load as cancelled
  const cancelRef = useRef<(() => void) | null>(null);

  const loadHexResolution = useCallback(
    async (res: number, tw: TimeWindowKey) => {
      // Cancel any previous in-flight load
      cancelRef.current?.();
      let cancelled = false;
      cancelRef.current = () => {
        cancelled = true;
      };

      const cacheKey = `${res}_${tw}`;

      // Fast path: cache hit (clear loading state in case a cancelled load left it true)
      const cached = hexCacheRef.current.get(cacheKey);
      if (cached) {
        setHexBaseFC(cached.fc);
        setHexData(cached.data);
        setHexLoading(false);
        return;
      }

      setHexLoading(true);
      try {
        const resp = await fetch(`/data/grid_hex_r${res}_${tw}.json`);
        if (cancelled) return;
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const hexGridData: HexGridSchema = await resp.json();
        if (cancelled) return;

        // Lazy-load h3-js on first use; cache the module ref
        if (!h3Ref.current) {
          try {
            const mod = await import("h3-js");
            h3Ref.current = mod as unknown as H3Module;
          } catch (h3Err) {
            console.error("[hex] h3-js failed to load:", h3Err);
            if (!cancelled) setH3Failed(true);
            return;
          }
        }
        if (cancelled) return;
        const h3 = h3Ref.current;

        // Build base FeatureCollection (geometry + initial pct_at_defaults).
        // Feature order matches hexGridData.cells order — MapInner relies on this.
        // cellToBoundary(id, true) returns [lng, lat][] — GeoJSON coordinate order.
        const fc: GeoJSON.FeatureCollection = {
          type: "FeatureCollection",
          features: hexGridData.cells.map((cell) => {
            const ring = h3.cellToBoundary(cell.id, true);
            // Defensive: ensure GeoJSON polygon ring is closed (RFC 7946)
            if (
              ring.length > 0 &&
              (ring[0][0] !== ring[ring.length - 1][0] ||
                ring[0][1] !== ring[ring.length - 1][1])
            ) {
              ring.push(ring[0]);
            }
            return {
              type: "Feature" as const,
              geometry: {
                type: "Polygon" as const,
                coordinates: [ring],
              },
              properties: {
                id: cell.id,
                pct_at_defaults:
                  cell.pct_within[hexGridData.defaults.frequency_idx][
                    hexGridData.defaults.walking_idx
                  ],
              },
            };
          }),
        };

        hexCacheRef.current.set(cacheKey, { fc, data: hexGridData });
        setHexBaseFC(fc);
        setHexData(hexGridData);
      } catch (err) {
        if (!cancelled) {
          console.error(`[hex] Failed to load r${res}_${tw}:`, err);
          setFailedResolutions((prev) => new Set([...prev, res]));
          // Clear stale hex data so UI doesn't show wrong resolution
          setHexBaseFC(null);
          setHexData(null);
        }
      } finally {
        if (!cancelled) setHexLoading(false);
      }
    },
    [],
  ); // stable — only touches refs and setters

  const handleViewModeChange = useCallback(
    (mode: "summary" | "detailed") => {
      setViewMode(mode);
      if (mode === "detailed") {
        void loadHexResolution(hexRes, timeWindow);
      } else {
        // Cancel any in-flight hex load when switching back to summary
        cancelRef.current?.();
      }
    },
    [hexRes, timeWindow, loadHexResolution],
  );

  const handleHexResChange = useCallback(
    (res: number) => {
      setHexRes(res);
      void loadHexResolution(res, timeWindow);
    },
    [timeWindow, loadHexResolution],
  );

  const handleTimeWindowChange = useCallback(
    (tw: TimeWindowKey) => {
      setTimeWindow(tw);
      if (viewMode === "detailed") {
        void loadHexResolution(hexRes, tw);
      }
    },
    [hexRes, viewMode, loadHexResolution],
  );

  const pct = activeGrid.city_wide.pct_within[freqIdx][walkIdx];
  const freqMin = activeGrid.axes.frequency_minutes[freqIdx];
  const walkMin = activeGrid.axes.walking_minutes[walkIdx];

  return (
    <div className="relative" style={{ height: "calc(100dvh - 3rem)" }}>
      <div className="absolute inset-0">
        <MapView
          data={activeGrid}
          freqIdx={freqIdx}
          walkIdx={walkIdx}
          isDark={isDark}
          devFlags={devFlags}
          viewMode={viewMode}
          hexBaseFC={hexBaseFC}
          hexData={hexData}
          showLabels={showLabels}
        />
      </div>
      <Controls
        axes={activeGrid.axes}
        defaults={activeGrid.defaults}
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
        h3Failed={h3Failed}
        showLabels={showLabels}
        onShowLabelsChange={setShowLabels}
        timeWindow={timeWindow}
        onTimeWindowChange={handleTimeWindowChange}
      />
      <DevOverlay flags={devFlags} onChange={setDevFlags} />
    </div>
  );
}
