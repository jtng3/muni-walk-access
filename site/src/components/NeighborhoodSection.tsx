import { useMemo, useState, useEffect, useRef, useCallback } from "react";
import { useUrlState, useUrlStringState } from "@/lib/useUrlState";
import type {
  GridSchema,
  ConfigSnapshot,
  TimeWindowKey,
  RouteMode,
} from "@/lib/types";
import { TIME_WINDOWS } from "@/lib/types";

const TIME_WINDOW_KEYS = TIME_WINDOWS.map(
  (tw) => tw.key,
) as unknown as readonly TimeWindowKey[];
const ROUTE_MODES: readonly RouteMode[] = ["aggregate", "headway"];
import NeighborhoodCard from "./NeighborhoodCard";
import NeighborhoodControls from "./NeighborhoodControls";
import DataSourceDates from "./DataSourceDates";
import type { LensFilter, SortKey, SortDir } from "./NeighborhoodControls";

interface NeighborhoodSectionProps {
  data: GridSchema;
  config: ConfigSnapshot;
}

export default function NeighborhoodSection({
  data,
  config,
}: NeighborhoodSectionProps) {
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

  const [timeWindow, setTimeWindow] = useUrlStringState<TimeWindowKey>(
    "tw",
    "am_peak",
    TIME_WINDOW_KEYS,
  );
  const [routeMode, setRouteMode] = useUrlStringState<RouteMode>(
    "rm",
    "aggregate",
    ROUTE_MODES,
  );
  const [lensFilter, setLensFilter] = useState<LensFilter>("all");
  const [sortKey, setSortKey] = useState<SortKey>("pct");
  const [sortDir, setSortDir] = useState<SortDir>("desc");
  const [headwayUnavailable, setHeadwayUnavailable] = useState(false);

  // Per-window grid data
  const [windowGrid, setWindowGrid] = useState<GridSchema | null>(null);
  const gridCacheRef = useRef<Map<string, GridSchema>>(new Map());

  useEffect(() => {
    const suffix =
      routeMode === "headway" ? `${timeWindow}_headway` : timeWindow;
    const cached = gridCacheRef.current.get(suffix);
    if (cached) {
      setWindowGrid(cached);
      if (routeMode === "headway") setHeadwayUnavailable(false);
      return;
    }
    let cancelled = false;
    fetch(`/data/grid_${suffix}.json`)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((d: GridSchema) => {
        if (!cancelled) {
          gridCacheRef.current.set(suffix, d);
          setWindowGrid(d);
          if (routeMode === "headway") setHeadwayUnavailable(false);
        }
      })
      .catch((err) => {
        console.error(
          `[NeighborhoodSection] Failed to load grid_${suffix}.json:`,
          err,
        );
        if (!cancelled) {
          setWindowGrid(null);
          if (routeMode === "headway") setHeadwayUnavailable(true);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [timeWindow, routeMode]);

  const activeGrid = windowGrid ?? data;

  const handleSortChange = useCallback((key: SortKey, dir: SortDir) => {
    setSortKey(key);
    setSortDir(dir);
  }, []);

  // Filter, then sort
  const neighborhoods = useMemo(() => {
    let list = [...activeGrid.neighborhoods];

    // Lens filter
    if (lensFilter === "ej") {
      list = list.filter((n) => n.lens_flags.ej_communities);
    } else if (lensFilter === "esn") {
      list = list.filter((n) => n.lens_flags.equity_strategy);
    }

    // Sort
    const mul = sortDir === "asc" ? 1 : -1;
    list.sort((a, b) => {
      switch (sortKey) {
        case "pct":
          return (
            mul *
            (a.pct_within[freqIdx][walkIdx] - b.pct_within[freqIdx][walkIdx])
          );
        case "population":
          return mul * (a.population - b.population);
        case "name":
          return mul * a.name.localeCompare(b.name);
      }
    });

    return list;
  }, [
    activeGrid.neighborhoods,
    lensFilter,
    sortKey,
    sortDir,
    freqIdx,
    walkIdx,
  ]);

  return (
    <section id="neighborhoods" className="max-w-7xl mx-auto px-4 py-12">
      <div className="flex items-baseline justify-between mb-6">
        <h2 className="text-2xl font-heading font-semibold">Neighborhoods</h2>
        <DataSourceDates config={config} />
      </div>
      {/* Sticky toolbar — borderless, Linear-style */}
      <div className="sticky top-12 z-20 mb-6 bg-background/95 backdrop-blur-sm pb-1">
        <NeighborhoodControls
          axes={activeGrid.axes}
          defaults={activeGrid.defaults}
          freqIdx={freqIdx}
          walkIdx={walkIdx}
          onFreqChange={setFreqIdx}
          onWalkChange={setWalkIdx}
          timeWindow={timeWindow}
          onTimeWindowChange={setTimeWindow}
          routeMode={routeMode}
          onRouteModeChange={setRouteMode}
          headwayUnavailable={headwayUnavailable}
          lensFilter={lensFilter}
          onLensFilterChange={setLensFilter}
          sortKey={sortKey}
          sortDir={sortDir}
          onSortChange={handleSortChange}
          count={neighborhoods.length}
        />
      </div>

      {/* Card grid — full width */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
        {neighborhoods.map((n) => (
          <NeighborhoodCard
            key={n.id}
            id={n.id}
            name={n.name}
            pct={n.pct_within[freqIdx][walkIdx]}
            population={n.population}
            lensFlags={n.lens_flags}
          />
        ))}
      </div>
    </section>
  );
}
