import { useState } from "react";
import { ChevronDown, ChevronUp } from "lucide-react";
import type {
  GridAxes,
  GridDefaults,
  TimeWindowKey,
  RouteMode,
} from "@/lib/types";
import { TIME_WINDOWS } from "@/lib/types";
import { Button } from "@/components/ui/button";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import FrequencySlider from "./FrequencySlider";
import WalkingTimeSlider from "./WalkingTimeSlider";

type LensFilter = "all" | "ej" | "esn";
type SortKey = "pct" | "population" | "name";
type SortDir = "asc" | "desc";

interface NeighborhoodControlsProps {
  axes: GridAxes;
  defaults: GridDefaults;
  freqIdx: number;
  walkIdx: number;
  onFreqChange: (idx: number) => void;
  onWalkChange: (idx: number) => void;
  timeWindow: TimeWindowKey;
  onTimeWindowChange: (tw: TimeWindowKey) => void;
  routeMode: RouteMode;
  onRouteModeChange: (mode: RouteMode) => void;
  headwayUnavailable: boolean;
  lensFilter: LensFilter;
  onLensFilterChange: (filter: LensFilter) => void;
  sortKey: SortKey;
  sortDir: SortDir;
  onSortChange: (key: SortKey, dir: SortDir) => void;
  count: number;
}

export type { LensFilter, SortKey, SortDir };

const MILE_FRACTIONS: Record<number, string> = {
  5: "¼",
  10: "½",
  15: "¾",
};

function SortButton({
  label,
  sortKey: key,
  active,
  dir,
  onClick,
}: {
  label: string;
  sortKey: SortKey;
  active: boolean;
  dir: SortDir;
  onClick: (key: SortKey, dir: SortDir) => void;
}) {
  return (
    <button
      onClick={() => {
        if (active) {
          onClick(key, dir === "asc" ? "desc" : "asc");
        } else {
          onClick(key, key === "name" ? "asc" : "desc");
        }
      }}
      className={`px-2 py-0.5 text-xs rounded-md transition-colors cursor-pointer ${
        active
          ? "text-foreground font-medium bg-muted"
          : "text-muted-foreground hover:text-foreground"
      }`}
    >
      {label}
      {active && (
        <span className="ml-0.5 text-[10px]">{dir === "asc" ? "↑" : "↓"}</span>
      )}
    </button>
  );
}

function FilterChip({
  label,
  active,
  onClick,
}: {
  label: string;
  active: boolean;
  onClick: () => void;
}) {
  return (
    <button
      onClick={onClick}
      className={`px-2.5 py-0.5 text-xs font-medium rounded-full transition-colors cursor-pointer ${
        active
          ? "text-primary-foreground bg-primary"
          : "text-muted-foreground border border-foreground/10 hover:text-foreground hover:border-foreground/20"
      }`}
    >
      {label}
    </button>
  );
}

export default function NeighborhoodControls({
  axes,
  defaults,
  freqIdx,
  walkIdx,
  onFreqChange,
  onWalkChange,
  timeWindow,
  onTimeWindowChange,
  routeMode,
  onRouteModeChange,
  headwayUnavailable,
  lensFilter,
  onLensFilterChange,
  sortKey,
  sortDir,
  onSortChange,
  count,
}: NeighborhoodControlsProps) {
  const [open, setOpen] = useState(true);

  const freqMin = axes.frequency_minutes[freqIdx];
  const walkMin = axes.walking_minutes[walkIdx];
  const walkMiles = MILE_FRACTIONS[walkMin] ?? `${(walkMin / 20).toFixed(1)}`;
  const twLabel =
    TIME_WINDOWS.find((tw) => tw.key === timeWindow)?.long ?? timeWindow;
  const freqLabel =
    routeMode === "headway" ? `wait ≤ ${freqMin}` : `every ${freqMin}`;

  return (
    <div>
      {/* Summary bar */}
      <div className="flex flex-wrap items-center gap-x-3 gap-y-2">
        <span className="text-sm text-muted-foreground">
          <span className="font-medium tabular-nums text-foreground">
            {count}
          </span>{" "}
          neighborhoods · {twLabel} · {freqLabel} min · {walkMiles} mi walk
        </span>

        {/* Right side */}
        <div className="ml-auto flex items-center gap-1">
          <FilterChip
            label="EJ"
            active={lensFilter === "ej"}
            onClick={() =>
              onLensFilterChange(lensFilter === "ej" ? "all" : "ej")
            }
          />
          <FilterChip
            label="ESN"
            active={lensFilter === "esn"}
            onClick={() =>
              onLensFilterChange(lensFilter === "esn" ? "all" : "esn")
            }
          />

          <span className="text-muted-foreground/30 mx-1">|</span>

          <SortButton
            label="%"
            sortKey="pct"
            active={sortKey === "pct"}
            dir={sortDir}
            onClick={onSortChange}
          />
          <SortButton
            label="Pop"
            sortKey="population"
            active={sortKey === "population"}
            dir={sortDir}
            onClick={onSortChange}
          />
          <SortButton
            label="A-Z"
            sortKey="name"
            active={sortKey === "name"}
            dir={sortDir}
            onClick={onSortChange}
          />
        </div>
      </div>

      {/* Pill toggle on a hairline — centered between summary and controls */}
      <div className="relative my-3">
        <div className="absolute inset-0 flex items-center" aria-hidden="true">
          <div className="w-full border-t border-foreground/8" />
        </div>
        <div className="relative flex justify-center">
          <button
            onClick={() => setOpen(!open)}
            className="inline-flex items-center gap-1 px-3 py-0.5 text-[11px] text-muted-foreground bg-background rounded-full border border-foreground/10 hover:text-foreground hover:border-foreground/20 transition-colors cursor-pointer"
          >
            {open ? (
              <>
                Hide
                <ChevronUp className="h-3 w-3" />
              </>
            ) : (
              <>
                Settings
                <ChevronDown className="h-3 w-3" />
              </>
            )}
          </button>
        </div>
      </div>

      {/* Expandable controls */}
      {open && (
        <div className="pb-2">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-x-10 gap-y-4">
            {/* Left column: time + metric */}
            <div className="space-y-3">
              <div className="space-y-1.5">
                <p className="text-xs font-medium text-muted-foreground">
                  Time of day
                </p>
                <div
                  className="flex gap-1"
                  role="group"
                  aria-label="Time window"
                >
                  {TIME_WINDOWS.map((tw) => (
                    <Button
                      key={tw.key}
                      variant={timeWindow === tw.key ? "default" : "outline"}
                      size="sm"
                      className="flex-1 px-0 h-auto py-1.5 flex-col gap-0"
                      onClick={() => onTimeWindowChange(tw.key)}
                      aria-pressed={timeWindow === tw.key}
                    >
                      <span className="text-[11px] leading-tight font-medium">
                        {tw.label}
                      </span>
                      <span className="text-[9px] leading-tight opacity-70 font-normal">
                        {tw.range}
                      </span>
                    </Button>
                  ))}
                </div>
              </div>

              <div className="space-y-1.5">
                <p className="text-xs font-medium text-muted-foreground">
                  Frequency metric
                </p>
                <div className="flex gap-1" role="group">
                  <TooltipProvider>
                    <Tooltip>
                      <TooltipTrigger
                        render={
                          <Button
                            variant={
                              routeMode === "aggregate" ? "default" : "outline"
                            }
                            size="sm"
                            className="flex-1 px-0 h-auto py-1.5 flex-col gap-0"
                            onClick={() => onRouteModeChange("aggregate")}
                            aria-pressed={routeMode === "aggregate"}
                          />
                        }
                      >
                        <span className="text-[11px] leading-tight font-medium">
                          All buses
                        </span>
                        <span className="text-[9px] leading-tight opacity-70 font-normal">
                          combined freq
                        </span>
                      </TooltipTrigger>
                      <TooltipContent side="bottom">
                        How often any bus arrives — all routes combined.
                      </TooltipContent>
                    </Tooltip>
                    <Tooltip>
                      <TooltipTrigger
                        render={
                          <Button
                            variant={
                              routeMode === "headway" ? "default" : "outline"
                            }
                            size="sm"
                            className="flex-1 px-0 h-auto py-1.5 flex-col gap-0"
                            onClick={() => onRouteModeChange("headway")}
                            aria-pressed={routeMode === "headway"}
                          />
                        }
                      >
                        <span className="text-[11px] leading-tight font-medium">
                          Your route
                        </span>
                        <span className="text-[9px] leading-tight opacity-70 font-normal">
                          longest wait
                        </span>
                      </TooltipTrigger>
                      <TooltipContent side="bottom">
                        Longest wait for your specific route.
                      </TooltipContent>
                    </Tooltip>
                  </TooltipProvider>
                </div>
                {headwayUnavailable && routeMode === "headway" && (
                  <p className="text-[10px] text-destructive/80 mt-1">
                    Route-level data unavailable
                  </p>
                )}
              </div>
            </div>

            {/* Right column: sliders */}
            <div className="space-y-3">
              <FrequencySlider
                axes={axes.frequency_minutes}
                value={freqIdx}
                defaultValue={defaults.frequency_idx}
                onChange={onFreqChange}
                routeMode={routeMode}
              />
              <WalkingTimeSlider
                axes={axes.walking_minutes}
                value={walkIdx}
                defaultValue={defaults.walking_idx}
                onChange={onWalkChange}
              />
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
