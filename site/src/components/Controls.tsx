import { useState } from "react";
import { ChevronDown, Settings2 } from "lucide-react";
import type { GridAxes, GridDefaults, TimeWindowKey } from "@/lib/types";
import { TIME_WINDOWS } from "@/lib/types";
import { Separator } from "@/components/ui/separator";
import { Button } from "@/components/ui/button";
import { Slider } from "@/components/ui/slider";
import FrequencySlider from "./FrequencySlider";
import WalkingTimeSlider from "./WalkingTimeSlider";
import { Switch } from "@/components/ui/switch";
import ThemeToggle from "./ThemeToggle";

const MILE_FRACTIONS: Record<number, string> = {
  5: "\u00BC",
  10: "\u00BD",
  15: "\u00BE",
};

function formatMiles(minutes: number): string {
  return MILE_FRACTIONS[minutes] ?? `${(minutes / 20).toFixed(1)}`;
}

// Cell counts per resolution from pipeline output (addresses only)
const HEX_RES_CELLS: Record<number, number> = {
  7: 29,
  8: 161,
  9: 896,
  10: 5245,
  11: 28_709,
};

interface ControlsProps {
  axes: GridAxes;
  defaults: GridDefaults;
  freqIdx: number;
  walkIdx: number;
  onFreqChange: (idx: number) => void;
  onWalkChange: (idx: number) => void;
  isDark: boolean;
  onThemeToggle: () => void;
  pct: number;
  freqMin: number;
  walkMin: number;
  totalAddresses: number;
  viewMode: "summary" | "detailed";
  onViewModeChange: (mode: "summary" | "detailed") => void;
  hexRes: number;
  onHexResChange: (res: number) => void;
  hexLoading: boolean;
  failedResolutions: Set<number>;
  h3Failed: boolean;
  showLabels: boolean;
  onShowLabelsChange: (show: boolean) => void;
  timeWindow: TimeWindowKey;
  onTimeWindowChange: (tw: TimeWindowKey) => void;
}

export default function Controls({
  axes,
  defaults,
  freqIdx,
  walkIdx,
  onFreqChange,
  onWalkChange,
  isDark,
  onThemeToggle,
  pct,
  freqMin,
  walkMin,
  totalAddresses,
  viewMode,
  onViewModeChange,
  hexRes,
  onHexResChange,
  hexLoading,
  failedResolutions,
  h3Failed,
  showLabels,
  onShowLabelsChange,
  timeWindow,
  onTimeWindowChange,
}: ControlsProps) {
  const [open, setOpen] = useState(true);
  const formattedPct = Math.round(pct * 100);
  const walkMiles = formatMiles(walkMin);

  if (!open) {
    return (
      <button
        onClick={() => setOpen(true)}
        className="absolute bottom-4 left-4 z-10 flex items-center gap-2 rounded-lg bg-card/60 backdrop-blur-md border border-border px-3 py-2 shadow-lg hover:bg-card/70 transition-colors cursor-pointer"
        aria-label="Open controls"
      >
        <span className="text-lg font-semibold text-foreground">
          {formattedPct}%
        </span>
        <span className="text-xs text-muted-foreground">
          {TIME_WINDOWS.find((tw) => tw.key === timeWindow)?.label ??
            timeWindow}{" "}
          &middot; {freqMin}min &middot; {walkMiles}mi
        </span>
        <Settings2 className="ml-1 h-3.5 w-3.5 text-muted-foreground" />
      </button>
    );
  }

  const cellCount = HEX_RES_CELLS[hexRes];

  return (
    <div className="absolute bottom-4 left-4 z-10 w-72 max-w-[calc(100vw-2rem)] rounded-lg bg-card/60 backdrop-blur-md border border-border p-4 shadow-lg space-y-3">
      {/* Compact headline + collapse button */}
      <div className="flex items-start justify-between">
        <div>
          <span className="text-3xl font-bold tabular-nums text-foreground">
            {formattedPct}%
          </span>
          <p className="text-xs text-muted-foreground mt-0.5">
            every {freqMin} min &middot; {walkMiles} mi walk &middot;{" "}
            {TIME_WINDOWS.find((tw) => tw.key === timeWindow)?.long ??
              timeWindow}
          </p>
          <p className="text-[10px] text-muted-foreground/70 mt-0.5">
            {totalAddresses.toLocaleString()} addresses analyzed
          </p>
        </div>
        <button
          onClick={() => setOpen(false)}
          className="rounded-md p-1 text-muted-foreground hover:text-foreground hover:bg-muted/50 transition-colors cursor-pointer"
          aria-label="Collapse controls"
        >
          <ChevronDown className="h-4 w-4" />
        </button>
      </div>

      {/* Time-of-day window — primary control */}
      <div className="space-y-1.5">
        <p className="text-xs font-medium text-muted-foreground">Time of day</p>
        <div className="flex gap-1" role="group" aria-label="Time window">
          {TIME_WINDOWS.map((tw) => (
            <Button
              key={tw.key}
              variant={timeWindow === tw.key ? "default" : "outline"}
              size="sm"
              className="flex-1 px-0 h-auto py-1.5 flex-col gap-0"
              onClick={() => onTimeWindowChange(tw.key)}
              aria-pressed={timeWindow === tw.key}
              aria-label={`${tw.long} (${tw.range})`}
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

      <Separator />

      <FrequencySlider
        axes={axes.frequency_minutes}
        value={freqIdx}
        defaultValue={defaults.frequency_idx}
        onChange={onFreqChange}
      />
      <WalkingTimeSlider
        axes={axes.walking_minutes}
        value={walkIdx}
        defaultValue={defaults.walking_idx}
        onChange={onWalkChange}
      />

      <Separator />

      <ThemeToggle isDark={isDark} onToggle={onThemeToggle} />

      <div className="flex items-center justify-between">
        <label
          className="text-xs font-medium text-muted-foreground"
          htmlFor="show-labels"
        >
          Neighborhood names
        </label>
        <Switch
          id="show-labels"
          checked={showLabels}
          onCheckedChange={onShowLabelsChange}
        />
      </div>

      <Separator />

      {/* Summary / Detailed toggle */}
      <div className="space-y-2">
        <p className="text-xs font-medium text-muted-foreground">View</p>
        <div className="flex gap-2" role="group" aria-label="View mode">
          <Button
            variant={viewMode === "summary" ? "default" : "outline"}
            size="sm"
            className="flex-1"
            onClick={() => onViewModeChange("summary")}
            aria-pressed={viewMode === "summary"}
          >
            Summary
          </Button>
          <Button
            variant={viewMode === "detailed" ? "default" : "outline"}
            size="sm"
            className="flex-1"
            onClick={() => onViewModeChange("detailed")}
            aria-pressed={viewMode === "detailed"}
          >
            Detailed
          </Button>
        </div>
      </div>

      {/* Global h3 failure — disables all hex features */}
      {h3Failed && viewMode === "detailed" && (
        <p className="text-[10px] text-destructive/80">
          Hex visualization unavailable (h3-js failed to load)
        </p>
      )}

      {/* Resolution picker — only in Detailed mode when h3 is working */}
      {viewMode === "detailed" && !h3Failed && (
        <div className="space-y-2">
          <div className="flex items-center justify-between">
            <label className="text-sm font-medium text-foreground">
              Resolution {hexRes}
              {cellCount !== undefined && (
                <span className="font-normal text-muted-foreground">
                  {" "}
                  ({cellCount.toLocaleString()} hexes)
                </span>
              )}
            </label>
            {hexLoading && (
              <span className="text-xs text-muted-foreground">Loading…</span>
            )}
          </div>
          {hexRes === 10 && (
            <p className="text-[10px] text-muted-foreground/70">
              Large download (~5 MB)
            </p>
          )}
          {hexRes === 11 && (
            <p className="text-[10px] text-muted-foreground/70">
              Very large download (~35 MB)
            </p>
          )}
          {failedResolutions.has(hexRes) && (
            <p className="text-[10px] text-destructive/80">
              Run pipeline to generate r{hexRes} data
            </p>
          )}
          <Slider
            min={7}
            max={11}
            step={1}
            value={[hexRes]}
            onValueChange={(v) => {
              const res = Array.isArray(v) ? v[0] : v;
              if (res !== undefined && !failedResolutions.has(res)) {
                onHexResChange(res);
              }
            }}
            disabled={hexLoading}
            getAriaValueText={() => `Resolution ${hexRes}`}
          />
          {/* Resolution tick marks */}
          <div className="relative mt-1 h-4" aria-hidden="true">
            {[7, 8, 9, 10, 11].map((res) => {
              const idx = res - 7;
              const pct = (idx / 4) * 100;
              const align =
                idx === 0
                  ? ""
                  : idx === 4
                    ? "-translate-x-full"
                    : "-translate-x-1/2";
              const isFailed = failedResolutions.has(res);
              const isCurrent = res === hexRes;
              return (
                <span
                  key={res}
                  className={`absolute text-[10px] ${align} ${
                    isFailed
                      ? "line-through text-muted-foreground/50"
                      : isCurrent
                        ? "font-semibold text-foreground"
                        : "text-muted-foreground"
                  }`}
                  style={{ left: `${pct}%` }}
                >
                  {res}
                </span>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}
