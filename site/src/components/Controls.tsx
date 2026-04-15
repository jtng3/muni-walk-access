import { useState } from "react";
import { ChevronDown, Settings2 } from "lucide-react";
import type { GridAxes, GridDefaults } from "@/lib/types";
import { Separator } from "@/components/ui/separator";
import FrequencySlider from "./FrequencySlider";
import WalkingTimeSlider from "./WalkingTimeSlider";
import ThemeToggle from "./ThemeToggle";

const MILE_FRACTIONS: Record<number, string> = {
  5: "\u00BC",
  10: "\u00BD",
  15: "\u00BE",
};

function formatMiles(minutes: number): string {
  return MILE_FRACTIONS[minutes] ?? `${(minutes / 20).toFixed(1)}`;
}

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
}: ControlsProps) {
  const [open, setOpen] = useState(true);
  const formattedPct = Math.round(pct * 100);
  const walkMiles = formatMiles(walkMin);

  if (!open) {
    return (
      <button
        onClick={() => setOpen(true)}
        className="absolute bottom-4 left-4 z-10 flex items-center gap-2 rounded-lg bg-card/80 backdrop-blur-md border border-border px-3 py-2 shadow-lg hover:bg-card/90 transition-colors cursor-pointer"
        aria-label="Open controls"
      >
        <span className="text-lg font-semibold text-foreground">
          {formattedPct}%
        </span>
        <span className="text-xs text-muted-foreground">
          {freqMin}min &middot; {walkMiles}mi
        </span>
        <Settings2 className="ml-1 h-3.5 w-3.5 text-muted-foreground" />
      </button>
    );
  }

  return (
    <div className="absolute bottom-4 left-4 z-10 w-72 max-w-[calc(100vw-2rem)] rounded-lg bg-card/80 backdrop-blur-md border border-border p-4 shadow-lg space-y-3">
      {/* Compact headline + collapse button */}
      <div className="flex items-start justify-between">
        <div>
          <span className="text-3xl font-bold tabular-nums text-foreground">
            {formattedPct}%
          </span>
          <p className="text-xs text-muted-foreground mt-0.5">
            every {freqMin} min &middot; {walkMiles} mi walk
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
    </div>
  );
}
