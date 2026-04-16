import { RotateCcw } from "lucide-react";
import { Slider } from "@/components/ui/slider";
import type { RouteMode } from "@/lib/types";

interface FrequencySliderProps {
  axes: readonly number[];
  value: number;
  defaultValue: number;
  onChange: (idx: number) => void;
  routeMode?: RouteMode;
}

export default function FrequencySlider({
  axes,
  value,
  defaultValue,
  onChange,
  routeMode = "aggregate",
}: FrequencySliderProps) {
  const changed = value !== defaultValue;
  const minutes = axes[value];
  const label =
    routeMode === "headway"
      ? `Frequent = wait ≤ ${minutes} min for your route`
      : `Frequent = every ${minutes} min or better`;
  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between min-h-[1.5rem]">
        <label className="text-sm font-medium text-foreground">{label}</label>
        <button
          onClick={() => onChange(defaultValue)}
          className={`ml-2 shrink-0 rounded p-0.5 text-muted-foreground hover:text-foreground transition-all ${
            changed
              ? "opacity-100 cursor-pointer"
              : "opacity-0 pointer-events-none"
          }`}
          aria-label="Reset frequency"
          tabIndex={changed ? 0 : -1}
        >
          <RotateCcw className="h-3 w-3" />
        </button>
      </div>
      <Slider
        min={0}
        max={axes.length - 1}
        step={1}
        value={[value]}
        onValueChange={(v) => onChange(Array.isArray(v) ? v[0] : v)}
        getAriaValueText={() =>
          routeMode === "headway"
            ? `wait at most ${axes[value]} minutes for your route`
            : `every ${axes[value]} minutes or better`
        }
      />
      <div className="relative mt-1 h-4" aria-hidden="true">
        {axes.map((v, i) => {
          const pct = (i / (axes.length - 1)) * 100;
          const align =
            i === 0
              ? ""
              : i === axes.length - 1
                ? "-translate-x-full"
                : "-translate-x-1/2";
          return (
            <span
              key={v}
              className={`absolute text-[10px] ${align} ${
                i === value
                  ? "font-semibold text-foreground"
                  : "text-muted-foreground"
              }`}
              style={{ left: `${pct}%` }}
            >
              {v}
            </span>
          );
        })}
      </div>
    </div>
  );
}
