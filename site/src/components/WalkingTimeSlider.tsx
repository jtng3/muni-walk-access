import { RotateCcw } from "lucide-react";
import { Slider } from "@/components/ui/slider";

interface WalkingTimeSliderProps {
  axes: readonly number[];
  value: number;
  defaultValue: number;
  onChange: (idx: number) => void;
}

const MILE_FRACTIONS: Record<number, string> = {
  5: "1/4",
  10: "1/2",
  15: "3/4",
};

function formatMiles(minutes: number): string {
  return MILE_FRACTIONS[minutes] ?? (minutes / 20).toFixed(2).replace(/0$/, "");
}

function formatMilesAria(minutes: number): string {
  const frac = MILE_FRACTIONS[minutes];
  if (frac) return `${frac} mile`;
  return `${(minutes / 20).toFixed(2).replace(/0$/, "")} miles`;
}

export default function WalkingTimeSlider({
  axes,
  value,
  defaultValue,
  onChange,
}: WalkingTimeSliderProps) {
  const minutes = axes[value];
  const changed = value !== defaultValue;
  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between min-h-[1.5rem]">
        <label className="text-sm font-medium text-foreground">
          {minutes} min walk &middot; {formatMiles(minutes)} mi
        </label>
        <button
          onClick={() => onChange(defaultValue)}
          className={`ml-2 shrink-0 rounded p-0.5 text-muted-foreground hover:text-foreground transition-all ${
            changed
              ? "opacity-100 cursor-pointer"
              : "opacity-0 pointer-events-none"
          }`}
          aria-label="Reset walking time"
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
          `${minutes} minute walk, approximately ${formatMilesAria(minutes)}`
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
