import { Slider } from "@/components/ui/slider";
import { Button } from "@/components/ui/button";

interface FrequencySliderProps {
  axes: readonly number[];
  value: number;
  defaultValue: number;
  onChange: (idx: number) => void;
}

export default function FrequencySlider({
  axes,
  value,
  defaultValue,
  onChange,
}: FrequencySliderProps) {
  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between">
        <label className="text-sm font-medium text-foreground">
          Frequent = every {axes[value]} min or better
        </label>
        {value !== defaultValue && (
          <Button
            variant="ghost"
            size="sm"
            onClick={() => onChange(defaultValue)}
          >
            Reset
          </Button>
        )}
      </div>
      <Slider
        min={0}
        max={axes.length - 1}
        step={1}
        value={[value]}
        onValueChange={(v) => onChange(Array.isArray(v) ? v[0] : v)}
        getAriaValueText={() => `every ${axes[value]} minutes or better`}
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
      <p className="text-xs text-muted-foreground">
        Frequency: what counts as a frequent bus? Drag to change.
      </p>
    </div>
  );
}
