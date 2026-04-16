import { useState } from "react";
import { ChevronDown, Wrench } from "lucide-react";
import { Switch } from "@/components/ui/switch";
import { Slider } from "@/components/ui/slider";

export interface DevFlags {
  terrain: boolean;
  fog: boolean;
  buildings3d: boolean;
  glowBorders: boolean;
  buildingGlow: boolean;
  fillOpacity: number;
}

export const DEFAULT_DEV_FLAGS: DevFlags = {
  terrain: false,
  fog: false,
  buildings3d: false,
  glowBorders: false,
  buildingGlow: false,
  fillOpacity: 0.65,
};

interface DevOverlayProps {
  flags: DevFlags;
  onChange: (flags: DevFlags) => void;
}

type BooleanDevFlag = {
  [K in keyof DevFlags]: DevFlags[K] extends boolean ? K : never;
}[keyof DevFlags];

const FLAG_LABELS: { key: BooleanDevFlag; label: string }[] = [
  { key: "terrain", label: "3D Terrain" },
  { key: "fog", label: "Fog" },
  { key: "buildings3d", label: "3D Buildings" },
  { key: "glowBorders", label: "Glow Borders" },
  { key: "buildingGlow", label: "Building Glow" },
];

export default function DevOverlay({ flags, onChange }: DevOverlayProps) {
  const [open, setOpen] = useState(false);

  if (!open) {
    return (
      <button
        onClick={() => setOpen(true)}
        className="absolute top-4 right-4 z-10 rounded-lg bg-card/60 backdrop-blur-md border border-border p-2 shadow-lg hover:bg-card/70 transition-colors cursor-pointer"
        aria-label="Open dev tools"
      >
        <Wrench className="h-4 w-4 text-muted-foreground" />
      </button>
    );
  }

  return (
    <div className="absolute top-4 right-4 z-10 w-52 rounded-lg bg-card/60 backdrop-blur-md border border-border p-3 shadow-lg space-y-2">
      <div className="flex items-center justify-between">
        <span className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
          Dev Tools
        </span>
        <button
          onClick={() => setOpen(false)}
          className="rounded-md p-1 text-muted-foreground hover:text-foreground hover:bg-muted/50 transition-colors cursor-pointer"
          aria-label="Collapse dev tools"
        >
          <ChevronDown className="h-3.5 w-3.5" />
        </button>
      </div>

      {FLAG_LABELS.map(({ key, label }) => (
        <div key={key} className="flex items-center justify-between">
          <span className="text-xs text-foreground">{label}</span>
          <Switch
            checked={flags[key]}
            onCheckedChange={() => onChange({ ...flags, [key]: !flags[key] })}
            size="sm"
          />
        </div>
      ))}

      <div className="space-y-1 pt-1">
        <div className="flex items-center justify-between">
          <span className="text-xs text-foreground">Fill Opacity</span>
          <span className="text-[10px] tabular-nums text-muted-foreground">
            {Math.round(flags.fillOpacity * 100)}%
          </span>
        </div>
        <Slider
          min={0}
          max={100}
          step={5}
          value={[Math.round(flags.fillOpacity * 100)]}
          onValueChange={(v) =>
            onChange({
              ...flags,
              fillOpacity: (Array.isArray(v) ? v[0] : v) / 100,
            })
          }
        />
      </div>
    </div>
  );
}
