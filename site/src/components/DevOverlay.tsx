import { useState } from "react";
import { ChevronDown, Wrench } from "lucide-react";
import { Switch } from "@/components/ui/switch";

export interface DevFlags {
  terrain: boolean;
  fog: boolean;
  buildings3d: boolean;
  glowBorders: boolean;
  buildingGlow: boolean;
}

export const DEFAULT_DEV_FLAGS: DevFlags = {
  terrain: true,
  fog: false, // WIP — not visually working yet
  buildings3d: true,
  glowBorders: false, // WIP — needs tuning
  buildingGlow: false, // WIP — needs deck.gl for true gradient
};

interface DevOverlayProps {
  flags: DevFlags;
  onChange: (flags: DevFlags) => void;
}

const FLAG_LABELS: { key: keyof DevFlags; label: string }[] = [
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
        className="absolute bottom-4 right-4 z-10 rounded-lg bg-card/80 backdrop-blur-md border border-border p-2 shadow-lg hover:bg-card/90 transition-colors cursor-pointer"
        aria-label="Open dev tools"
      >
        <Wrench className="h-4 w-4 text-muted-foreground" />
      </button>
    );
  }

  return (
    <div className="absolute bottom-4 right-4 z-10 w-52 rounded-lg bg-card/80 backdrop-blur-md border border-border p-3 shadow-lg space-y-2">
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
    </div>
  );
}
