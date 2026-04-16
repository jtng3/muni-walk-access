import type { LensFlags } from "@/lib/types";
import { VIRIDIS_STOPS } from "@/lib/choropleth";
import LensBadge from "./LensBadge";

interface NeighborhoodCardProps {
  id: string;
  name: string;
  pct: number;
  population: number;
  lensFlags: LensFlags;
}

/** Interpolate viridis color for a 0-1 percentage value. */
function viridisColor(pct: number): string {
  // Map pct (0..1) across the stops (index 6=0% → index 0=100%)
  const t = Math.max(0, Math.min(1, pct));
  const idx = (1 - t) * 6;
  const lo = Math.floor(idx);
  const hi = Math.min(lo + 1, 6);
  const frac = idx - lo;

  // Parse hex to RGB and lerp
  const parse = (hex: string) => [
    parseInt(hex.slice(1, 3), 16),
    parseInt(hex.slice(3, 5), 16),
    parseInt(hex.slice(5, 7), 16),
  ];
  const a = parse(VIRIDIS_STOPS[lo]);
  const b = parse(VIRIDIS_STOPS[hi]);
  const r = Math.round(a[0] + (b[0] - a[0]) * frac);
  const g = Math.round(a[1] + (b[1] - a[1]) * frac);
  const bl = Math.round(a[2] + (b[2] - a[2]) * frac);
  return `rgb(${r},${g},${bl})`;
}

export default function NeighborhoodCard({
  id,
  name,
  pct,
  population,
  lensFlags,
}: NeighborhoodCardProps) {
  const pctRounded = Math.round(pct * 100);
  const color = viridisColor(pct);

  return (
    <article
      id={`n-${id}`}
      className="flex flex-col gap-2 rounded-xl bg-card p-4 text-card-foreground ring-1 ring-foreground/10 shadow-sm"
    >
      {/* Hero: percentage + name */}
      <div className="flex items-baseline gap-2">
        <span className="text-2xl font-bold tabular-nums leading-none">
          {pctRounded}%
        </span>
        <h3 className="text-sm font-medium leading-tight min-w-0">{name}</h3>
      </div>

      {/* Viridis bar */}
      <div className="h-2 rounded-full bg-muted overflow-hidden">
        <div
          className="h-full rounded-full transition-all duration-300"
          style={{
            width: `${Math.max(pctRounded, 1)}%`,
            backgroundColor: color,
          }}
        />
      </div>

      {/* Population + badges */}
      <div className="flex items-center justify-between">
        <p className="text-xs text-muted-foreground">
          {population.toLocaleString("en-US")} residents
        </p>
        <LensBadge flags={lensFlags} />
      </div>
    </article>
  );
}
