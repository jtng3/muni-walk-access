import type { LensFlags } from "@/lib/types";
import { Badge } from "@/components/ui/badge";
import {
  Tooltip,
  TooltipTrigger,
  TooltipContent,
  TooltipProvider,
} from "@/components/ui/tooltip";

const LENS_LABELS: Record<keyof LensFlags, string> = {
  analysis_neighborhoods: "Analysis Neighborhoods",
  ej_communities: "Environmental Justice Communities",
  equity_strategy: "SFMTA Equity Strategy Neighborhoods",
};

const VARIANT_CLASS: Record<number, string> = {
  0: "border-border bg-transparent text-foreground",
  1: "border-transparent bg-muted text-foreground",
  2: "border-transparent bg-[color:oklch(0.666_0.179_58.318/0.15)] text-[color:oklch(0.47_0.13_58)]",
  3: "border-transparent bg-destructive/10 text-destructive",
};

interface LensBadgeProps {
  flags: LensFlags;
}

export default function LensBadge({ flags }: LensBadgeProps) {
  const safeFlags: LensFlags = flags || {
    analysis_neighborhoods: false,
    ej_communities: false,
    equity_strategy: false,
  };

  const lensKeys = Object.keys(LENS_LABELS) as (keyof LensFlags)[];
  const total = lensKeys.length;
  const flaggedLenses = lensKeys.filter((key) => safeFlags[key]);
  const count = flaggedLenses.length;
  const flaggedNames = flaggedLenses.map((key) => LENS_LABELS[key]);

  const tooltipText =
    count === 0
      ? "Not flagged by any equity lens"
      : `Flagged by: ${flaggedNames.join(", ")}`;

  const ariaLabel =
    count === 0
      ? `0 of ${total} equity lenses: Not flagged by any equity lens`
      : `${count} of ${total} equity lenses: ${flaggedNames.join(", ")}`;

  return (
    <TooltipProvider delay={0}>
      <Tooltip>
        <TooltipTrigger
          render={<span />}
          role="img"
          aria-label={ariaLabel}
          tabIndex={0}
          className="cursor-default rounded-4xl focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none"
        >
          <Badge className={VARIANT_CLASS[count]}>
            {count} of {total}
          </Badge>
        </TooltipTrigger>
        <TooltipContent>{tooltipText}</TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}
