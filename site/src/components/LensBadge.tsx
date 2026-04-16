import type { LensFlags } from "@/lib/types";
import { Badge } from "@/components/ui/badge";
import {
  Tooltip,
  TooltipTrigger,
  TooltipContent,
  TooltipProvider,
} from "@/components/ui/tooltip";
import { Scale, Target } from "lucide-react";

interface LensBadgeProps {
  flags: LensFlags;
}

function LensChip({
  label,
  abbr,
  icon: Icon,
  className,
}: {
  label: string;
  abbr: string;
  icon: React.ComponentType<{ className?: string }>;
  className: string;
}) {
  return (
    <TooltipProvider delay={0}>
      <Tooltip>
        <TooltipTrigger
          render={<span />}
          role="img"
          aria-label={label}
          tabIndex={0}
          className="cursor-default rounded-4xl focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none"
        >
          <Badge className={className}>
            <Icon className="size-3" />
            {abbr}
          </Badge>
        </TooltipTrigger>
        <TooltipContent>{label}</TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}

export default function LensBadge({ flags }: LensBadgeProps) {
  const safeFlags: LensFlags = flags || {
    analysis_neighborhoods: false,
    ej_communities: false,
    equity_strategy: false,
  };

  const hasEJ = safeFlags.ej_communities;
  const hasESN = safeFlags.equity_strategy;

  if (!hasEJ && !hasESN) return null;

  return (
    <span className="inline-flex items-center gap-1">
      {hasEJ && (
        <LensChip
          label="Environmental Justice Community"
          abbr="EJ"
          icon={Scale}
          className="border-transparent bg-destructive/10 text-destructive gap-1"
        />
      )}
      {hasESN && (
        <LensChip
          label="SFMTA Equity Strategy Neighborhood"
          abbr="ESN"
          icon={Target}
          className="border-transparent bg-[color:oklch(0.666_0.179_58.318/0.15)] text-[color:oklch(0.47_0.13_58)] gap-1"
        />
      )}
    </span>
  );
}
