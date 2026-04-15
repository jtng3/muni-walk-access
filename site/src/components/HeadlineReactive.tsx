const MILE_FRACTIONS: Record<number, string> = {
  5: "1/4",
  10: "1/2",
  15: "3/4",
};

function formatMiles(minutes: number): string {
  return MILE_FRACTIONS[minutes] ?? (minutes / 20).toFixed(2).replace(/0$/, "");
}

interface HeadlineReactiveProps {
  pct: number;
  frequencyMin: number;
  walkingMin: number;
}

export default function HeadlineReactive({
  pct,
  frequencyMin,
  walkingMin,
}: HeadlineReactiveProps) {
  const formattedPct = Math.round(pct * 100);
  const walkMiles = formatMiles(walkingMin);

  return (
    <div className="space-y-4">
      <h1>
        <span className="block text-5xl font-semibold text-foreground">
          {formattedPct}%
        </span>
        <span className="mt-2 block text-xl text-foreground">
          of SF residents live within a {walkMiles}-mile / {walkingMin}-minute
          walk of a stop with a bus every {frequencyMin} minutes or better.
        </span>
      </h1>
      <p className="text-lg text-muted-foreground">
        This figure reflects residents within a {walkingMin}-minute walk (
        {walkMiles} mi) of a Muni stop served by a bus at least every{" "}
        {frequencyMin} minutes during the AM peak. These are specific
        assumptions about frequency and distance — not universal measures of
        transit access.
      </p>
    </div>
  );
}
