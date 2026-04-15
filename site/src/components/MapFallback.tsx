import type { GridSchema } from "@/lib/types";
import NeighborhoodNameList from "./NeighborhoodNameList";

// TODO: set GitHub issues URL once repo is public
const ISSUES_URL = "https://github.com/jaegertang3/muni-walk-access/issues";

interface MapFallbackProps {
  data: GridSchema;
}

export default function MapFallback({ data }: MapFallbackProps) {
  return (
    <div className="flex h-full w-full flex-col rounded-lg bg-muted p-6">
      <p className="text-muted-foreground">
        Map failed to load. Try refreshing. If this persists, please{" "}
        <a
          href={ISSUES_URL}
          className="underline hover:text-foreground"
          target="_blank"
          rel="noopener noreferrer"
        >
          open an issue
        </a>
        .
      </p>
      <NeighborhoodNameList data={data} />
    </div>
  );
}
