import type { GridSchema } from "@/lib/types";
import NeighborhoodNameList from "./NeighborhoodNameList";

interface MapSkeletonProps {
  data: GridSchema;
}

export default function MapSkeleton({ data }: MapSkeletonProps) {
  return (
    <div className="h-full w-full">
      <div className="h-[70%] animate-pulse rounded-lg bg-muted" />
      <NeighborhoodNameList data={data} />
    </div>
  );
}
