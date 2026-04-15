import { lazy, Suspense, useState, useCallback } from "react";
import { ErrorBoundary } from "@/lib/ErrorBoundary";
import MapSkeleton from "./MapSkeleton";
import MapFallback from "./MapFallback";
import type { GridSchema } from "@/lib/types";

const LazyMap = lazy(() => import("./MapInner"));

interface MapViewProps {
  data: GridSchema;
  freqIdx: number;
  walkIdx: number;
  isDark: boolean;
}

export default function MapView({
  data,
  freqIdx,
  walkIdx,
  isDark,
}: MapViewProps) {
  const [runtimeError, setRuntimeError] = useState(false);
  const handleError = useCallback(() => setRuntimeError(true), []);

  if (runtimeError) {
    return <MapFallback data={data} />;
  }

  return (
    <ErrorBoundary fallback={<MapFallback data={data} />}>
      <Suspense fallback={<MapSkeleton data={data} />}>
        <LazyMap
          data={data}
          freqIdx={freqIdx}
          walkIdx={walkIdx}
          isDark={isDark}
          onError={handleError}
        />
      </Suspense>
    </ErrorBoundary>
  );
}
