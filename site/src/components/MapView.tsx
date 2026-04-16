import { lazy, Suspense, useState, useCallback } from "react";
import { ErrorBoundary } from "@/lib/ErrorBoundary";
import MapSkeleton from "./MapSkeleton";
import MapFallback from "./MapFallback";
import type { GridSchema, HexGridSchema } from "@/lib/types";
import type { DevFlags } from "./DevOverlay";

const LazyMap = lazy(() => import("./MapInner"));

interface MapViewProps {
  data: GridSchema;
  freqIdx: number;
  walkIdx: number;
  isDark: boolean;
  devFlags: DevFlags;
  viewMode: "summary" | "detailed";
  hexBaseFC: GeoJSON.FeatureCollection | null;
  hexData: HexGridSchema | null;
}

export default function MapView({
  data,
  freqIdx,
  walkIdx,
  isDark,
  devFlags,
  viewMode,
  hexBaseFC,
  hexData,
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
          devFlags={devFlags}
          onError={handleError}
          viewMode={viewMode}
          hexBaseFC={hexBaseFC}
          hexData={hexData}
        />
      </Suspense>
    </ErrorBoundary>
  );
}
