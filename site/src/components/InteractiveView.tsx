import { useMemo, useState } from "react";
import { useUrlState } from "@/lib/useUrlState";
import { useTheme } from "@/lib/useTheme";
import type { GridSchema } from "@/lib/types";
import Controls from "./Controls";
import MapView from "./MapView";
import DevOverlay, { DEFAULT_DEV_FLAGS } from "./DevOverlay";
import type { DevFlags } from "./DevOverlay";

interface InteractiveViewProps {
  data: GridSchema;
}

export default function InteractiveView({ data }: InteractiveViewProps) {
  const totalAddresses = useMemo(
    () => data.neighborhoods.reduce((sum, n) => sum + n.population, 0),
    [data.neighborhoods],
  );
  const [freqIdx, setFreqIdx] = useUrlState(
    "freq",
    data.defaults.frequency_idx,
    0,
    data.axes.frequency_minutes.length - 1,
  );
  const [walkIdx, setWalkIdx] = useUrlState(
    "walk",
    data.defaults.walking_idx,
    0,
    data.axes.walking_minutes.length - 1,
  );
  const [theme, toggleTheme] = useTheme();
  const isDark = theme === "dark";
  const [devFlags, setDevFlags] = useState<DevFlags>(DEFAULT_DEV_FLAGS);

  const pct = data.city_wide.pct_within[freqIdx][walkIdx];
  const freqMin = data.axes.frequency_minutes[freqIdx];
  const walkMin = data.axes.walking_minutes[walkIdx];

  return (
    <div className="relative" style={{ height: "calc(100dvh - 3rem)" }}>
      <div className="absolute inset-0">
        <MapView
          data={data}
          freqIdx={freqIdx}
          walkIdx={walkIdx}
          isDark={isDark}
          devFlags={devFlags}
        />
      </div>
      <Controls
        axes={data.axes}
        defaults={data.defaults}
        freqIdx={freqIdx}
        walkIdx={walkIdx}
        onFreqChange={setFreqIdx}
        onWalkChange={setWalkIdx}
        isDark={isDark}
        onThemeToggle={toggleTheme}
        pct={pct}
        freqMin={freqMin}
        walkMin={walkMin}
        totalAddresses={totalAddresses}
      />
      <DevOverlay flags={devFlags} onChange={setDevFlags} />
    </div>
  );
}
