import type { GridAxes, GridDefaults } from "@/lib/types";
import FrequencySlider from "./FrequencySlider";
import WalkingTimeSlider from "./WalkingTimeSlider";

interface ControlsProps {
  axes: GridAxes;
  defaults: GridDefaults;
  freqIdx: number;
  walkIdx: number;
  onFreqChange: (idx: number) => void;
  onWalkChange: (idx: number) => void;
}

export default function Controls({
  axes,
  defaults,
  freqIdx,
  walkIdx,
  onFreqChange,
  onWalkChange,
}: ControlsProps) {
  return (
    <div className="mx-auto max-w-md rounded-lg bg-muted/50 p-6 space-y-6">
      <FrequencySlider
        axes={axes.frequency_minutes}
        value={freqIdx}
        defaultValue={defaults.frequency_idx}
        onChange={onFreqChange}
      />
      <WalkingTimeSlider
        axes={axes.walking_minutes}
        value={walkIdx}
        defaultValue={defaults.walking_idx}
        onChange={onWalkChange}
      />
    </div>
  );
}
