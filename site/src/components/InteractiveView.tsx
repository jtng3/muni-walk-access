import { useUrlState } from "@/lib/useUrlState";
import type { GridSchema } from "@/lib/types";
import HeadlineReactive from "./HeadlineReactive";
import Controls from "./Controls";
import MapView from "./MapView";

interface InteractiveViewProps {
  data: GridSchema;
}

export default function InteractiveView({ data }: InteractiveViewProps) {
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

  const pct = data.city_wide.pct_within[freqIdx][walkIdx];
  const freqMin = data.axes.frequency_minutes[freqIdx];
  const walkMin = data.axes.walking_minutes[walkIdx];

  return (
    <>
      <HeadlineReactive pct={pct} frequencyMin={freqMin} walkingMin={walkMin} />
      <Controls
        axes={data.axes}
        defaults={data.defaults}
        freqIdx={freqIdx}
        walkIdx={walkIdx}
        onFreqChange={setFreqIdx}
        onWalkChange={setWalkIdx}
      />
      <div className="mt-6 h-[60vh] min-h-[400px] rounded-lg overflow-hidden">
        <MapView data={data} freqIdx={freqIdx} walkIdx={walkIdx} />
      </div>
    </>
  );
}
