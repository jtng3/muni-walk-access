import type { ConfigSnapshot } from "@/lib/types";

interface DataSourceDatesProps {
  config: ConfigSnapshot;
  className?: string;
}

function fmt(raw: string | undefined): string | null {
  if (!raw) return null;
  const d = new Date(raw);
  if (isNaN(d.getTime())) return null;
  return d.toLocaleDateString("en-US", {
    year: "numeric",
    month: "short",
    day: "numeric",
  });
}

function fmtYmd(raw: string | undefined): string | null {
  if (!raw || raw.length !== 8) return null;
  return fmt(`${raw.slice(0, 4)}-${raw.slice(4, 6)}-${raw.slice(6, 8)}`);
}

export default function DataSourceDates({
  config,
  className = "",
}: DataSourceDatesProps) {
  const dv = config?.data_versions;
  if (!dv) return null;

  const gtfs = fmt(dv.gtfs_feed_date);
  const addresses = fmt(dv.datasf_data_updated?.["ramy-di5m"]);
  const osm = fmtYmd(dv.osm_extract_date);

  return (
    <span className={`text-[10px] text-muted-foreground/70 ${className}`}>
      GTFS{gtfs && ` ${gtfs}`} · Addresses{addresses && ` ${addresses}`} · OSM
      {osm && ` ${osm}`}
    </span>
  );
}
