import type { GridSchema } from "@/lib/types";

interface NeighborhoodNameListProps {
  data: GridSchema;
}

export default function NeighborhoodNameList({
  data,
}: NeighborhoodNameListProps) {
  const sorted = [...data.neighborhoods]
    .map((n) => n.name)
    .sort((a, b) => a.localeCompare(b));

  return (
    <ul className="mt-4 columns-2 gap-4 text-sm text-muted-foreground">
      {sorted.map((name) => (
        <li key={name}>{name}</li>
      ))}
    </ul>
  );
}
