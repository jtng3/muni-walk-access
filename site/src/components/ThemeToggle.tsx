import { Switch } from "@/components/ui/switch";

interface ThemeToggleProps {
  isDark: boolean;
  onToggle: () => void;
}

export default function ThemeToggle({ isDark, onToggle }: ThemeToggleProps) {
  return (
    <div className="flex items-center justify-between">
      <span className="text-sm font-medium">Dark mode</span>
      <Switch
        checked={isDark}
        onCheckedChange={onToggle}
        size="sm"
        aria-label="Toggle dark mode"
      />
    </div>
  );
}
