import { Switch } from "@/components/ui/switch";

interface ThemeToggleProps {
  isDark: boolean;
  onToggle: () => void;
}

export default function ThemeToggle({ isDark, onToggle }: ThemeToggleProps) {
  return (
    <div className="flex items-center justify-between">
      <label
        className="text-xs font-medium text-muted-foreground"
        htmlFor="theme-toggle"
      >
        Dark mode
      </label>
      <Switch
        id="theme-toggle"
        checked={isDark}
        onCheckedChange={onToggle}
        aria-label="Toggle dark mode"
      />
    </div>
  );
}
