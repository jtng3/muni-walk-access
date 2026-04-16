import { useState, useCallback } from "react";
import { Button } from "@/components/ui/button";

interface CopyUrlButtonProps {
  neighborhoodId: string;
}

export default function CopyUrlButton({ neighborhoodId }: CopyUrlButtonProps) {
  const [copied, setCopied] = useState(false);

  const handleCopy = useCallback(async () => {
    const url = `${window.location.origin}${window.location.pathname}#n=${neighborhoodId}`;
    try {
      await navigator.clipboard.writeText(url);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      // Clipboard API fails in insecure contexts or denied permission.
      // Fail silently — the URL is still visible in the address bar.
    }
  }, [neighborhoodId]);

  return (
    <Button
      variant="ghost"
      size="sm"
      onClick={handleCopy}
      aria-label={copied ? "Link copied" : `Copy link to ${neighborhoodId}`}
    >
      {copied ? "Copied!" : "Copy link"}
    </Button>
  );
}
