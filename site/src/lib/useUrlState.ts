import { useState, useEffect, useCallback } from "react";

function parseHash(): URLSearchParams {
  const raw = window.location.hash.slice(1); // strip leading '#'
  return new URLSearchParams(raw);
}

function writeHash(params: URLSearchParams): void {
  const str = params.toString();
  // replaceState avoids polluting browser history during rapid slider drags.
  // pushState is called explicitly by consumers (e.g. onValueCommitted) when
  // a discrete history entry is desired.
  if (str) {
    window.history.replaceState(null, "", `#${str}`);
  } else {
    // Clean URL — remove the hash entirely
    window.history.replaceState(
      null,
      "",
      window.location.pathname + window.location.search,
    );
  }
}

/**
 * Syncs a numeric value to a URL hash key.
 *
 * - Reads from `window.location.hash` on mount (falls back to `defaultValue`).
 * - `setValue(v)` updates React state AND replaces the hash (no history entry).
 * - `hashchange` events (browser back/forward) re-read the key.
 * - Default values are NOT written to the hash (clean URL = default state).
 */
export function useUrlState(
  key: string,
  defaultValue: number,
  min = 0,
  max = Infinity,
): [number, (v: number) => void] {
  function clamp(n: number): number {
    return Number.isNaN(n) || !Number.isInteger(n)
      ? defaultValue
      : Math.max(min, Math.min(max, n));
  }

  const [value, setValueInternal] = useState(() => {
    if (typeof window === "undefined") return defaultValue;
    const params = parseHash();
    const raw = params.get(key);
    if (raw === null) return defaultValue;
    return clamp(Number(raw));
  });

  const setValue = useCallback(
    (v: number) => {
      const clamped = clamp(v);
      setValueInternal(clamped);
      const params = parseHash();
      if (clamped === defaultValue) {
        params.delete(key);
      } else {
        params.set(key, String(clamped));
      }
      writeHash(params);
    },
    [key, defaultValue, min, max],
  );

  useEffect(() => {
    const onHashChange = () => {
      const params = parseHash();
      const raw = params.get(key);
      if (raw === null) {
        setValueInternal(defaultValue);
      } else {
        setValueInternal(clamp(Number(raw)));
      }
    };
    window.addEventListener("hashchange", onHashChange);
    return () => window.removeEventListener("hashchange", onHashChange);
  }, [key, defaultValue, min, max]);

  return [value, setValue];
}
