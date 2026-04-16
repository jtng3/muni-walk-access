import { useState, useEffect, useCallback } from "react";

function parseHash(): URLSearchParams {
  const raw = window.location.hash.slice(1); // strip leading '#'
  return new URLSearchParams(raw);
}

function writeHash(params: URLSearchParams): void {
  const str = params.toString();
  // replaceState avoids polluting browser history during rapid slider drags.
  if (str) {
    window.history.replaceState(null, "", `#${str}`);
  } else {
    window.history.replaceState(
      null,
      "",
      window.location.pathname + window.location.search,
    );
  }
  // replaceState doesn't fire hashchange, so dispatch manually so other
  // React islands (map ↔ neighborhood section) stay in sync.
  window.dispatchEvent(new HashChangeEvent("hashchange"));
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

/**
 * Syncs a string value to a URL hash key.
 * Same semantics as useUrlState but for a fixed set of valid string values.
 */
export function useUrlStringState<T extends string>(
  key: string,
  defaultValue: T,
  validValues: readonly T[],
): [T, (v: T) => void] {
  const [value, setValueInternal] = useState<T>(() => {
    if (typeof window === "undefined") return defaultValue;
    const params = parseHash();
    const raw = params.get(key);
    if (raw === null) return defaultValue;
    return validValues.includes(raw as T) ? (raw as T) : defaultValue;
  });

  const setValue = useCallback(
    (v: T) => {
      if (!validValues.includes(v)) return;
      setValueInternal(v);
      const params = parseHash();
      if (v === defaultValue) {
        params.delete(key);
      } else {
        params.set(key, v);
      }
      writeHash(params);
    },
    [key, defaultValue, validValues],
  );

  useEffect(() => {
    const onHashChange = () => {
      const params = parseHash();
      const raw = params.get(key);
      if (raw === null) {
        setValueInternal(defaultValue);
      } else {
        setValueInternal(
          validValues.includes(raw as T) ? (raw as T) : defaultValue,
        );
      }
    };
    window.addEventListener("hashchange", onHashChange);
    return () => window.removeEventListener("hashchange", onHashChange);
  }, [key, defaultValue, validValues]);

  return [value, setValue];
}
