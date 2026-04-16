// @ts-check
import { defineConfig } from "astro/config";

import react from "@astrojs/react";

import tailwindcss from "@tailwindcss/vite";

import mdx from "@astrojs/mdx";

// https://astro.build/config
export default defineConfig({
  integrations: [react(), mdx()],

  // Path alias required by shadcn/ui
  vite: {
    plugins: [tailwindcss()],
    resolve: {
      alias: {
        "@": new URL("./src", import.meta.url).pathname,
      },
    },
  },
});
