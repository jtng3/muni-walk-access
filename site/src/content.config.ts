import { defineCollection } from "astro:content";
import { z } from "astro/zod";
import { glob } from "astro/loaders";

const neighborhoods = defineCollection({
  loader: glob({ pattern: "**/*.mdx", base: "./src/content/neighborhoods" }),
  schema: z.object({
    name: z.string(),
    lens_flags: z.record(z.string(), z.boolean()),
    counterintuitive: z.boolean(),
  }),
});

export const collections = { neighborhoods };
