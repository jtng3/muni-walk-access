// Build config for Vercel deployment. Everything here is the single source of truth for build behavior. Dashboard is used only for Root Directory, Deployment Protection, and secret values -- all one-time setup.
import { routes, type VercelConfig } from "@vercel/config/v1";

export const config: VercelConfig = {
  framework: "astro",
  installCommand: "npm install",
  buildCommand: "npm run build",
  outputDirectory: "dist",
  headers: [
    routes.cacheControl("/data/(.*\\.geojson)", {
      public: true,
      maxAge: "1 hour",
      sMaxAge: "1 hour",
    }),
    routes.cacheControl("/downloads/(.*)", {
      public: true,
      maxAge: "1 day",
      sMaxAge: "1 day",
    }),
    routes.cacheControl("/tiles/(.*\\.pmtiles)", {
      public: true,
      maxAge: "1 week",
      immutable: true,
    }),
    routes.cacheControl("/_astro/(.*)", {
      public: true,
      maxAge: "1 year",
      immutable: true,
    }),
    routes.cacheControl("/fonts/(.*)", {
      public: true,
      maxAge: "1 year",
      immutable: true,
    }),
  ],
};
