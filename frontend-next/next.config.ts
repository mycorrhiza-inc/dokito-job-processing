import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // For Docker deployment as SPA, use standalone output only in production
  output: process.env.NODE_ENV === 'production' ? 'standalone' : undefined,
  images: {
    unoptimized: true,
  },
  env: {
    API_URL: process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8080/api/v1',
  },
  // Enable hot reloading in development
  experimental: {
    turbo: {
      // Enable turbopack for faster hot reloading
      rules: {},
    },
  },
};

export default nextConfig;
