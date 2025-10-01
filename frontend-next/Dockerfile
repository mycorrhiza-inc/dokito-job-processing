# Use Node.js 18 as base with explicit platform support
FROM --platform=$BUILDPLATFORM node:18 AS base

# Install dependencies only when needed
FROM base AS deps
ARG TARGETPLATFORM
ARG BUILDPLATFORM
RUN echo "Building for $TARGETPLATFORM on $BUILDPLATFORM"
RUN apt-get update && apt-get install -y python3 make g++ && rm -rf /var/lib/apt/lists/*
WORKDIR /app

# Copy package files
COPY package.json package-lock.json* ./

# Install all dependencies first (including dev dependencies for build)
RUN --mount=type=cache,target=/root/.npm \
    npm ci --no-audit --no-fund

# Force reinstall platform-specific lightningcss package
RUN npm uninstall lightningcss-darwin-arm64 || true
RUN npm rebuild lightningcss

# Verify installation
RUN node -e "console.log('LightningCSS check:'); try { require('lightningcss'); console.log('✓ LightningCSS loaded successfully'); } catch (e) { console.error('✗ LightningCSS failed:', e.message); process.exit(1); }"

# Build the application
FROM base AS builder
ARG TARGETPLATFORM
ARG BUILDPLATFORM
RUN echo "Building for $TARGETPLATFORM on $BUILDPLATFORM"
RUN apt-get update && apt-get install -y python3 make g++ && rm -rf /var/lib/apt/lists/*
WORKDIR /app

# Copy package files
COPY package.json package-lock.json* ./

# Install dependencies fresh for build stage
RUN --mount=type=cache,target=/root/.npm \
    npm ci --no-audit --no-fund

# Force rebuild native modules for target platform
RUN npm rebuild

# Copy source code
COPY . .

# Set environment variables for build
ENV NEXT_TELEMETRY_DISABLED=1
ENV NODE_ENV=production

# Verify LightningCSS installation
RUN node -e "console.log('Build stage LightningCSS check:'); try { require('lightningcss'); console.log('✓ LightningCSS available'); } catch (e) { console.error('✗ LightningCSS failed:', e.message); process.exit(1); }"

# Build the Next.js application
RUN npm run build

# Development image - use Node.js with hot reload
FROM base AS development

ARG TARGETPLATFORM
ARG BUILDPLATFORM
RUN apt-get update && apt-get install -y python3 make g++ && rm -rf /var/lib/apt/lists/*
WORKDIR /app

# Copy package files
COPY package.json package-lock.json* ./
RUN --mount=type=cache,target=/root/.npm \
    npm ci --no-audit --no-fund
RUN npm rebuild

# Don't run development as root
RUN addgroup --system --gid 1001 nodejs
RUN adduser --system --uid 1001 --home /home/nextjs --shell /bin/bash nextjs
RUN mkdir -p /home/nextjs

# Change ownership of working directory and home directory
RUN chown -R nextjs:nodejs /app
RUN chown -R nextjs:nodejs /home/nextjs

USER nextjs

# Expose port
EXPOSE 3000

ENV PORT=3000
ENV NODE_ENV=development

# Start the Next.js development server
CMD ["npm", "run", "dev"]

# Production image - use Node.js for standalone mode
FROM base AS production

WORKDIR /app

# Don't run production as root
RUN addgroup --system --gid 1001 nodejs
RUN adduser --system --uid 1001 nextjs

# Copy the standalone output
COPY --from=builder /app/.next/standalone ./
COPY --from=builder /app/.next/static ./.next/static
COPY --from=builder /app/public ./public

# Change ownership
COPY --from=builder --chown=nextjs:nodejs /app/.next/standalone ./ 

USER nextjs

# Expose port
EXPOSE 3000

ENV PORT=3000
ENV HOSTNAME="0.0.0.0"

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:3000/health.html || exit 1

# Start the Next.js server
CMD ["node", "server.js"]
