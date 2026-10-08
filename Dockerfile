# Debian (glibc) rather than Alpine: DuckDB's S3 extensions are published for glibc.
FROM node:24-bookworm-slim@sha256:d6aa754f16b3197301076f047b5def2f02ea1dbbc2ca920407d46d7ec7f87b20 AS builder
WORKDIR /lake
COPY package.json package-lock.json ./
RUN npm ci
COPY tsconfig.json ./
COPY src src
COPY config config
RUN npm run build && npm prune --omit=dev

FROM node:24-bookworm-slim@sha256:d6aa754f16b3197301076f047b5def2f02ea1dbbc2ca920407d46d7ec7f87b20
WORKDIR /lake
COPY --from=builder /lake/node_modules node_modules
COPY --from=builder /lake/lib lib
COPY config config
COPY package.json ./

USER node
# The S3 extensions are installed at build time, so the portal does not download them at start.
RUN node -e "require('@duckdb/node-api').DuckDBInstance.create(':memory:').then((i) => i.connect()).then((c) => c.run('INSTALL httpfs; INSTALL aws;'))"

ENV PORT=8100
EXPOSE 8100
# live (the default) follows the chains in DATASETS and serves them; backfill, discover, follow and
# serve run the other parts on their own: `docker run <image> lib/backfill.js`.
ENTRYPOINT ["node"]
CMD ["lib/live.js"]
