FROM node:22-bookworm-slim

ENV NODE_ENV=production \
    PORT=3009 \
    STORAGE=local \
    DB_STORE=file \
    DATA_DIR=/data \
    MAX_FILE_SIZE=52428800

WORKDIR /app
COPY --chown=node:node server.js ./server.js
RUN mkdir -p /data/files && chown -R node:node /app /data
USER node
EXPOSE 3009

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:3009/').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"

CMD ["node", "/app/server.js"]
