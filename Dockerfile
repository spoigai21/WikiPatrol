# One image for every service; the command picks which (compose and Kubernetes set it).
FROM node:26-slim
WORKDIR /app
ENV NODE_ENV=production KAFKAJS_NO_PARTITIONER_WARNING=1
COPY package.json package-lock.json ./
# tsx runs the TypeScript directly, so it ships (it is a dev dependency).
RUN npm ci --include=dev --no-audit --no-fund && npm cache clean --force
COPY tsconfig.json ./
COPY src ./src
COPY prompts ./prompts
USER node
EXPOSE 8080
ENTRYPOINT ["npx", "tsx"]
CMD ["src/ingest/main.ts"]
