FROM node:25-alpine

RUN npm install -g pnpm

WORKDIR /app

EXPOSE 3000

CMD ["sh", "-c", "pnpm install --no-frozen-lockfile --node-linker=hoisted && pnpm dev --host 0.0.0.0 --port 3000"]
