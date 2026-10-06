FROM node:20-alpine

WORKDIR /app

# Install dependencies
COPY package.json package-lock.json* ./
RUN npm install

# Copy source code
COPY src ./src
COPY public ./public
COPY next.config.js tsconfig.json tailwind.config.ts postcss.config.js ./

# API base URL baked into the client bundle at build time.
# Dev compose omits it (falls back to http://localhost:8000/api);
# prod compose passes /api so the app works behind one reverse-proxied domain.
ARG NEXT_PUBLIC_API_URL
ENV NEXT_PUBLIC_API_URL=${NEXT_PUBLIC_API_URL}

# Build the application
RUN npm run build

EXPOSE 3000

CMD ["npm", "start"]
