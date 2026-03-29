FROM node:20-alpine

WORKDIR /app

# Install dependencies
COPY package.json package-lock.json* ./
RUN npm install

# Copy source code
COPY src ./src
COPY public ./public
COPY next.config.js tsconfig.json tailwind.config.ts postcss.config.js ./

# Build the application
RUN npm run build

EXPOSE 3000

CMD ["npm", "start"]
