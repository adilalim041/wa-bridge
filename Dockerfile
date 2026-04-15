FROM node:20-slim

# git required for GitHub-pinned npm deps (baileys + its transitive libsignal-node)
RUN apt-get update && apt-get install -y --no-install-recommends git openssh-client ca-certificates && rm -rf /var/lib/apt/lists/*

# Force HTTPS for all github URLs (avoid ssh for transitive deps like libsignal-node)
RUN git config --global url."https://github.com/".insteadOf "ssh://git@github.com/" \
 && git config --global url."https://github.com/".insteadOf "git@github.com:" \
 && git config --global url."https://github.com/".insteadOf "git+ssh://git@github.com/"

WORKDIR /app

COPY package*.json ./
RUN npm ci --omit=dev

COPY . .

EXPOSE 3001

CMD ["node", "src/index.js"]
