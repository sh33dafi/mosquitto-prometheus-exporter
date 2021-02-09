FROM node:15-alpine3.12
RUN apk add dumb-init
WORKDIR /home/app/

COPY --chown=node:node package.json /home/app/package.json
COPY --chown=node:node package-lock.json /home/app/package-lock.json
COPY --chown=node:node src/main.js /home/app/src/main.js

ENV NODE_ENV production
RUN npm ci --only=production

# RUN apk add dumb-init
# CMD ["dumb-init", "node", "server.js"]
EXPOSE 3000
USER node
CMD ["dumb-init", "node", "src/main"]
