FROM node:12.18.2

WORKDIR /app

COPY package*.json .
RUN npm install
COPY . .
CMD node index.js