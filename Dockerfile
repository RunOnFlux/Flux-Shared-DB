FROM node:16

WORKDIR /usr/src/app
COPY package*.json ./
RUN npm install
COPY . .
EXPOSE 3307
EXPOSE 7071
EXPOSE 8008
CMD [ "node", "ClusterOperator/server.js" ]