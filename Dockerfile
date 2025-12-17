FROM node:20 AS build-env

WORKDIR /app
COPY . /app

RUN npm ci --omit=dev

FROM gcr.io/distroless/nodejs20-debian12
COPY --from=build-env /app /app
WORKDIR /app
EXPOSE 3307
EXPOSE 7071
EXPOSE 8008
CMD [ "ClusterOperator/server.js" ]
