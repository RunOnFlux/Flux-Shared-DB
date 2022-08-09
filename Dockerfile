FROM node:16 AS build-env

WORKDIR /app
COPY . /app

RUN npm ci --omit=dev

FROM gcr.io/distroless/nodejs:16
COPY --from=build-env /app /app
WORKDIR /app
EXPOSE 3307
EXPOSE 7071
EXPOSE 8008
CMD [ "ClusterOperator/server.js" ]
