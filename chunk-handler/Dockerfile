FROM clojure:temurin-21-tools-deps-alpine
WORKDIR /app
COPY deps.edn .
RUN clj -P
COPY src ./src
CMD ["clj", "-M", "-m", "handler"]
