docker build -t hawkeye .
CONTAINER=$(docker run hawkeye sleep 2000)
cp $CONTAINER:/hawkeye/target/release/hawkeye hawkeye-linux-64
docker kill $CONTAINER
