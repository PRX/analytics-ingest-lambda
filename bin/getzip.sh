#!/bin/sh
OUT_FILE="$1"
if [[ "$OUT_FILE" == "" ]]; then
  OUT_FILE="."
fi

# find the image id
images=$(docker images --filter "label=org.prx.lambda" --format "{{.ID}}")
code=$?
if [[ ! $code -eq 0 ]]; then
  exit $code
fi
image_id=$(echo "$images" | head -n 1)
if [[ "$image_id" == "" ]]; then
  echo "No image found for this repo - try docker building first"
  exit 1
fi

# create a temporary container for this image
container_id=$(docker create $image_id)

# find the zip file within the container (it's the value of the label)
zip_path=$(docker inspect --format "{{ index .Config.Labels \"org.prx.lambda\"}}" $container_id)
echo "Copying $zip_path -> $OUT_FILE"
docker cp $container_id:$zip_path $OUT_FILE

# cleanup
cleaned=`docker rm $container_id`
