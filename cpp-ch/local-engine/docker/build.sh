sudo docker run  --rm --volume="$2":/output --volume="$1":/clickhouse --volume=/tmp/.cache:/ccache -e ENABLE_EMBEDDED_COMPILER=ON libchbuilder:1.0