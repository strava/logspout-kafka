# How to Build Logspout Kafka

If you need to update a go dependency, say sarama, here is some information that should help!

## Testing locally.

* Go to the "build" directory in config repo
* There is a `build.sh` file. This file is created from the original logspout library: https://github.com/gliderlabs/logspout/blob/99f410a90c92b014a3d1a19c49811951b28d6836/Dockerfile#L9 . It's added during ONBUILD.
* Modify the build.sh to `go get` the module locally.
