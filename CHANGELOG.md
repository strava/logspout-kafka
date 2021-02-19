# Changelog

The changelog and versioning starts at 1.1.0 because the original repo didn't include versioning. We added versioning when this was forked to Strava.

# v2.0.1-rc1

Don't break when encountering an error while formatting to LogstashMessage.

# v2.0.0-rc1

Remove the "template" functionality and add LogstashMessage struct to kafka.go.

This simplifies the already complicated build process for logspout-kafka, and gives us more options to add message fields.

# v1.1.0

Only allow JSON-formatted logs.
