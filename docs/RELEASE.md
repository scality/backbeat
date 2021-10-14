# Backbeat Release Plan

## Docker Image generation

Docker images are hosted on [registry.scality.com](registry.scality.com).
It has two names spaces for backbeat:

* Production Namespace: registry.scality.com/backbeat
* Dev Namespace: registry.scality.com/backbeat-dev

The CI will push images with every CI build tagging the
content with the developer’s branch short SHA-1 commit hash.
This allows those images to be used by developers, CI builds,
build chain and so on.

Tagged versions of backbeat will be stored in the production namespace.

## How to pull docker images

```sh
docker pull registry.scality.com/backbeat-dev/backbeat:<SHA-1 commit hash>
docker pull registry.scality.com/backbeat/backbeat:<tag>
```

## Release Process

To release a production image:

* Chose the name of the tag for the repository and the docker image.

* Update the `package.json` using the command `yarn version` with the same tag.

* Create a PR and merge the `package.json` change.

* Trigger the [Release Workflow] via the workflow dispatch function.
Fill the form information, select the desired branch and run it.

[Release Workflow]:
https://github.com/scality/backbeat/actions/workflows/release.yaml
