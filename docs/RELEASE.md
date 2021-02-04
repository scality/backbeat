# Backbeat Release Plan

## Docker Image generation

Docker images are hosted on [registry.scality.com](registry.scality.com).
It has two names spaces for backbeat:

* Production Namespace: registry.scality.com/backbeat
* Dev Namespace: registry.scality.com/backbeat-dev

The CI will push images with every CI build tagging the
content with the developer’s branch short SHA-1 commit hash.
This allows those images to be used by developers, CI builds, build chain and so on.

Tagged versions of backbeat will be stored in the production namespace.

## How to pull docker images

```sh
docker pull registry.scality.com/backbeat-dev/backbeat:<short SHA-1 commit hash>
docker pull registry.scality.com/backbeat/backbeat:<tag>
```

## Release Process

To release a production image:

* Decide an appropriate tag which will be used to tag the repository and
the docker image.
* Update the version in `package.json` using the command `yarn version` with
the same tag.
* Create a PR and merge the `package.json` change.
* Tag the repository using the same tag.
* With the below parameters, [force a build](https://eve.devsca.com/github/scality/backbeat/#/builders/bootstrap/force/force):
  * A given branch that ideally match the tag.
  * Use the `release` stage.
  * Extra property with name as `tag` and the value as the actual tag.
