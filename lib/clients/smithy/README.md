# Smithy Client

This directory contains the Smithy-based TypeScript/JavaScript client for Cloudserver's internal APIs. The client is generated from the .smithy model files that we defined in the /models folder.

If you need to work on it, install Smithy first : https://smithy.io/2.0/guides/smithy-cli/cli_installation.html

## Architecture

Each .smithy file defines one api, and we have the Backbeat service defined in models/backbeat.smithy uses these apis.

The client generation is performed this way :
From the main backbeat directory, run :

```bash
# Generate TypeScript from .smithy model files
yarn run generate-smithy-client

# Compile TypeScript to JavaScript and create npm package
yarn run build-smithy-client
```

### Local testing

1. Install test dependencies: `cd localTests && bun install`
2. Start CloudServer: `S3VAULT=mem S3METADATA=mem S3DATA=mem REMOTE_MANAGEMENT_DISABLE=true yarn start`
3. Initialize test data: `bun initBucketForTests.ts`
4. Run tests: `bun testsApis.ts` (or other test files)
