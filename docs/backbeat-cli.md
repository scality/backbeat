# Backbeat CLI documentation

Welcome to the **Backbeat CLI** documentation. This guide provides detailed instructions on installing, configuring, and using the Backbeat Command Line Interface (CLI) to interact with the Backbeat service.

## Table of Contents

- [Introduction](#introduction)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
  - [Available commands](#available-commands)
    - [list-lifecycle-currents](#list-lifecycle-currents)
    - [list-lifecycle-noncurrents](#list-lifecycle-noncurrents)
    - [list-lifecycle-orphans](#list-lifecycle-orphans)
- [Examples](#examples)
- [Help](#help)


## Introduction

The **Backbeat CLI** is a command-line tool designed to facilitate interactions with the Backbeat service. It allows users to list lifecycle objects within buckets, providing functionalities to manage and monitor lifecycle states effectively.

## Installation

To install and set up the Backbeat CLI, follow these steps:

1. **Clone the repository**

```bash
git clone http://github.com/scality/backbeat
```

1. ** TODO: REMOVE BEFORE MERGING**

```bash
git checkout improvement/BB-616/bb-cli
```

2. **Go to the project directory**
   
```bash
cd backbeat
```

3. **Install dependencies**

Ensure you have Node.js installed (version 16 or higher is recommended).

```bash
npm install
```

4. **Create an alias for easy access**

```bash
alias backbeat-cli='node bin/backbeat.cli.js'
```

## Configuration

Before using the Backbeat CLI, configure the necessary environment variables to establish a connection with the Backbeat service.

### Environment variables

- `BACKBEAT_ENDPOINT`: The endpoint URL for the Backbeat service.  
  *Default*: `http://127.0.0.1:8000`

- `AWS_REGION`: The AWS region to use.  
  *Default*: `us-east-1`

- `AWS_ACCESS_KEY_ID`: Your AWS access key ID.  
  *Default*: `accessKey1`

- `AWS_SECRET_ACCESS_KEY`: Your AWS secret access key.  
  *Default*: `verySecretKey1`

You can set these variables in your shell or include them in a configuration file as needed.

```bash
export BACKBEAT_ENDPOINT='http://your-backbeat-endpoint.com'
export AWS_REGION='your-region'
export AWS_ACCESS_KEY_ID='your-access-key-id'
export AWS_SECRET_ACCESS_KEY='your-secret-access-key'
```

## Usage

After installation and configuration, you can use the Backbeat CLI to execute various commands. The general syntax is:

```bash
backbeat-cli <command> [options]
```

### Available commands

#### `list-lifecycle-currents`

**Description**: List current lifecycle objects in a specified bucket.

**Options**:

- `-b, --bucket <bucket>`: **(Required)** Name of the bucket.
- `-d, --before-date <date>`: Limit to keys modified before this date (`YYYY-MM-DD`).
- `-e, --excluded-data-store-name <name>`: Exclude specific data store name.
- `-enc, --encoding-type <type>`: Encoding type (e.g., `url`).  
  *Default*: `url`
- `-m, --marker <marker>`: Marker for pagination.
- `-k, --max-keys <number>`: Maximum number of keys to return.
- `-p, --prefix <prefix>`: Filter keys by prefix.

**Usage example**:

```bash
backbeat-cli list-lifecycle-currents --bucket my-bucket --before-date 2024-10-26
```

#### `list-lifecycle-noncurrents`

**Description**: List non-current lifecycle objects in a specified bucket.

**Options**:

- `-b, --bucket <bucket>`: **(Required)** Name of the bucket.
- `-d, --before-date <date>`: Limit to keys modified before this date (`YYYY-MM-DD`).
- `-e, --excluded-data-store-name <name>`: Exclude specific data store name.
- `-enc, --encoding-type <type>`: Encoding type (e.g., `url`).  
  *Default*: `url`
- `-km, --key-marker <marker>`: Key marker for pagination.
- `-vm, --version-id-marker <id>`: Version ID marker for pagination.
- `-k, --max-keys <number>`: Maximum number of keys to return.
- `-p, --prefix <prefix>`: Filter keys by prefix.

**Usage example**:

```bash
backbeat-cli list-lifecycle-noncurrents --bucket my-bucket --prefix logs/
```

#### `list-lifecycle-orphans`

**Description**: List orphan lifecycle objects in a specified bucket.

**Options**:

- `-b, --bucket <bucket>`: **(Required)** Name of the bucket.
- `-d, --before-date <date>`: Limit to keys modified before this date (`YYYY-MM-DD`).
- `-e, --excluded-data-store-name <name>`: Exclude specific data store name.
- `-enc, --encoding-type <type>`: Encoding type (e.g., `url`).  
  *Default*: `url`
- `-m, --marker <marker>`: Marker for pagination.
- `-k, --max-keys <number>`: Maximum number of keys to return.
- `-p, --prefix <prefix>`: Filter keys by prefix.

**Usage example**:

```bash
backbeat-cli list-lifecycle-orphans --bucket my-bucket --max-keys 50
```

## Examples

### Example 1: List current lifecycle objects

Lists current lifecycle objects in the source bucket that were modified before October 26, 2024:

```bash
backbeat-cli list-lifecycle-currents --bucket source --before-date 2024-10-26
```

### Example 2: List non-current lifecycle objects with prefix

Lists non-current lifecycle objects in my-bucket with keys starting with images/:

```bash
backbeat-cli list-lifecycle-noncurrents --bucket my-bucket --prefix images/
```

### Example 3: List orphan lifecycle objects with pagination

Lists orphan lifecycle objects in the archive bucket, starting from marker abc123, limiting the output to 100 keys:

```bash
backbeat-cli list-lifecycle-orphans --bucket archive --marker abc123 --max-keys 100
```

## Help

To view the help information for the Backbeat CLI and its commands, use the `--help` flag:

```bash
backbeat-cli --help
```

For help on a specific command, append --help after the command:

```bash
backbeat-cli list-lifecycle-currents --help
```

This will display detailed information about the command's usage, options, and examples.
