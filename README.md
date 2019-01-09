# Athenai

Athenai snapshots your Athena history to S3.

## Usage

Athenai requires AWS credentials to run, and also needs to know which region to use when talking to the Athena API. Make sure that `AWS_REGION` is set, and that AWS SDK has access to credentials, either through environment variables or an EC2 metadata server.

Athenai will write the history to the S3 location given by the `HISTORY_BASE_URI` environment variable. If you also provide `STATE_URI` it will store its state in that location so that the next run will only read state up until the point where the previous run started.

You can run Athenai from a checkout, like this:

```shell
$ bundle install
$ HISTORY_BASE_URI=s3://my-athena-history/data/ -e STATE_URI=s3://my-athena-history/state.json bundle exec bin/athenai save-history
```

or with Docker, like this:

```shell
$ docker run -it --rm -e AWS_REGION=us-east-2 -e HISTORY_BASE_URI=s3://my-athena-history/data/ -e STATE_URI=s3://my-athena-history/state.json burtcorp/athenai
```

## Development

You run the tests with:

```shell
$ bundle exec rake spec
```
