# Web3 Indexer with Apibara

This repository uses [Apibara](https://github.com/apibara/apibara) to index
web3 data.


## Getting Started

Create a new virtual environment for this project. While this step is not
required, it is _highly recommended_ to avoid conflicts between different
installed packages.

    python3 -m venv venv

Then activate the virtual environment.

    source venv/bin/activate

Then install `poetry` and use it to install the package dependencies.

    python3 -m pip install poetry
    poetry install

Start MongoDB using the provided `docker-compose` file:

    docker-compose up

Notice that you can use any managed MongoDB like MongoDB Atlas.

Unless you're running Apibara DNA locally, you need to connect to one of the
streams we provide.
You should register for a FREE API Key on [the Apibara
website](https://www.apibara.com/).
Once you have the key, expose it to the indexer by setting the `DNA_TOKEN`
environment variable:

    export DNA_TOKEN="<my-token>"

Then start the indexer by running the `indexer start` command. The `indexer`
command runs the cli application defined in `src/indexer/main.py`. This is a
standard Click application.

Notice that by default the indexer will start indexing from where it left off in the previous run. If you want restart, use the `--restart` flag.

    indexer start --restart

Notice that will also delete the database with the indexer's data.


## Customizing the template

You can change the id of the indexer by changing the value of the `indexer_id`
variable in `src/indexer/indexer.py`. This id is also used as the name of the
Mongo database where the indexer data is stored.


## Running in production

This template includes a `Dockerfile` that you can use to package the indexer
for production usage.

If you're using the provided `docker-compose.prod.yml`, make sure to pass the
`DNA_TOKEN` environment variable when starting.
You can do that by creating a `.env` file in the current directory.
