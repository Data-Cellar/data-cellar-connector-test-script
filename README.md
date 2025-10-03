# Data Cellar Connector Test Script

A Python script that automates contract negotiation and data transfer with Data Cellar connectors.

This tool helps verify that your connector responds correctly and can communicate with other Data Cellar connectors. By default, the arguments are set to connect with the CTIC-FAEN connector.

## Overview

This script automates the entire data transfer process:

1. **Establish connection** for receiving pull credentials (SSE or RabbitMQ)
2. **Negotiate contract** with a counter-party connector
3. **Initiate transfer** process
4. **Receive credentials** via chosen messaging method
5. **Data transfer** to retrieve the data

This script acts as your *Application* in the diagram below, automating contract negotiation and data transfer through your *Connector*. It uses the obtained credentials to fetch data from the provider's data source.

```mermaid
graph TD
  %% External
  Provider[Provider Connector<br/>Proxy to Data Source]
  DataSource[Provider Data Source API]

  %% Your side
  App[Your Application]
  YourConnector[Your Connector]

  %% Flow
  App -->|Contract Negotiation<br/>& Data Transfer| YourConnector
  YourConnector <-->|Dataspace Protocol| Provider
  YourConnector -->|Credentials result from the contract negotiation and data transfer| App
  App -->|HTTP request with credentials| Provider --> DataSource

  %% Styling
  classDef external fill:#e1f5fe
  classDef yours fill:#f3e5f5

  class Provider,DataSource external
  class App,YourConnector yours
```

## Requirements

- Python 3.11+
- [uv](https://docs.astral.sh/uv/getting-started/installation/)

## Installation

```bash
uv sync
```

## Usage

### Basic SSE Example

SSE (Server-Sent Events) is the default and recommended method for most use cases:

```bash
uv run python main.py \
  --api-auth-key YOUR_API_KEY \
  --connector-id YOUR_CONNECTOR_ID \
  --connector-host connector.example.com
```

### RabbitMQ Example

For environments that prefer to use message broker-based delivery instead of HTTP SSE:

```bash
uv run python main.py \
  --api-auth-key YOUR_API_KEY \
  --connector-id YOUR_CONNECTOR_ID \
  --connector-host connector.example.com \
  --messaging-method rabbitmq \
  --rabbitmq-url amqp://user:pass@rabbitmq.example.com:5672
```

> [!TIP]
> **Note on RabbitMQ Connectivity:** RabbitMQ message brokers in Data Cellar deployments do not expose their ports by default. To connect from outside the network, you have two options:
>
> 1. **Run an ephemeral proxy** to forward the RabbitMQ port:
>    ```bash
>    docker run --rm -it -p 5672:5672 --network datacellar_network alpine/socat tcp-listen:5672,reuseaddr,fork tcp:<connector-broker-container-name>:5672
>    ```
>
> 2. **Run this script inside a container** in the Data Cellar participant template network, which provides direct access to the RabbitMQ broker.

## Command-Line Arguments

### Required Arguments

| Argument           | Description                                           |
| ------------------ | ----------------------------------------------------- |
| `--api-auth-key`   | Your Data Cellar connector API key for authentication |
| `--connector-id`   | Your Data Cellar connector ID                         |
| `--connector-host` | Your Data Cellar connector hostname                   |

### Counter-Party Configuration

| Argument                       | Default                                      | Description                                                                                  |
| ------------------------------ | -------------------------------------------- | -------------------------------------------------------------------------------------------- |
| `--counter-party-protocol-url` | `https://ctic.dcserver.cticpoc.com/protocol` | Counter-party Data Cellar connector protocol URL                                             |
| `--counter-party-connector-id` | `ctic`                                       | Counter-party Data Cellar connector ID                                                       |
| `--counter-party-dataset-id`   | None (randomly selected)                     | ID of the dataset to transfer (if not provided, a random GET-api-* dataset will be selected) |

### Connector Configuration

| Argument                 | Default             | Description                                    |
| ------------------------ | ------------------- | ---------------------------------------------- |
| `--connector-port`       | `443`               | Your Data Cellar connector port                |
| `--connector-scheme`     | `https`             | Your Data Cellar connector scheme (http/https) |
| `--participant-id`       | (uses connector-id) | Your Data Cellar connector participant ID      |
| `--consumer-backend-url` | (auto-constructed)  | Consumer backend URL for SSE                   |

### Messaging Configuration

| Argument             | Default                                            | Description                                       |
| -------------------- | -------------------------------------------------- | ------------------------------------------------- |
| `--messaging-method` | `sse`                                              | Credential delivery method: `sse` or `rabbitmq`   |
| `--consumer-id`      | None (falls back to `data-transfer-pull-consumer`) | Consumer ID for RabbitMQ (rabbitmq only)          |
| `--queue-timeout`    | `60`                                               | RabbitMQ queue timeout in seconds (rabbitmq only) |
| `--rabbitmq-url`     | None                                               | RabbitMQ connection URL (required for rabbitmq)   |

### Other Options

| Argument      | Default | Description                                          |
| ------------- | ------- | ---------------------------------------------------- |
| `--log-level` | `DEBUG` | Logging level: DEBUG, INFO, WARNING, ERROR, CRITICAL |

## Output

The script provides detailed logging at each step:

- Connection establishment
- Contract negotiation progress
- Transfer process initiation
- Credential receipt
- Data retrieval with response preview

Successful completion displays a preview of the retrieved data.

## Detailed Example

Assume the following participant template `.env` configuration file (shortened for brevity):

```console
$ cat participant-template/deploy/participants/ctic/.env 
DOMAIN_NAME=dcserver.cticpoc.com
PARTICIPANT_NAME="ctic"
EDC_CONNECTOR_API_KEY="The-Secret-Connector-Api-Key"
EDC_CONNECTOR_API_KEY_HEADER="X-API-Key"
RABBITMQ_DEFAULT_USER="guest"
RABBITMQ_DEFAULT_PASS="Secret-Rabbit-Password"
```

To fetch the CTIC-FAEN dataset (configured by default), run:

```console
uv run python main.py --api-auth-key The-Secret-Connector-Api-Key \
    --connector-id ctic \
    --connector-host ctic.dcserver.cticpoc.com
```

Alternatively, you can use RabbitMQ messaging instead of HTTP SSE (assuming the RabbitMQ broker port in `ctic.dcserver.cticpoc.com` is publicly exposed):

```console
uv run python main.py --api-auth-key The-Secret-Connector-Api-Key \
    --connector-id ctic \
    --connector-host ctic.dcserver.cticpoc.com \
    --messaging-method=rabbitmq \
    --rabbitmq-url=amqp://guest:Secret-Rabbit-Password@ctic.dcserver.cticpoc.com:5672
```

## FAQs

**Where can I find my connector's API authentication key?**

Your API authentication key is a secret value stored in your participant's dotenv configuration file. You can find it at:

```
deploy/participants/<your-participant-name>/.env
```

**What's my connector host URL?**

Your connector host follows this pattern: `https://<your-participant-name>.<your-base-domain>`

For example, if your participant name is `ctic` and your base domain is `dcserver.cticpoc.com`, your connector host would be:

```
https://ctic.dcserver.cticpoc.com
```

**Should I use RabbitMQ or HTTP SSE?**

For most use cases, we recommend **HTTP SSE** as it's simpler to set up and configure. 

Choose RabbitMQ only if you have advanced requirements that need more control over message handling. If you're unsure which to pick, HTTP SSE is likely the better choice for your needs.
