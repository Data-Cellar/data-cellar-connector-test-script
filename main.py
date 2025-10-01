"""
Data Cellar connector data transfer script.

This script performs a complete data transfer flow with a Data Cellar connector by:
1. Establishing a connection to receive pull credentials (SSE or RabbitMQ)
2. Negotiating a contract with a counter-party connector
3. Starting a data transfer process
4. Retrieving credentials via the chosen messaging method
5. Executing an authenticated data request

The script supports two messaging methods for credential delivery:
- SSE (Server-Sent Events): HTTP-based streaming connection
- RabbitMQ: Message broker-based delivery
"""

import argparse
import asyncio
import json
import logging
import pprint
from typing import Dict, Optional
from urllib.parse import urlparse

import coloredlogs
import edcpy.config
import httpx
from edcpy.edc_api import ConnectorController
from edcpy.messaging import MessagingClient

_logger = logging.getLogger(__name__)

# HTTP constants
HTTP_STATUS_OK = 200

# Timeout constants (in seconds)
SSE_CONNECTION_TIMEOUT = 5.0
SSE_READ_TIMEOUT = 60.0
SSE_WRITE_TIMEOUT = 5.0
SSE_POOL_TIMEOUT = 5.0
CREDENTIALS_WAIT_TIMEOUT = 60.0
DEFAULT_QUEUE_TIMEOUT = 60

# Default configuration values
DEFAULT_CONNECTOR_PORT = 443
DEFAULT_CONSUMER_ID = "data-transfer-pull-consumer"

# Logging and display constants
RESPONSE_PREVIEW_WIDTH = 100
RESPONSE_PREVIEW_MAX_LENGTH = 1024

# SSE message constants
SSE_DATA_PREFIX = "data: "
SSE_MESSAGE_TYPE_PULL = "pull_message"


class SSEPullCredentialsReceiver:
    """
    Receives pull credentials from the provider's SSE endpoint.

    This class manages an SSE (Server-Sent Events) connection to receive
    pull credentials for data transfers. It allows you to start listening
    before the Transfer Process ID is known, ensuring you don't miss the
    access token message.

    Attributes:
        args: Command-line arguments containing authentication and connection details
        headers: HTTP headers for SSE connection authentication
        _futures: Dictionary mapping transfer IDs to futures awaiting credentials
        _listener_task: Background task that listens to the SSE stream
        _connected_event: Event that signals when SSE connection is established
    """

    def __init__(self, args: argparse.Namespace):
        """
        Initialize the SSE credentials receiver.

        Args:
            args: Parsed command-line arguments containing API auth key and URLs
        """

        self.args = args

        self.headers = {
            "Authorization": f"Bearer {args.api_auth_key}",
            "Accept": "text/event-stream",
        }

        # Dictionary of futures, one per transfer ID. Each future is resolved
        # exactly once with the pull-message credentials dict
        self._futures: Dict[str, asyncio.Future] = {}

        # Background listener task that consumes the SSE stream
        self._listener_task: Optional[asyncio.Task] = None

        # Event that becomes true once the SSE connection is confirmed (HTTP 200)
        self._connected_event: asyncio.Event = asyncio.Event()

    async def start_listening(self, protocol_url: str):
        """
        Start the background SSE listener.

        This coroutine returns once the HTTP stream is ready, ensuring that
        callers can safely trigger contract negotiation without the risk of
        missing the first credential message.

        Args:
            protocol_url: The counter-party's protocol URL, used to determine
                         the provider hostname for the SSE stream

        Raises:
            asyncio.TimeoutError: If SSE connection is not established within timeout
        """

        # Prevent starting multiple listeners
        if self._listener_task and not self._listener_task.done():
            _logger.warning("SSE listener already running")
            return

        # Determine the consumer backend URL (use provided or construct from args)
        consumer_backend_url = (
            self.args.consumer_backend_url
            or f"{self.args.connector_scheme}://{self.args.connector_host}"
        )

        if not self.args.consumer_backend_url:
            _logger.warning(
                "Consumer backend URL is not set, using default: %s",
                consumer_backend_url,
            )

        # Extract provider hostname and construct SSE stream URL
        provider_host = urlparse(protocol_url).hostname
        url = f"{consumer_backend_url}/pull/stream/provider/{provider_host}"

        _logger.info(f"Connecting to SSE stream for provider: {provider_host}")

        # Reset connection state and start the listener task
        self._connected_event.clear()
        self._listener_task = asyncio.create_task(self._listen_sse_stream(url))

        # Wait for HTTP 200 response or fail fast if connection cannot be established
        await asyncio.wait_for(
            self._connected_event.wait(), timeout=SSE_CONNECTION_TIMEOUT
        )

    def _parse_sse_message(self, line: str) -> Optional[dict]:
        """
        Parse an SSE message line and extract the JSON payload.

        Args:
            line: Raw SSE message line

        Returns:
            Parsed message dict if valid, None otherwise
        """

        if not line.strip().startswith(SSE_DATA_PREFIX):
            return None

        try:
            # Remove SSE 'data: ' prefix and parse JSON payload
            json_payload = line.strip()[len(SSE_DATA_PREFIX) :]
            return json.loads(json_payload)
        except json.JSONDecodeError:
            _logger.warning(f"Invalid JSON in SSE message: {line}")
            return None

    def _process_pull_message(self, message: dict):
        """
        Process a pull_message and resolve the corresponding future.

        Args:
            message: The pull_message dict containing transfer credentials
        """

        # Only process pull_message type
        if message.get("type") != SSE_MESSAGE_TYPE_PULL:
            return

        transfer_id = message.get("transfer_process_id")
        if not transfer_id:
            return

        # Get or create a future for this transfer ID
        future = self._futures.get(transfer_id)
        if future is None:
            future = asyncio.get_event_loop().create_future()
            self._futures[transfer_id] = future

        # Resolve the future with credentials (only once)
        if not future.done():
            _logger.info(f"Received credentials for transfer: {transfer_id}")
            future.set_result(message)

    async def _listen_sse_stream(self, url: str):
        """
        Internal background task that consumes the SSE stream.

        This task continuously reads from the SSE stream, parses messages,
        and resolves futures for transfer IDs as credentials arrive.

        Args:
            url: The SSE stream endpoint URL
        """

        timeout = httpx.Timeout(
            connect=SSE_CONNECTION_TIMEOUT,
            read=SSE_READ_TIMEOUT,
            write=SSE_WRITE_TIMEOUT,
            pool=SSE_POOL_TIMEOUT,
        )

        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                async with client.stream("GET", url, headers=self.headers) as response:
                    if response.status_code != HTTP_STATUS_OK:
                        raise ConnectionError(
                            f"SSE connection failed with status {response.status_code}"
                        )

                    _logger.info("SSE stream connected successfully")
                    self._connected_event.set()

                    # Process each line from the SSE stream
                    async for line in response.aiter_lines():
                        message = self._parse_sse_message(line)

                        if message:
                            self._process_pull_message(message)

        except Exception as e:
            # Ensure listeners are not left waiting forever if connection fails
            self._connected_event.set()
            _logger.error(f"SSE listener error: {e}", exc_info=True)

    async def get_credentials(
        self, transfer_id: str, timeout: float = CREDENTIALS_WAIT_TIMEOUT
    ) -> dict:
        """
        Await and retrieve credentials for a specific transfer ID.

        This method will either return credentials that have already been
        received via SSE, or wait for them to arrive (up to the timeout).

        Args:
            transfer_id: The transfer process ID to get credentials for
            timeout: Maximum time to wait for credentials (seconds)

        Returns:
            Dict containing the pull_message with credentials and request details

        Raises:
            TimeoutError: If credentials are not received within the timeout period
        """

        # Get or create a future for this transfer ID
        future = self._futures.get(transfer_id)

        if future is None:
            future = asyncio.get_event_loop().create_future()
            self._futures[transfer_id] = future

        try:
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError as exc:
            raise TimeoutError(
                f"Timeout ({timeout}s) waiting for credentials for transfer: {transfer_id}"
            ) from exc

    async def stop_listening(self):
        """
        Stop the SSE listener and clean up resources.

        This method cancels the background listener task and clears all
        pending futures and connection state. It should be called when
        the SSE connection is no longer needed.
        """

        if self._listener_task and not self._listener_task.done():
            self._listener_task.cancel()

            try:
                await self._listener_task
            except asyncio.CancelledError:
                # Expected when cancelling the task
                pass

        # Clean up all pending futures and reset state
        self._futures.clear()
        self._connected_event.clear()


def create_argument_parser() -> argparse.ArgumentParser:
    """
    Create and configure the command-line argument parser.

    Returns:
        Configured ArgumentParser with all required and optional arguments
    """

    parser = argparse.ArgumentParser(
        description="Data Cellar connector data transfer - performs a complete "
        "data transfer flow including contract negotiation and authenticated data retrieval",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--log-level",
        default="DEBUG",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level",
    )

    parser.add_argument(
        "--counter-party-protocol-url",
        default="https://ctic.dcserver.cticpoc.com/protocol",
        help="Counter party protocol URL",
    )

    parser.add_argument(
        "--counter-party-connector-id",
        default="ctic",
        help="Counter party connector ID",
    )

    parser.add_argument(
        "--counter-party-dataset-id",
        help="ID of the dataset to transfer",
        default="GET-api-dataset-9fe2f7c3-4dce-4964-8e7d-2f0b32c343fe",
    )

    parser.add_argument(
        "--api-auth-key", required=True, help="EDC connector API key for authentication"
    )

    parser.add_argument(
        "--connector-id",
        default="ctic",
        help="EDC connector ID",
        required=True,
    )

    parser.add_argument(
        "--participant-id",
        help="EDC connector participant ID",
    )

    parser.add_argument(
        "--connector-host",
        help="EDC connector host",
        required=True,
    )

    parser.add_argument(
        "--connector-port",
        help="EDC connector port",
        type=int,
        default=DEFAULT_CONNECTOR_PORT,
    )

    parser.add_argument(
        "--connector-scheme",
        help="EDC connector scheme (e.g., http or https)",
        default="https",
    )

    parser.add_argument(
        "--consumer-backend-url",
        help="Consumer backend URL",
    )

    parser.add_argument(
        "--messaging-method",
        choices=["sse", "rabbitmq"],
        default="sse",
        help="Method for receiving pull credentials (SSE or RabbitMQ)",
    )

    parser.add_argument(
        "--consumer-id",
        help="Consumer ID for RabbitMQ messaging (only used with --messaging-method=rabbitmq)",
    )

    parser.add_argument(
        "--queue-timeout",
        type=int,
        default=DEFAULT_QUEUE_TIMEOUT,
        help="Timeout in seconds for RabbitMQ queue operations (only used with --messaging-method=rabbitmq)",
    )

    parser.add_argument(
        "--rabbitmq-url",
        help="RabbitMQ URL",
    )

    return parser


def build_connector_config_from_args(
    args: argparse.Namespace,
) -> edcpy.config.AppConfig:
    """
    Build an AppConfig instance from parsed command-line arguments.

    Args:
        args: Parsed command-line arguments

    Returns:
        AppConfig instance configured with connector details
    """

    # Use participant_id if provided, otherwise fall back to connector_id
    participant_id = args.participant_id or args.connector_id

    return edcpy.config.AppConfig(
        rabbit_url=args.rabbitmq_url,
        connector=edcpy.config.AppConfig.Connector(
            connector_id=args.connector_id,
            host=args.connector_host,
            participant_id=participant_id,
            scheme=args.connector_scheme,
            api_key=args.api_auth_key,
            management_port=args.connector_port,
            public_port=args.connector_port,
            protocol_port=args.connector_port,
            control_port=args.connector_port,
        ),
    )


def ensure_url_ends_with_slash(url: str) -> str:
    """
    Ensure a URL ends with a trailing slash.

    Some EDC endpoints require a trailing slash to function correctly.
    This function normalizes URLs to always end with '/'.

    Args:
        url: URL to normalize

    Returns:
        URL with trailing slash (e.g., '/public/' instead of '/public')
    """

    return url.rstrip("/") + "/"


async def negotiate_contract(
    controller: ConnectorController,
    protocol_url: str,
    connector_id: str,
    dataset_id: str,
) -> dict:
    """
    Negotiate a contract with the counter-party connector.

    Args:
        controller: ConnectorController instance
        protocol_url: Counter-party protocol URL
        connector_id: Counter-party connector ID
        dataset_id: Dataset ID to transfer

    Returns:
        Transfer details dictionary containing negotiation results
    """

    _logger.info("Step 2: Negotiating contract")

    return await controller.run_negotiation_flow(
        counter_party_protocol_url=protocol_url,
        counter_party_connector_id=connector_id,
        asset_query=dataset_id,
    )


async def initiate_transfer(
    controller: ConnectorController, transfer_details: dict
) -> str:
    """
    Initiate a pull-based data transfer.

    Args:
        controller: ConnectorController instance
        transfer_details: Transfer details from contract negotiation

    Returns:
        Transfer process ID
    """

    _logger.info("Step 3: Starting transfer process")

    transfer_id = await controller.run_transfer_flow(
        transfer_details=transfer_details, is_provider_push=False
    )

    _logger.info(f"Transfer process ID: {transfer_id}")
    return transfer_id


async def execute_authenticated_request(request_args: dict) -> str:
    """
    Execute an authenticated data request using provided credentials.

    Args:
        request_args: Dictionary containing HTTP request arguments (url, method, headers, etc.)

    Returns:
        Response text from the authenticated request
    """

    _logger.info("Step 5: Executing authenticated data request")

    # IMPORTANT: Normalize URL to include trailing slash (required by some endpoints)
    request_args = {**request_args}
    request_args["url"] = ensure_url_ends_with_slash(request_args["url"])

    async with httpx.AsyncClient() as client:
        response = await client.request(**request_args)
        data = response.text

        _logger.info(
            "Data transfer completed successfully ✅\n--- Response preview ---\n%s\n--- End of preview ---",
            pprint.pformat(data, width=RESPONSE_PREVIEW_WIDTH, compact=True)[
                :RESPONSE_PREVIEW_MAX_LENGTH
            ],
        )

        return data


async def run_data_transfer_with_sse(
    args: argparse.Namespace, controller: ConnectorController
) -> str:
    """
    Run data transfer using SSE for credential delivery.

    Args:
        args: Parsed command-line arguments
        controller: ConnectorController instance

    Returns:
        Response data from the authenticated request
    """

    sse_receiver = SSEPullCredentialsReceiver(args)

    try:
        # Step 1: Start listening for SSE events before initiating transfer
        _logger.info("Step 1: Establishing SSE connection")
        await sse_receiver.start_listening(args.counter_party_protocol_url)

        # Step 2: Negotiate contract
        transfer_details = await negotiate_contract(
            controller,
            args.counter_party_protocol_url,
            args.counter_party_connector_id,
            args.counter_party_dataset_id,
        )

        # Step 3: Initiate transfer
        transfer_id = await initiate_transfer(controller, transfer_details)

        # Step 4: Await credentials via SSE
        _logger.info("Step 4: Awaiting transfer credentials via SSE")
        pull_message = await sse_receiver.get_credentials(transfer_id)

        # Step 5: Execute authenticated request
        return await execute_authenticated_request(pull_message["request_args"])

    finally:
        _logger.debug("Cleaning up SSE listener")
        await sse_receiver.stop_listening()


async def run_data_transfer_with_rabbitmq(
    args: argparse.Namespace, controller: ConnectorController
) -> str:
    """
    Run data transfer using RabbitMQ for credential delivery.

    Args:
        args: Parsed command-line arguments
        controller: ConnectorController instance

    Returns:
        Response data from the authenticated request
    """

    consumer_id = args.consumer_id or DEFAULT_CONSUMER_ID

    messaging_client = MessagingClient(
        consumer_id=consumer_id, config=controller.config
    )

    # Start RabbitMQ consumer
    async with messaging_client.pull_consumer(timeout=args.queue_timeout) as consumer:
        _logger.info(
            "Step 1: RabbitMQ consumer started, ready to receive pull credentials"
        )

        # Step 2: Negotiate contract
        transfer_details = await negotiate_contract(
            controller,
            args.counter_party_protocol_url,
            args.counter_party_connector_id,
            args.counter_party_dataset_id,
        )

        # Step 3: Initiate transfer
        transfer_id = await initiate_transfer(controller, transfer_details)

        # Step 4: Await credentials via RabbitMQ
        _logger.info("Step 4: Awaiting transfer credentials via RabbitMQ")

        async with consumer.wait_for_message(
            timeout=args.queue_timeout
        ) as http_pull_message:
            # Step 5: Execute authenticated request
            return await execute_authenticated_request(http_pull_message.request_args)


async def main(args: argparse.Namespace):
    """
    Main function executing the complete connector data transfer flow.

    This function orchestrates a full data transfer flow using either SSE or
    RabbitMQ for credential delivery:
    1. Establishes connection for receiving credentials (SSE or RabbitMQ)
    2. Negotiates a contract with the counter-party connector
    3. Initiates a pull-based data transfer
    4. Retrieves transfer credentials via chosen method
    5. Executes an authenticated data request

    Args:
        args: Parsed command-line arguments

    Returns:
        The response data from the authenticated request
    """

    # Validate RabbitMQ configuration if needed
    if args.messaging_method == "rabbitmq" and not args.rabbitmq_url:
        raise ValueError(
            "RabbitMQ URL is required when using --messaging-method=rabbitmq"
        )

    # Initialize connector controller
    connector_config = build_connector_config_from_args(args)
    controller = ConnectorController(config=connector_config)

    _logger.info(f"Starting data transfer for asset: {args.counter_party_dataset_id}")
    _logger.info(f"Using messaging method: {args.messaging_method.upper()}")

    # Route to appropriate messaging implementation
    if args.messaging_method == "sse":
        return await run_data_transfer_with_sse(args, controller)
    else:
        return await run_data_transfer_with_rabbitmq(args, controller)


if __name__ == "__main__":
    # Parse command-line arguments
    parser = create_argument_parser()
    args = parser.parse_args()

    # Configure logging with colored output
    coloredlogs.install(level=args.log_level)

    # Run the data transfer
    asyncio.run(main(args))
