# Flow Event Listener

This script watches specific Flow blockchain events and saves them into a MongoDB database. I built it to keep track of things happening in certain contracts without constantly hitting APIs manually.

## Getting Started

Pretty straightforward:

1.  **Prerequisites:**

    - Node.js (v16+)
    - npm (or yarn)
    - A MongoDB database somewhere (local or Atlas is fine)

2.  **Clone & Install:**

    ```bash
    git clone <your-repo-url>
    cd <your-repo-directory>
    npm install
    ```

3.  **Configure (`.env` file):**
    Create a file named `.env` in the root folder and add your details:

    ```dotenv
    # Your Flow Access Node API (Mainnet, Testnet, etc.)
    FLOW_ACCESS_NODE=[https://rest-mainnet.onflow.org](https://rest-mainnet.onflow.org)

    # Your MongoDB connection string
    MONGODB_URI=mongodb://user:password@host:port/database_name

    # Optional: If running multiple monitors, give this one a unique ID
    # PROJECT_ID=my_unique_monitor_name
    ```

4.  **Run it:**
    ```bash
    node monitor.js
    ```
    It'll connect to Mongo, figure out where to start polling on Flow, and start logging events it finds.

## What it Monitors

The script watches contracts/events defined in the `contracts` object in `monitor.js`.

Right now, it's actively listening for:

- **TSHOT (`A.05b67ba314000b2d.TSHOT`):** `TokensInitialized`, `TokensWithdrawn`, `TokensDeposited`, `TokensMinted`, `TokensBurned`
- **TSHOTExchange (`A.05b67ba314000b2d.TSHOTExchange`):** `swapNFTsForTSHOT`, `commitSwap`, `swapTSHOTForNFTs`

There are other contracts defined in the code (TopShot, Pinnacle, etc.), but their events are commented out. Just uncomment the event names you want to track in the `contracts` object if you need them.

## Database Stuff

It uses a MongoDB database (defaults to `flow_events` but uses the one in your `MONGODB_URI`).

- **`raw_events` collection:** Stores the actual event data found (block height, timestamp, transaction ID, event payload, etc.).
- **`processed_blocks` collection:** Just keeps track of the last block height the script checked, so it knows where to pick up if restarted.
