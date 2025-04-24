// monitor_final_optimized.js
const fcl = require("@onflow/fcl");
const { MongoClient } = require("mongodb");
const pLimit = require("p-limit"); // Use p-limit for concurrent fetching
require("dotenv").config();

// --- Configuration ---
const FLOW_ACCESS_NODE = process.env.FLOW_ACCESS_NODE;
fcl.config().put("accessNode.api", FLOW_ACCESS_NODE);

const MONGODB_URI = process.env.MONGODB_URI;
const DATABASE_NAME = "flow_events";
const RAW_EVENTS_COLLECTION = "raw_events";
const PROCESSED_BLOCKS_COLLECTION = "processed_blocks";

const mongoClient = new MongoClient(MONGODB_URI);
let eventsCollection;
let processedBlocksCollection;

const projectId = process.env.PROJECT_ID || "flow_monitors"; // Match project ID

// Concurrency limit for fetching TX details within the monitor
// Adjust based on Access Node performance and rate limits (start lower, e.g., 5-10)
const TX_FETCH_CONCURRENCY = 8;

// Delay between polling cycles when caught up (milliseconds)
const CAUGHT_UP_POLL_DELAY = 5000; // 5 seconds
// Delay between polling cycles when behind (milliseconds)
const POLLING_DELAY = 500; // 0.5 seconds

// Block range to fetch events for in each poll cycle
const BLOCK_POLL_RANGE = 25;

// Contracts to monitor (Ensure this matches your needs)
const contracts = {
  TSHOT: {
    address: "A.05b67ba314000b2d.TSHOT",
    events: [
      "TokensInitialized",
      "TokensWithdrawn",
      "TokensDeposited",
      "TokensMinted",
      "TokensBurned",
    ],
  },
  TSHOT_EXCHANGE: {
    address: "A.05b67ba314000b2d.TSHOTExchange",
    // Add specific events if needed, otherwise monitor all contract events (might be too noisy)
    // For now, let's assume you only specified events you really need
    events: ["swapNFTsForTSHOT", "commitSwap", "swapTSHOTForNFTs"], // Example relevant events
  },
  // Add other contracts and specific events you need to monitor
  // Example (commented out):
  /*
   TOP_SHOT: {
     address: "A.0b2a3299cc857e29.TopShot",
     events: ["MomentMinted", "Deposit", "Withdraw"],
   },
   FlowFees: { // Good to monitor for gas check reference, though status is better
     address: "A.f919ee77447b7497.FlowFees",
     events: ["FeesDeducted"]
   }
   */
};

// Generate event types
function generateEventTypes() {
  const eventTypes = new Set(); // Use a Set to avoid duplicates if contracts overlap
  for (const contractKey in contracts) {
    const contract = contracts[contractKey];
    if (contract.events && contract.address && Array.isArray(contract.events)) {
      for (const event of contract.events) {
        eventTypes.add(`${contract.address}.${event}`);
      }
    } else {
      console.warn(
        `Skipping invalid contract configuration entry: ${contractKey}`
      );
    }
  }
  if (eventTypes.size === 0) {
    console.error(
      "FATAL: No valid event types generated from contract configuration. Exiting."
    );
    process.exit(1);
  }
  return Array.from(eventTypes);
}
const eventTypes = generateEventTypes();

// --- MongoDB Connection and Indexing ---
async function connectToMongo() {
  try {
    await mongoClient.connect();
    const database = mongoClient.db(DATABASE_NAME);
    eventsCollection = database.collection(RAW_EVENTS_COLLECTION);
    processedBlocksCollection = database.collection(
      PROCESSED_BLOCKS_COLLECTION
    );
    console.log(`Connected to MongoDB database: ${DATABASE_NAME}`);

    // Ensure indexes exist for performance
    await eventsCollection.createIndex({ projectId: 1, blockHeight: -1 });
    await eventsCollection.createIndex({ transactionId: 1 });
    await eventsCollection.createIndex({ type: 1 });
    await eventsCollection.createIndex({ contractAddress: 1 });
    await eventsCollection.createIndex({ payerAddress: 1 }); // Index new fields
    await eventsCollection.createIndex({ statusCode: 1 });
    await processedBlocksCollection.createIndex(
      { projectId: 1 },
      { unique: true }
    ); // Unique index for progress tracking

    console.log("Database indexes verified/created.");
  } catch (error) {
    console.error("MongoDB connection error:", error);
    process.exit(1); // Exit if DB connection fails
  }
}

// --- Block Height Functions ---
async function getLatestSealedBlockHeight() {
  try {
    // Use fcl.latestBlock() for potentially more robust method
    const latestBlock = await fcl.latestBlock(true); // true for sealed
    return latestBlock.height;
  } catch (error) {
    console.error(`Error getting latest sealed block: ${error.message}`);
    return null;
  }
}

async function getLatestProcessedBlockHeight() {
  try {
    const latestDoc = await processedBlocksCollection.findOne({
      projectId: projectId,
    });
    // Return blockHeight, or 0 if not found (to start from beginning if needed)
    return latestDoc?.blockHeight ?? 0;
  } catch (error) {
    console.error(`Error getting latest processed block: ${error.message}`);
    // Return 0 on error to potentially retry from start or last known good height
    // depending on recovery strategy (or handle error more gracefully)
    return 0;
  }
}

// --- **UPDATED storeEvent Function** ---
// Populates document with event data + enriched tx/status data
async function storeEvent(event, tx, txStatus) {
  try {
    let contractAddress = null;
    try {
      const typeComponents = event.type.split(".");
      if (typeComponents.length >= 3)
        contractAddress = `A.${typeComponents[1]}.${typeComponents[2]}`;
    } catch (e) {
      /* ignore parsing error */
    }

    const eventDoc = {
      // Core event data
      projectId: projectId,
      contractAddress: contractAddress,
      blockHeight: event.blockHeight,
      blockTimestamp: event.blockTimestamp, // FCL provides this as ISO string
      type: event.type,
      transactionId: event.transactionId,
      eventIndex: event.eventIndex,
      data: event.data,
      processedAt: new Date(), // Monitor ingestion timestamp

      // Enriched Fields (initialize)
      payerAddress: null,
      authorizerAddresses: null,
      transactionArguments: null,
      gasLimit: null,
      gasUsed: null, // Will store scaled executionEffort
      statusCode: null,
      errorMessage: null,
    };

    // Populate from tx object if available
    if (tx) {
      eventDoc.payerAddress = tx.payer ?? null;
      eventDoc.authorizerAddresses = tx.authorizers ?? null;
      eventDoc.transactionArguments = tx.args ?? null; // Use tx.args
      if (tx.gasLimit !== undefined && tx.gasLimit !== null) {
        eventDoc.gasLimit = parseInt(tx.gasLimit, 10);
        if (isNaN(eventDoc.gasLimit)) eventDoc.gasLimit = null;
      }
    }

    // Populate from txStatus object if available
    if (txStatus) {
      eventDoc.statusCode = txStatus.statusCode ?? null;
      eventDoc.errorMessage = txStatus.errorMessage || null; // Store empty string as null

      // Extract Gas Used / Execution Effort
      let effortValueScaled = null;
      if (txStatus.gasUsed !== undefined && txStatus.gasUsed !== null) {
        effortValueScaled = parseInt(txStatus.gasUsed, 10);
        if (isNaN(effortValueScaled)) effortValueScaled = null;
      }
      if (
        effortValueScaled === null &&
        txStatus.events &&
        Array.isArray(txStatus.events)
      ) {
        const feesEvent = txStatus.events.find(
          (e) => e.type === "A.f919ee77447b7497.FlowFees.FeesDeducted"
        );
        if (
          feesEvent?.data &&
          typeof feesEvent.data.executionEffort === "string"
        ) {
          try {
            const floatEffort = parseFloat(feesEvent.data.executionEffort);
            effortValueScaled = Math.round(floatEffort * 1e8);
            if (isNaN(effortValueScaled)) effortValueScaled = null;
          } catch {
            effortValueScaled = null;
          }
        }
      }
      if (effortValueScaled !== null) {
        eventDoc.gasUsed = effortValueScaled;
      }
    }

    // Use updateOne with upsert based on a unique key (txId + eventIndex) to prevent duplicates
    // This handles potential overlaps or re-processing gracefully.
    const filter = {
      transactionId: event.transactionId,
      eventIndex: event.eventIndex,
      projectId: projectId, // Ensure uniqueness per project
    };
    const update = { $set: eventDoc };
    const options = { upsert: true };

    await eventsCollection.updateOne(filter, update, options);
    return true;
  } catch (error) {
    console.error(
      `Error storing event (TxID: ${event?.transactionId}, Type: ${event?.type}):`,
      error
    );
    return false;
  }
}

// --- **UPDATED pollEvents Function (using p-limit)** ---
async function pollEvents() {
  let currentPollDelay = POLLING_DELAY; // Start with default delay
  try {
    const latestSealedBlock = await getLatestSealedBlockHeight();
    if (!latestSealedBlock) {
      console.log("Couldn't determine latest sealed block, retrying...");
      setTimeout(pollEvents, CAUGHT_UP_POLL_DELAY); // Wait longer if block height fails
      return;
    }

    const lastProcessedBlock = await getLatestProcessedBlockHeight();
    // Start from next block, or handle initial start (e.g., from latest - N blocks)
    let fromBlockHeight =
      lastProcessedBlock > 0
        ? lastProcessedBlock + 1
        : Math.max(1, latestSealedBlock - 5); // Start 5 blocks back if fresh start

    if (fromBlockHeight > latestSealedBlock) {
      process.stdout.write(
        `Waiting for new blocks... Current: ${latestSealedBlock}\r`
      );
      currentPollDelay = CAUGHT_UP_POLL_DELAY; // Use longer delay when caught up
      setTimeout(pollEvents, currentPollDelay);
      return;
    }

    const toBlockHeight = Math.min(
      fromBlockHeight + BLOCK_POLL_RANGE - 1,
      latestSealedBlock
    );
    // Clear waiting message if present
    process.stdout.clearLine(0);
    process.stdout.cursorTo(0);
    console.log(
      `Polling events from block ${fromBlockHeight} to ${toBlockHeight} (latest: ${latestSealedBlock})`
    );

    const startTime = Date.now();
    let totalEventsFoundInBatch = 0;
    let totalEventsStoredInBatch = 0;
    const txDataCache = new Map();
    const allEventsInBatch = [];

    // 1. Collect all relevant events
    // Consider fetching event types in parallel too if many types
    for (const eventType of eventTypes) {
      try {
        const events = await fcl
          .send([
            fcl.getEventsAtBlockHeightRange(
              eventType,
              fromBlockHeight,
              toBlockHeight
            ),
          ])
          .then(fcl.decode);
        if (events.length > 0) {
          allEventsInBatch.push(...events);
          totalEventsFoundInBatch += events.length;
        }
      } catch (error) {
        if (
          !error.message.includes("cannot get events") &&
          !error.message.includes("event type not found") &&
          !error.message.includes("could not decode sequence number")
        ) {
          console.error(
            `Error querying event type ${eventType}: ${error.message}`
          );
        }
      }
    }

    if (totalEventsFoundInBatch === 0) {
      console.log(
        ` -> No relevant events found in blocks ${fromBlockHeight}-${toBlockHeight}.`
      );
    } else {
      console.log(
        ` -> Found ${totalEventsFoundInBatch} total events. Fetching transaction details...`
      );

      // 2. Get unique transaction IDs
      const uniqueTxIds = [
        ...new Set(allEventsInBatch.map((e) => e.transactionId)),
      ];
      console.log(
        ` -> Processing ${uniqueTxIds.length} unique transaction IDs.`
      );

      // 3. Fetch details in parallel using p-limit
      const limit = pLimit(TX_FETCH_CONCURRENCY);
      const fetchPromises = uniqueTxIds.map((txId) =>
        limit(async () => {
          if (txDataCache.has(txId)) return;
          try {
            // Fetch both concurrently within the limited task
            const [tx, txStatus] = await Promise.all([
              fcl.send([fcl.getTransaction(txId)]).then(fcl.decode),
              fcl.send([fcl.getTransactionStatus(txId)]).then(fcl.decode),
            ]);
            txDataCache.set(txId, { tx, txStatus, error: null });
          } catch (fetchError) {
            // Log less critical errors, perhaps only occasionally or based on error type
            if (!fetchError.message.includes("transaction not found")) {
              // Example: Don't spam for common "not found" errors if expected
              console.warn(
                ` -> WARN: Failed to fetch details for tx ${txId}: ${
                  fetchError.message.split("\n")[0]
                }`
              );
            }
            txDataCache.set(txId, {
              tx: null,
              txStatus: null,
              error: fetchError,
            });
          }
        })
      );
      await Promise.all(fetchPromises); // Wait for fetches

      // 4. Sort events (optional, but good for ordered insertion)
      allEventsInBatch.sort((a, b) => {
        if (a.blockHeight !== b.blockHeight)
          return a.blockHeight - b.blockHeight;
        if (a.transactionIndex !== b.transactionIndex)
          return a.transactionIndex - b.transactionIndex;
        return a.eventIndex - b.eventIndex;
      });

      // 5. Store events using cached data
      for (const event of allEventsInBatch) {
        const cachedData = txDataCache.get(event.transactionId);
        const stored = await storeEvent(
          event,
          cachedData?.tx,
          cachedData?.txStatus
        );
        if (stored) totalEventsStoredInBatch++;
      }
      const duration = (Date.now() - startTime) / 1000;
      console.log(
        ` -> Stored ${totalEventsStoredInBatch}/${totalEventsFoundInBatch} events for range. (${duration.toFixed(
          2
        )}s)`
      );
    }

    // 6. Update processed block height
    await processedBlocksCollection.updateOne(
      { projectId: projectId },
      { $set: { blockHeight: toBlockHeight, updatedAt: new Date() } },
      { upsert: true }
    );

    // 7. Schedule next poll
    currentPollDelay =
      toBlockHeight >= latestSealedBlock ? CAUGHT_UP_POLL_DELAY : POLLING_DELAY;
  } catch (error) {
    console.error(`Error in pollEvents loop: ${error.message}`);
    currentPollDelay = CAUGHT_UP_POLL_DELAY * 2; // Wait longer after a major error
  } finally {
    setTimeout(pollEvents, currentPollDelay); // Schedule next run
  }
}

// --- Main Execution ---
async function main() {
  console.log("Starting Flow event monitoring (Optimized Version)...");
  console.log(`Project ID: ${projectId}`);
  console.log(`Monitoring ${eventTypes.length} event types.`);

  await connectToMongo();
  await pollEvents(); // Start the polling loop
}

// Graceful shutdown
const cleanup = async () => {
  console.log("\nShutting down monitor...");
  await mongoClient.close();
  console.log("MongoDB connection closed.");
  process.exit(0);
};
process.on("SIGINT", cleanup); // Handle Ctrl+C
process.on("SIGTERM", cleanup); // Handle kill commands

main().catch(async (error) => {
  // Ensure async for await mongoClient.close
  console.error("Fatal error during script startup:", error);
  try {
    if (mongoClient) await mongoClient.close(); // Attempt to close client on startup error too
  } catch (closeErr) {
    console.error(
      "Error closing MongoDB client during fatal error handling:",
      closeErr
    );
  } finally {
    process.exit(1);
  }
});
