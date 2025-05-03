// monitor.js (Based on monitor_final_optimized.js with dynamic import and getBlock fixes)

// Keep require for CommonJS modules
const fcl = require("@onflow/fcl");
const { MongoClient } = require("mongodb");
require("dotenv").config();
// p-limit will be imported dynamically

// --- Configuration ---
const FLOW_ACCESS_NODE = process.env.FLOW_ACCESS_NODE;
// FCL config moved inside main async function

const MONGODB_URI = process.env.MONGODB_URI;
const DATABASE_NAME = "flow_events";
const RAW_EVENTS_COLLECTION = "raw_events";
const PROCESSED_BLOCKS_COLLECTION = "processed_blocks";

// MongoDB Client and Collections will be initialized inside main
let mongoClient = null; // Initialize client variable for potential cleanup
let eventsCollection;
let processedBlocksCollection;

const projectId = process.env.PROJECT_ID || "flow_monitors"; // Match project ID

const TX_FETCH_CONCURRENCY = 8; // From your config
const CAUGHT_UP_POLL_DELAY = 5000; // From your config
const POLLING_DELAY = 500; // From your config
const BLOCK_POLL_RANGE = 25; // From your config

// Contracts to monitor (Copied from your script - ADD OTHERS BACK IF NEEDED)
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
    events: [
      "NFTToTSHOTSwapCompleted",
      "TSHOTToNFTSwapInitiated",
      "TSHOTToNFTSwapCompleted",
    ],
  },
};

// Generate event types
function generateEventTypes() {
  const eventTypes = new Set();
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

// --- Helper Functions (Defined outside main async scope) ---

// connectToMongo: Uses global client/collection vars
async function connectToMongo() {
  try {
    if (!mongoClient) {
      mongoClient = new MongoClient(MONGODB_URI);
    }
    // Ensure connection before proceeding
    if (!mongoClient.topology || !mongoClient.topology.isConnected()) {
      console.log("Connecting/Reconnecting MongoDB Client...");
      await mongoClient.connect();
      console.log("MongoDB Client Connected.");
    } else {
      console.log("MongoDB Client already connected.");
    }

    const database = mongoClient.db(DATABASE_NAME);
    // Assign to global vars
    eventsCollection = database.collection(RAW_EVENTS_COLLECTION);
    processedBlocksCollection = database.collection(
      PROCESSED_BLOCKS_COLLECTION
    );
    console.log(`Using MongoDB database: ${DATABASE_NAME}`);

    // Ensure indexes exist
    console.log("Verifying database indexes...");
    await Promise.all([
      eventsCollection.createIndex({ projectId: 1, blockHeight: -1 }),
      eventsCollection.createIndex({ transactionId: 1 }, { unique: false }), // Ensure not unique if duplicates possible before upsert logic was added
      eventsCollection.createIndex({ type: 1 }),
      eventsCollection.createIndex({ contractAddress: 1 }),
      eventsCollection.createIndex({ payerAddress: 1 }),
      eventsCollection.createIndex({ statusCode: 1 }),
      processedBlocksCollection.createIndex({ projectId: 1 }, { unique: true }),
    ]);

    console.log("Database indexes verified/created.");
  } catch (error) {
    console.error("MongoDB connection or indexing error:", error);
    if (mongoClient && typeof mongoClient.close === "function") {
      await mongoClient
        .close()
        .catch((e) =>
          console.error("Error closing client after connection error:", e)
        );
    }
    process.exit(1); // Exit if DB setup fails
  }
}

// *** CORRECTED getLatestSealedBlockHeight function ***
async function getLatestSealedBlockHeight() {
  try {
    // Use fcl.send combined with fcl.getBlock(true) for broader compatibility
    const latestBlock = await fcl
      .send([
        fcl.getBlock(true), // Pass true to get the latest *sealed* block
      ])
      .then(fcl.decode);

    // Add a check to ensure the response is valid
    if (latestBlock && typeof latestBlock.height !== "undefined") {
      return latestBlock.height;
    } else {
      console.error(
        "Error getting latest sealed block: Invalid block data received.",
        latestBlock
      );
      return null;
    }
  } catch (error) {
    // Log the specific error message
    console.error(
      `Error getting latest sealed block via fcl.send/getBlock: ${error.message}`
    );
    return null;
  }
}

// getLatestProcessedBlockHeight: Uses processedBlocksCollection
async function getLatestProcessedBlockHeight() {
  if (!processedBlocksCollection) {
    console.warn(
      "getLatestProcessedBlockHeight called before DB connection ready."
    );
    return 0; // Default to 0 if called too early
  }
  try {
    const latestDoc = await processedBlocksCollection.findOne({
      projectId: projectId,
    });
    return latestDoc?.blockHeight ?? 0; // Default to 0 if not found
  } catch (error) {
    console.error(`Error getting latest processed block: ${error.message}`);
    return 0; // Default to 0 on error
  }
}

// storeEvent: Uses eventsCollection, projectId
async function storeEvent(event, tx, txStatus) {
  if (!eventsCollection) {
    console.warn("storeEvent called before DB connection ready.");
    return false;
  }
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
      projectId: projectId,
      contractAddress: contractAddress,
      blockHeight: event.blockHeight,
      blockTimestamp: event.blockTimestamp,
      type: event.type,
      transactionId: event.transactionId,
      eventIndex: event.eventIndex,
      data: event.data,
      processedAt: new Date(),
      payerAddress: null,
      authorizerAddresses: null,
      transactionArguments: null,
      gasLimit: null,
      gasUsed: null,
      statusCode: null,
      errorMessage: null,
    };

    // Populate from tx
    if (tx) {
      eventDoc.payerAddress = tx.payer ?? null;
      eventDoc.authorizerAddresses = tx.authorizers ?? null;
      eventDoc.transactionArguments = tx.args ?? null;
      if (tx.gasLimit !== undefined && tx.gasLimit !== null) {
        try {
          eventDoc.gasLimit = parseInt(tx.gasLimit, 10);
          if (isNaN(eventDoc.gasLimit)) eventDoc.gasLimit = null;
        } catch {
          eventDoc.gasLimit = null;
        }
      }
    }
    // Populate from txStatus
    if (txStatus) {
      eventDoc.statusCode = txStatus.statusCode ?? null;
      eventDoc.errorMessage = txStatus.errorMessage || null;
      let effortValueScaled = null;
      if (txStatus.gasUsed !== undefined && txStatus.gasUsed !== null) {
        try {
          effortValueScaled = parseInt(txStatus.gasUsed, 10);
          if (isNaN(effortValueScaled)) effortValueScaled = null;
        } catch {
          effortValueScaled = null;
        }
      }
      // Fallback gas calculation from FlowFees event (Your original logic)
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

    // Use upsert based on unique key
    const filter = {
      transactionId: event.transactionId,
      eventIndex: event.eventIndex,
      projectId: projectId,
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

// --- Main Async Function ---
async function startMonitor() {
  // --- Dynamic Import for p-limit ---
  const { default: pLimit } = await import("p-limit");
  const txFetchLimit = pLimit(TX_FETCH_CONCURRENCY);
  console.log(`p-limit imported, concurrency set to ${TX_FETCH_CONCURRENCY}.`);

  // --- FCL Config ---
  if (!FLOW_ACCESS_NODE) {
    console.error(
      "FATAL: FLOW_ACCESS_NODE environment variable is not set. Exiting."
    );
    process.exit(1); // Exit if FCL node not set
  }
  try {
    await fcl.config().put("accessNode.api", FLOW_ACCESS_NODE);
    console.log(`FCL configured for Access Node: ${FLOW_ACCESS_NODE}`);
  } catch (fclError) {
    console.error(`FATAL: Failed to configure FCL: ${fclError.message}`);
    process.exit(1);
  }

  // --- Initialize MongoDB Client ---
  if (!MONGODB_URI) {
    console.error(
      "FATAL: MONGODB_URI environment variable is not set. Exiting."
    );
    process.exit(1); // Exit if DB URI missing
  }
  // Assign to the module-level variable
  mongoClient = new MongoClient(MONGODB_URI);

  // --- Polling Function (defined inside main async scope) ---
  async function pollEvents() {
    let currentPollDelay = POLLING_DELAY;
    try {
      // Check DB connection & Ensure collections are assigned
      if (
        !mongoClient ||
        !mongoClient.topology ||
        !mongoClient.topology.isConnected() ||
        !eventsCollection ||
        !processedBlocksCollection
      ) {
        console.warn(
          "MongoDB disconnected or collections not ready, attempting to connect/reconnect..."
        );
        await connectToMongo();
        // Add a small delay after reconnecting before polling
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }

      const latestSealedBlock = await getLatestSealedBlockHeight();
      if (latestSealedBlock === null) {
        console.log(
          "Couldn't determine latest sealed block, retrying after delay..."
        );
        setTimeout(pollEvents, CAUGHT_UP_POLL_DELAY * 2); // Wait longer
        return;
      }

      const lastProcessedBlock = await getLatestProcessedBlockHeight();
      let fromBlockHeight =
        lastProcessedBlock > 0
          ? lastProcessedBlock + 1
          : Math.max(1, latestSealedBlock - 5);

      if (fromBlockHeight > latestSealedBlock) {
        process.stdout.write(
          `Waiting for new blocks... Current: ${latestSealedBlock}\r`
        );
        currentPollDelay = CAUGHT_UP_POLL_DELAY;
        setTimeout(pollEvents, currentPollDelay);
        return;
      }

      const toBlockHeight = Math.min(
        fromBlockHeight + BLOCK_POLL_RANGE - 1,
        latestSealedBlock
      );
      process.stdout.clearLine(0); // Clear previous waiting message
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
          if (events && events.length > 0) {
            // Check events is not null
            allEventsInBatch.push(...events);
            totalEventsFoundInBatch += events.length;
          }
        } catch (error) {
          // Enhanced error filtering from your script
          if (
            !error.message?.includes("cannot get events") &&
            !error.message?.includes("event type not found") &&
            !error.message?.includes("could not decode sequence number")
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
        const uniqueTxIds = [
          ...new Set(allEventsInBatch.map((e) => e.transactionId)),
        ];
        console.log(
          ` -> Processing ${uniqueTxIds.length} unique transaction IDs.`
        );

        // 3. Fetch details in parallel using p-limit
        const fetchPromises = uniqueTxIds.map((txId) =>
          txFetchLimit(async () => {
            // Use the txFetchLimit defined in outer scope
            if (!txId || txDataCache.has(txId)) return; // Skip null/empty IDs and duplicates
            try {
              const [tx, txStatus] = await Promise.all([
                fcl.send([fcl.getTransaction(txId)]).then(fcl.decode),
                fcl.send([fcl.getTransactionStatus(txId)]).then(fcl.decode),
              ]);
              txDataCache.set(txId, { tx, txStatus, error: null });
            } catch (fetchError) {
              if (!fetchError.message?.includes("transaction not found")) {
                // Less verbose logging
                console.warn(
                  ` -> WARN: Failed to fetch details for tx ${txId}: ${
                    fetchError.message?.split("\n")[0]
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
        await Promise.all(fetchPromises);

        // 4. Sort events
        allEventsInBatch.sort((a, b) => {
          if (a.blockHeight !== b.blockHeight)
            return a.blockHeight - b.blockHeight;
          // Use transactionIndex if available (newer FCL might provide this)
          if (
            a.transactionIndex !== undefined &&
            b.transactionIndex !== undefined &&
            a.transactionIndex !== b.transactionIndex
          )
            return a.transactionIndex - b.transactionIndex;
          return a.eventIndex - b.eventIndex;
        });

        // 5. Store events using cached data
        for (const event of allEventsInBatch) {
          // Add null check for event.transactionId just in case
          if (!event || !event.transactionId) continue;
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
      if (processedBlocksCollection) {
        await processedBlocksCollection.updateOne(
          { projectId: projectId },
          { $set: { blockHeight: toBlockHeight, updatedAt: new Date() } },
          { upsert: true }
        );
      } else {
        console.error(
          "CRITICAL: processedBlocksCollection not available, cannot update progress! Retrying connection..."
        );
        // Force a longer delay and hope connectMongo fixes it next cycle
        currentPollDelay = CAUGHT_UP_POLL_DELAY * 3;
        setTimeout(pollEvents, currentPollDelay);
        return; // Exit this cycle early
      }

      // 7. Schedule next poll
      currentPollDelay =
        toBlockHeight >= latestSealedBlock
          ? CAUGHT_UP_POLL_DELAY
          : POLLING_DELAY;
    } catch (error) {
      console.error(`Error in pollEvents loop: ${error.message}`, error); // Log full error for debugging
      currentPollDelay = CAUGHT_UP_POLL_DELAY * 2; // Wait longer after a major error in the loop
    } finally {
      // Ensure setTimeout is always called to continue the loop
      setTimeout(pollEvents, currentPollDelay);
    }
  } // End pollEvents function definition

  // --- Start Execution Flow ---
  console.log(
    "Starting Flow event monitoring (Optimized Version - Dynamic Import & getBlock Fix)..."
  );
  console.log(`Project ID: ${projectId}`);
  console.log(
    `Monitoring ${eventTypes.length} event types: ${eventTypes.join(", ")}`
  );

  await connectToMongo(); // Initial connection and index check
  await pollEvents(); // Start the polling loop
} // End startMonitor async function

// --- Graceful Shutdown (Copied from your script) ---
const cleanup = async () => {
  console.log("\nShutting down monitor...");
  // Use the module-level mongoClient variable
  if (mongoClient && typeof mongoClient.close === "function") {
    try {
      await mongoClient.close();
      console.log("MongoDB connection closed.");
    } catch (closeErr) {
      console.error(
        "Error closing MongoDB connection during cleanup:",
        closeErr
      );
    }
  } else {
    console.log("MongoDB client already closed or not initialized.");
  }
  process.exit(0);
};
process.on("SIGINT", cleanup); // Handle Ctrl+C
process.on("SIGTERM", cleanup); // Handle kill commands

// --- Execute Main Function ---
startMonitor().catch(async (error) => {
  // Make catch async
  console.error("Fatal error during script startup:", error);
  // Attempt cleanup even on startup error
  if (mongoClient && typeof mongoClient.close === "function") {
    try {
      await mongoClient.close();
      console.log("MongoDB connection closed after startup error.");
    } catch (closeErr) {
      console.error(
        "Error closing MongoDB client during fatal error handling:",
        closeErr
      );
    }
  }
  process.exit(1);
});
