// backfill_parallel_v1.js
const { MongoClient } = require("mongodb");
const fcl = require("@onflow/fcl");
const { default: pLimit } = require("p-limit");
require("dotenv").config();
const path = require("path");

// --- Configuration ---
const MONGODB_URI = process.env.MONGODB_URI;
const FLOW_ACCESS_NODE = process.env.FLOW_ACCESS_NODE;
const DATABASE_NAME = "flow_events";
const COLLECTION_NAME = "raw_events";

// Concurrency Limit for FCL Calls (Number of simultaneous requests)
// START LOW (e.g., 5-15) and increase carefully based on rate limits/errors
const CONCURRENCY = 8;

// Fields to Add Configuration
const FIELDS_TO_ADD = {
  payerAddress: "payerAddress",
  authorizerAddresses: "authorizerAddresses",
  transactionArguments: "transactionArguments",
  gasLimit: "gasLimit",
  gasUsed: "gasUsed", // Stores scaled executionEffort
  statusCode: "statusCode",
  errorMessage: "errorMessage",
};
const FETCH_TRANSACTION_STATUS = true; // Required for gasUsed, statusCode, errorMessage

// Rate Limiting Delay (can often be much lower with concurrency, e.g., 0-50ms)
// The p-limit library controls concurrency, reducing need for long delays unless specifically rate limited
const DELAY_MS = 40; // Lower delay now, relying on CONCURRENCY limit

const STATUS_UPDATE_INTERVAL = 100; // Update status line every X records scheduled

// --- FCL Setup ---
fcl.config().put("accessNode.api", FLOW_ACCESS_NODE);

// --- Helper Function for ETA Formatting ---
function formatDuration(totalSeconds) {
  if (isNaN(totalSeconds) || !isFinite(totalSeconds) || totalSeconds < 0) {
    return "??h ??m ??s";
  }
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = Math.floor(totalSeconds % 60);
  const hStr = String(hours).padStart(2, "0");
  const mStr = String(minutes).padStart(2, "0");
  const sStr = String(seconds).padStart(2, "0");
  return `${hStr}h ${mStr}m ${sStr}s`;
}

// --- Processing function for a single record ---
// Moved core logic here to be used with p-limit
async function processRecord(eventDoc, collection, countersRef) {
  let skipUpdate = false;
  let foundNewData = false;
  const updatePayload = { $set: { backfilledAt: new Date() } };
  const txId = eventDoc.transactionId;

  try {
    // --- Fetch Transaction Details ---
    const tx = await fcl.send([fcl.getTransaction(txId)]).then(fcl.decode);

    if (tx) {
      // Extract fields if they don't exist
      if (
        FIELDS_TO_ADD.payerAddress &&
        tx.payer &&
        !eventDoc[FIELDS_TO_ADD.payerAddress]
      ) {
        updatePayload.$set[FIELDS_TO_ADD.payerAddress] = tx.payer;
        foundNewData = true;
      }
      if (
        FIELDS_TO_ADD.authorizerAddresses &&
        tx.authorizers &&
        !eventDoc[FIELDS_TO_ADD.authorizerAddresses]
      ) {
        updatePayload.$set[FIELDS_TO_ADD.authorizerAddresses] = tx.authorizers;
        foundNewData = true;
      }
      if (
        FIELDS_TO_ADD.transactionArguments &&
        tx.args &&
        !eventDoc[FIELDS_TO_ADD.transactionArguments]
      ) {
        updatePayload.$set[FIELDS_TO_ADD.transactionArguments] = tx.args;
        foundNewData = true;
      }
      if (
        FIELDS_TO_ADD.gasLimit &&
        tx.gasLimit !== undefined &&
        tx.gasLimit !== null &&
        !eventDoc[FIELDS_TO_ADD.gasLimit]
      ) {
        updatePayload.$set[FIELDS_TO_ADD.gasLimit] = parseInt(tx.gasLimit, 10);
        foundNewData = true;
      }
    } else {
      console.warn(`\n -> Transaction object not found for tx ${txId}.`);
      skipUpdate = true;
    }

    // --- Conditionally Fetch Transaction Status ---
    if (!skipUpdate && FETCH_TRANSACTION_STATUS) {
      const needStatus =
        (FIELDS_TO_ADD.gasUsed && !eventDoc[FIELDS_TO_ADD.gasUsed]) ||
        (FIELDS_TO_ADD.statusCode && !eventDoc[FIELDS_TO_ADD.statusCode]) ||
        (FIELDS_TO_ADD.errorMessage && !eventDoc[FIELDS_TO_ADD.errorMessage]);

      if (needStatus) {
        // Small delay before status call might still be wise if hitting limits hard
        // await new Promise(resolve => setTimeout(resolve, DELAY_MS / 2));
        const txStatus = await fcl
          .send([fcl.getTransactionStatus(txId)])
          .then(fcl.decode);

        if (txStatus) {
          // Status Code
          if (
            FIELDS_TO_ADD.statusCode &&
            txStatus.statusCode !== undefined &&
            txStatus.statusCode !== null &&
            !eventDoc[FIELDS_TO_ADD.statusCode]
          ) {
            updatePayload.$set[FIELDS_TO_ADD.statusCode] = txStatus.statusCode;
            foundNewData = true;
          }
          // Error Message
          if (
            FIELDS_TO_ADD.errorMessage &&
            txStatus.errorMessage &&
            !eventDoc[FIELDS_TO_ADD.errorMessage]
          ) {
            updatePayload.$set[FIELDS_TO_ADD.errorMessage] =
              txStatus.errorMessage;
            foundNewData = true;
          }
          // Gas Used / Execution Effort
          if (FIELDS_TO_ADD.gasUsed && !eventDoc[FIELDS_TO_ADD.gasUsed]) {
            let effortValueScaled = null;
            if (txStatus.gasUsed !== undefined && txStatus.gasUsed !== null) {
              effortValueScaled = parseInt(txStatus.gasUsed, 10);
              if (!isNaN(effortValueScaled)) {
                updatePayload.$set[FIELDS_TO_ADD.gasUsed] = effortValueScaled;
                foundNewData = true;
              } else {
                effortValueScaled = null;
              }
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
                  const floatEffort = parseFloat(
                    feesEvent.data.executionEffort
                  );
                  effortValueScaled = Math.round(floatEffort * 1e8);
                  if (!isNaN(effortValueScaled)) {
                    updatePayload.$set[FIELDS_TO_ADD.gasUsed] =
                      effortValueScaled;
                    foundNewData = true;
                  } else {
                    effortValueScaled = null;
                  }
                } catch (e) {
                  effortValueScaled = null;
                }
              }
            }
            if (effortValueScaled === null) {
              // Optionally log occasional warnings if needed, but less necessary now
            }
          } // end gasUsed check
        } else {
          // end if(txStatus)
          console.warn(`\n -> Transaction status not found for tx ${txId}.`);
          // Potentially skip update if status was essential and not found? Decide based on needs.
        }
      } // end if(needStatus)
    } // end if(!skipUpdate && FETCH_...)

    // --- Update MongoDB Document ---
    if (!skipUpdate && foundNewData) {
      try {
        const updateResult = await collection.updateOne(
          { _id: eventDoc._id },
          updatePayload
        );
        if (updateResult.modifiedCount === 1) {
          countersRef.updated++; // Increment shared counter
        } else {
          countersRef.skipped++; // Already updated or issue
        }
      } catch (updateError) {
        console.error(
          `\n -> ERROR updating MongoDB for doc _id: ${eventDoc._id}:`,
          updateError
        );
        countersRef.errors++; // Increment error counter
      }
    } else if (!skipUpdate && !foundNewData) {
      countersRef.skipped++; // No new data needed
      // Optionally mark as checked:
      // await collection.updateOne({ _id: eventDoc._id }, { $set: { backfilledAt: new Date() } });
    } else {
      countersRef.skipped++; // Skipped due to FCL error etc.
    }
  } catch (error) {
    console.error(
      `\n -> ERROR during FCL processing for ${txId}: ${
        error.message.split("\n")[0]
      }`
    );
    countersRef.errors++; // Increment error counter
  } finally {
    countersRef.completed++; // Increment completed counter regardless of outcome
    // Apply a small delay *after* processing, even with concurrency control, can help pace overall load
    if (DELAY_MS > 0) {
      await new Promise((resolve) => setTimeout(resolve, DELAY_MS));
    }
  }
}

// --- Main Execution ---
async function main() {
  if (!MONGODB_URI || !FLOW_ACCESS_NODE) {
    console.error(
      "FATAL ERROR: MONGODB_URI or FLOW_ACCESS_NODE environment variable is not set."
    );
    process.exit(1);
  }
  // Print config... (same as before)
  console.log(`--- Configuration ---`);
  console.log(`Concurrency: ${CONCURRENCY}`);
  console.log(`Adding Payer: YES (Field: ${FIELDS_TO_ADD.payerAddress})`); // Assume adding if key exists
  console.log(
    `Adding Authorizers: YES (Field: ${FIELDS_TO_ADD.authorizerAddresses})`
  );
  console.log(
    `Adding Arguments: YES (Field: ${FIELDS_TO_ADD.transactionArguments})`
  );
  console.log(`Adding Gas Limit: YES (Field: ${FIELDS_TO_ADD.gasLimit})`);
  console.log(`Workspaceing Status: ${FETCH_TRANSACTION_STATUS}`);
  if (FETCH_TRANSACTION_STATUS) {
    console.log(
      ` -> Adding Gas Used/Effort: YES (Field: ${FIELDS_TO_ADD.gasUsed})`
    );
    console.log(
      ` -> Adding Status Code: YES (Field: ${FIELDS_TO_ADD.statusCode})`
    );
    console.log(
      ` -> Adding Error Message: YES (Field: ${FIELDS_TO_ADD.errorMessage})`
    );
  } else {
    console.log(` -> Skipping Gas Used/Effort, Status Code, Error Message.`);
  }
  console.log(`Base Delay between records: ${DELAY_MS}ms`);
  console.log(
    `Status update interval: ${STATUS_UPDATE_INTERVAL} records scheduled`
  );
  console.log(`--------------------`);
  if (
    !FETCH_TRANSACTION_STATUS &&
    (FIELDS_TO_ADD.gasUsed ||
      FIELDS_TO_ADD.statusCode ||
      FIELDS_TO_ADD.errorMessage)
  ) {
    console.warn(
      "Warning: FETCH_TRANSACTION_STATUS is false, but status-dependent fields are configured. They will not be processed."
    );
  }

  const client = new MongoClient(MONGODB_URI);
  let db, eventsCollection;
  // Use a shared object for counters, passed by reference
  const counters = {
    scheduled: 0,
    completed: 0,
    updated: 0,
    skipped: 0,
    errors: 0,
  };
  let totalDocsToProcess = 0;
  let startTime = Date.now();
  let statusIntervalId = null; // To clear interval later

  try {
    await client.connect();
    db = client.db(DATABASE_NAME);
    eventsCollection = db.collection(COLLECTION_NAME);
    console.log(`Connected to MongoDB: ${DATABASE_NAME}.${COLLECTION_NAME}`);

    const orChecks = [];
    if (FIELDS_TO_ADD.payerAddress)
      orChecks.push({ [FIELDS_TO_ADD.payerAddress]: { $exists: false } });
    if (FIELDS_TO_ADD.authorizerAddresses)
      orChecks.push({
        [FIELDS_TO_ADD.authorizerAddresses]: { $exists: false },
      });
    if (FIELDS_TO_ADD.transactionArguments)
      orChecks.push({
        [FIELDS_TO_ADD.transactionArguments]: { $exists: false },
      });
    if (FIELDS_TO_ADD.gasLimit)
      orChecks.push({ [FIELDS_TO_ADD.gasLimit]: { $exists: false } });
    if (FETCH_TRANSACTION_STATUS) {
      if (FIELDS_TO_ADD.gasUsed)
        orChecks.push({ [FIELDS_TO_ADD.gasUsed]: { $exists: false } });
      if (FIELDS_TO_ADD.statusCode)
        orChecks.push({ [FIELDS_TO_ADD.statusCode]: { $exists: false } });
      if (FIELDS_TO_ADD.errorMessage)
        orChecks.push({ [FIELDS_TO_ADD.errorMessage]: { $exists: false } });
    }
    if (orChecks.length === 0) {
      console.log("No fields configured for backfilling. Exiting.");
      return;
    }
    const query = { $or: orChecks };
    // const query = { backfilledAt: { $exists: false } }; // Alternative

    totalDocsToProcess = await eventsCollection.countDocuments(query);
    if (totalDocsToProcess === 0) {
      console.log("No documents found needing backfill. Exiting.");
      return;
    }

    console.log(
      `Found ${totalDocsToProcess} documents to potentially backfill... Starting process.`
    );
    startTime = Date.now();

    // Setup p-limit
    const limit = pLimit(CONCURRENCY);
    const promises = [];

    // Setup periodic status update using setInterval
    statusIntervalId = setInterval(() => {
      const currentTime = Date.now();
      const elapsedTimeMs = currentTime - startTime;
      const elapsedTimeSec = Math.max(elapsedTimeMs / 1000, 1);
      // Calculate speed based on *completed* tasks for better accuracy
      const itemsPerSecond = counters.completed / elapsedTimeSec;
      // Calculate ETA based on *remaining scheduled* tasks
      const remainingItems = totalDocsToProcess - counters.completed;
      const estimatedRemainingSeconds =
        itemsPerSecond > 0 ? remainingItems / itemsPerSecond : Infinity;
      const percentage = (
        (counters.completed / totalDocsToProcess) *
        100
      ).toFixed(2);
      const eta = formatDuration(estimatedRemainingSeconds);

      const statusLine = `Completed: ${
        counters.completed
      }/${totalDocsToProcess} (${percentage}%) | Speed: ${itemsPerSecond.toFixed(
        1
      )} rec/s | Updated: ${counters.updated} | Skip/Exist: ${
        counters.skipped
      } | Err: ${counters.errors} | Active: ${limit.activeCount} | Pending: ${
        limit.pendingCount
      } | ETA: ${eta} `;

      process.stdout.clearLine(0);
      process.stdout.cursorTo(0);
      process.stdout.write(statusLine);
    }, 2000); // Update status every 2 seconds

    const cursor = eventsCollection.find(query);

    while (await cursor.hasNext()) {
      const eventDoc = await cursor.next();
      counters.scheduled++;

      if (!eventDoc || !eventDoc.transactionId) {
        counters.skipped++; // Count as skipped right away
        counters.completed++; // Also count as completed processing
        continue;
      }

      // Schedule the processing task using the limiter
      promises.push(
        limit(() => processRecord(eventDoc, eventsCollection, counters))
      );

      // Optional: If promise buffer gets huge, await some promises to clear memory,
      // but this can reduce overall throughput. Usually not needed unless RAM is very limited.
      // if (promises.length > 10000) { await Promise.allSettled(promises.slice(0, 5000)); promises.splice(0, 5000); }
    }

    // Wait for all scheduled promises to complete
    console.log(
      `\nAll ${totalDocsToProcess} documents scheduled. Waiting for final ${limit.pendingCount} tasks to complete...`
    );
    await Promise.allSettled(promises); // Use allSettled to ensure all finish even if some reject
  } catch (error) {
    console.error(
      "\nAn unexpected error occurred during the main backfill execution:",
      error
    );
  } finally {
    if (statusIntervalId) clearInterval(statusIntervalId); // Stop the status updates
    process.stdout.write("\n"); // Ensure final summary starts on a new line

    // --- Final Summary ---
    console.log("\n--- Backfill Summary ---");
    console.log(`Total documents targeted: ${totalDocsToProcess}`);
    console.log(`Tasks completed:       ${counters.completed}`);
    console.log(`Successfully updated:    ${counters.updated}`);
    console.log(`Skipped (no tx/exist): ${counters.skipped}`);
    console.log(`Errors (fetch/update): ${counters.errors}`);
    console.log(
      `Total time: ${formatDuration((Date.now() - startTime) / 1000)}`
    );
    console.log("------------------------");

    if (client) {
      await client.close();
      console.log("MongoDB connection closed.");
    }
  }
}

// --- Run the Script ---
main();
