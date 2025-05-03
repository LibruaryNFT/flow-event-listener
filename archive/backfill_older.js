// backfill_final_with_status_v3.js
const { MongoClient } = require("mongodb");
const fcl = require("@onflow/fcl");
require("dotenv").config();
const path = require("path"); // Needed for formatDuration helper potentially, but not strictly required for the core logic here

// --- Configuration ---
const MONGODB_URI = process.env.MONGODB_URI;
const FLOW_ACCESS_NODE = process.env.FLOW_ACCESS_NODE;
const DATABASE_NAME = "flow_events";
const COLLECTION_NAME = "raw_events";

// --- New Fields Configuration ---
const FIELDS_TO_ADD = {
  payerAddress: "payerAddress",
  authorizerAddresses: "authorizerAddresses",
  transactionArguments: "transactionArguments",
  gasLimit: "gasLimit",
  gasUsed: "gasUsed", // Stores scaled executionEffort
  statusCode: "statusCode",
  errorMessage: "errorMessage",
};

// <<< MUST BE true TO FETCH STATUS-related fields (gasUsed, statusCode, errorMessage) >>>
const FETCH_TRANSACTION_STATUS = true;

const BATCH_SIZE = 1; // Process one document at a time
// <<< ADJUST DELAY! START HIGHER (e.g., 150-250ms) IF FETCHING STATUS >>>
const DELAY_MS = 150;
const STATUS_UPDATE_INTERVAL = 50; // Update status line every X records

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

// --- Main Function ---
async function backfillData() {
  if (!MONGODB_URI || !FLOW_ACCESS_NODE) {
    console.error(
      "FATAL ERROR: MONGODB_URI or FLOW_ACCESS_NODE environment variable is not set."
    );
    process.exit(1);
  }

  // --- Print Configuration --- (Same as before)
  console.log(`--- Configuration ---`);
  console.log(`Adding Payer: YES (Field: ${FIELDS_TO_ADD.payerAddress})`);
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
  console.log(`Delay between records: ${DELAY_MS}ms`);
  console.log(`Status update interval: ${STATUS_UPDATE_INTERVAL} records`);
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
  // --- End Configuration Print ---

  const client = new MongoClient(MONGODB_URI);
  let db, eventsCollection;
  let processedCount = 0;
  let updatedCount = 0;
  let skippedCount = 0;
  let errorCount = 0;
  let startTime = Date.now(); // Record start time for ETA

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
      console.log(
        "No fields configured for backfilling in FIELDS_TO_ADD. Exiting."
      );
      return;
    }
    const query = { $or: orChecks };
    // const query = { backfilledAt: { $exists: false } }; // Alternative

    const totalDocsToProcess = await eventsCollection.countDocuments(query);

    if (totalDocsToProcess === 0) {
      console.log(
        "No documents found needing backfill based on current query. Exiting."
      );
      return;
    }

    console.log(
      `Found ${totalDocsToProcess} documents to potentially backfill... Starting process.`
    );
    startTime = Date.now(); // Reset start time just before loop

    const cursor = eventsCollection.find(query).batchSize(BATCH_SIZE);

    while (await cursor.hasNext()) {
      const eventDoc = await cursor.next();
      processedCount++;
      let skipUpdate = false;
      let foundNewData = false;

      if (!eventDoc || !eventDoc.transactionId) {
        // Only log skips occasionally to avoid cluttering status line
        if (processedCount % STATUS_UPDATE_INTERVAL === 1) {
          console.warn(
            `\nSkipping doc _id: ${eventDoc?._id} - Missing transactionId (logged occasionally)`
          );
        }
        skippedCount++;
        continue; // Skip to the status update part
      }

      const txId = eventDoc.transactionId;
      // We won't log individual processing start here to keep console clean for status line

      const updatePayload = { $set: { backfilledAt: new Date() } };

      try {
        // --- Fetch Transaction Details ---
        const tx = await fcl.send([fcl.getTransaction(txId)]).then(fcl.decode);

        if (tx) {
          // Extract Payer
          if (
            FIELDS_TO_ADD.payerAddress &&
            tx.payer &&
            !eventDoc[FIELDS_TO_ADD.payerAddress]
          ) {
            updatePayload.$set[FIELDS_TO_ADD.payerAddress] = tx.payer;
            foundNewData = true;
          }
          // Extract Authorizers
          if (
            FIELDS_TO_ADD.authorizerAddresses &&
            tx.authorizers &&
            !eventDoc[FIELDS_TO_ADD.authorizerAddresses]
          ) {
            updatePayload.$set[FIELDS_TO_ADD.authorizerAddresses] =
              tx.authorizers;
            foundNewData = true;
          }
          // Extract Arguments (Using tx.args)
          if (
            FIELDS_TO_ADD.transactionArguments &&
            tx.args &&
            !eventDoc[FIELDS_TO_ADD.transactionArguments]
          ) {
            updatePayload.$set[FIELDS_TO_ADD.transactionArguments] = tx.args;
            foundNewData = true;
          }
          // Extract Gas Limit
          if (
            FIELDS_TO_ADD.gasLimit &&
            tx.gasLimit !== undefined &&
            tx.gasLimit !== null &&
            !eventDoc[FIELDS_TO_ADD.gasLimit]
          ) {
            updatePayload.$set[FIELDS_TO_ADD.gasLimit] = parseInt(
              tx.gasLimit,
              10
            ); // Store as number
            foundNewData = true;
          }
        } else {
          // Log occasionally
          if (processedCount % STATUS_UPDATE_INTERVAL === 1)
            console.warn(
              `\n -> Transaction object not found for tx ${txId} (logged occasionally)`
            );
          skippedCount++;
          skipUpdate = true;
        }

        // --- Conditionally Fetch Transaction Status ---
        if (!skipUpdate && FETCH_TRANSACTION_STATUS) {
          const needStatus =
            (FIELDS_TO_ADD.gasUsed && !eventDoc[FIELDS_TO_ADD.gasUsed]) ||
            (FIELDS_TO_ADD.statusCode && !eventDoc[FIELDS_TO_ADD.statusCode]) ||
            (FIELDS_TO_ADD.errorMessage &&
              !eventDoc[FIELDS_TO_ADD.errorMessage]);

          if (needStatus) {
            // Log fetch occasionally
            // if (processedCount % STATUS_UPDATE_INTERVAL === 1) console.log(`\n -> Fetching status for tx ${txId} (logged occasionally)`);
            await new Promise((resolve) => setTimeout(resolve, DELAY_MS / 3));

            const txStatus = await fcl
              .send([fcl.getTransactionStatus(txId)])
              .then(fcl.decode);

            if (txStatus) {
              // Extract Status Code
              if (
                FIELDS_TO_ADD.statusCode &&
                txStatus.statusCode !== undefined &&
                txStatus.statusCode !== null &&
                !eventDoc[FIELDS_TO_ADD.statusCode]
              ) {
                updatePayload.$set[FIELDS_TO_ADD.statusCode] =
                  txStatus.statusCode;
                foundNewData = true;
              }
              // Extract Error Message
              if (
                FIELDS_TO_ADD.errorMessage &&
                txStatus.errorMessage &&
                !eventDoc[FIELDS_TO_ADD.errorMessage]
              ) {
                updatePayload.$set[FIELDS_TO_ADD.errorMessage] =
                  txStatus.errorMessage;
                foundNewData = true;
              }
              // Extract Gas Used / Execution Effort
              if (FIELDS_TO_ADD.gasUsed && !eventDoc[FIELDS_TO_ADD.gasUsed]) {
                let effortValueScaled = null;
                if (
                  txStatus.gasUsed !== undefined &&
                  txStatus.gasUsed !== null
                ) {
                  effortValueScaled = parseInt(txStatus.gasUsed, 10);
                  if (!isNaN(effortValueScaled)) {
                    updatePayload.$set[FIELDS_TO_ADD.gasUsed] =
                      effortValueScaled;
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
                  const feesEventType =
                    "A.f919ee77447b7497.FlowFees.FeesDeducted";
                  const feesEvent = txStatus.events.find(
                    (e) => e.type === feesEventType
                  );
                  if (
                    feesEvent &&
                    feesEvent.data &&
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
                    } catch (parseError) {
                      effortValueScaled = null;
                    }
                  }
                }
                if (
                  effortValueScaled === null &&
                  processedCount % STATUS_UPDATE_INTERVAL === 1
                ) {
                  // Log warning occasionally if we expected gas but didn't find it
                  console.warn(
                    `\n   -> Could not determine GasUsed/Effort for tx ${txId} (logged occasionally)`
                  );
                }
              } // End gasUsed check
            } else {
              // End if(txStatus)
              if (processedCount % STATUS_UPDATE_INTERVAL === 1)
                console.warn(
                  `\n -> Transaction status not found for tx ${txId} (logged occasionally)`
                );
            }
          } // End if(needStatus)
        } // End if(!skipUpdate && FETCH...)
      } catch (error) {
        errorCount++;
        if (processedCount % STATUS_UPDATE_INTERVAL === 1) {
          // Log errors occasionally
          console.error(
            `\n -> ERROR during FCL fetch for ${txId}: ${
              error.message.split("\n")[0]
            } (logged occasionally)`
          );
        }
        skipUpdate = true;
        await new Promise((resolve) => setTimeout(resolve, DELAY_MS * 2)); // Shorter delay on error now
      }

      // --- Update MongoDB Document ---
      if (!skipUpdate && foundNewData) {
        try {
          const updateResult = await eventsCollection.updateOne(
            { _id: eventDoc._id },
            updatePayload
          );
          if (updateResult.modifiedCount === 1) updatedCount++;
          else skippedCount++; // Didn't modify (maybe updated between find/update)
        } catch (updateError) {
          errorCount++;
          if (processedCount % STATUS_UPDATE_INTERVAL === 1)
            console.error(
              `\n -> ERROR updating MongoDB for doc _id: ${eventDoc._id}: ${updateError.message} (logged occasionally)`
            );
        }
      } else if (!skipUpdate && !foundNewData) {
        skippedCount++; // No new data needed, count as skipped
        // Optionally mark as checked even if no data changed:
        // await eventsCollection.updateOne({ _id: eventDoc._id }, { $set: { backfilledAt: new Date() } });
      }

      // --- Delay ---
      await new Promise((resolve) => setTimeout(resolve, DELAY_MS));

      // --- STATUS UPDATE ---
      if (
        processedCount % STATUS_UPDATE_INTERVAL === 0 ||
        processedCount === totalDocsToProcess
      ) {
        const currentTime = Date.now();
        const elapsedTimeMs = currentTime - startTime;
        const elapsedTimeSec = Math.max(elapsedTimeMs / 1000, 1); // Avoid division by zero, ensure min 1 sec
        const itemsPerSecond = processedCount / elapsedTimeSec;
        const remainingItems = totalDocsToProcess - processedCount;
        const estimatedRemainingSeconds = remainingItems / itemsPerSecond;
        const percentage = (
          (processedCount / totalDocsToProcess) *
          100
        ).toFixed(2);
        const eta = formatDuration(estimatedRemainingSeconds);

        const statusLine = `Processed: ${processedCount}/${totalDocsToProcess} (${percentage}%) | Speed: ${itemsPerSecond.toFixed(
          1
        )} rec/s | Updated: ${updatedCount} | Skip/Exist: ${skippedCount} | Err: ${errorCount} | ETA: ${eta} `;

        process.stdout.clearLine(0); // Clear current line
        process.stdout.cursorTo(0); // Move cursor to beginning of line
        process.stdout.write(statusLine); // Write new status line
      }
      // --- END STATUS UPDATE ---
    } // End while loop

    process.stdout.write("\n"); // Move to next line after loop finishes

    console.log("\n--- Backfill Summary ---");
    console.log(`Total documents checked: ${processedCount}`);
    console.log(`Successfully updated:    ${updatedCount}`);
    console.log(`Skipped (no tx/up-to-date): ${skippedCount}`);
    console.log(`Errors (fetch/update): ${errorCount}`);
    console.log(
      `Total time: ${formatDuration((Date.now() - startTime) / 1000)}`
    );
    console.log("------------------------");
  } catch (error) {
    console.error(
      "\nAn unexpected error occurred during the backfill process:",
      error
    );
  } finally {
    if (client) {
      await client.close();
      console.log("MongoDB connection closed.");
    }
  }
}

// --- Run the Script ---
backfillData();
