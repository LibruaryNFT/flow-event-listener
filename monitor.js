const fcl = require("@onflow/fcl");
const { MongoClient } = require("mongodb");
require("dotenv").config();

// Configure FCL
fcl.config().put("accessNode.api", process.env.FLOW_ACCESS_NODE);

// MongoDB connection
const mongoClient = new MongoClient(process.env.MONGODB_URI);
let eventsCollection;
let processedBlocksCollection;

// Project ID
const projectId = process.env.PROJECT_ID || "flow_monitors";

// Contracts to monitor
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
    events: ["swapNFTsForTSHOT", "commitSwap", "swapTSHOTForNFTs"],
  },
  TOP_SHOT: {
    address: "A.0b2a3299cc857e29.TopShot",
    events: [
      //"PlayCreated",
      //"NewSeriesStarted",
      //"SetCreated",
      //"PlayAddedToSet",
      //"PlayRetiredFromSet",
      //"SetLocked",
      //"MomentMinted",
      //"MomentDestroyed",
      //"SubeditionCreated",
      //"SubeditionAddedToMoment"
    ],
  },
  FAST_BREAK_V1: {
    address: "A.0b2a3299cc857e29.FastBreakV1",
    events: [
      //"FastBreakPlayerCreated",
      //"FastBreakRunCreated",
      //"FastBreakRunStatusChange",
      //"FastBreakGameCreated",
      //"FastBreakGameStatusChange",
      //"FastBreakNFTBurned",
      //"FastBreakGameWinner",
      //"FastBreakGameStatAdded"
    ],
  },
  PACKNFT_DAPPER: {
    address: "A.0b2a3299cc857e29.PackNFT",
    events: [
      //"RevealRequest",
      //"OpenRequest",
      //"Revealed",
      //"Opened",
      //"Minted",
      //"Withdraw",
      //"Deposit",
      //"ContractInitialized"
    ],
  },
  PINNACLE: {
    address: "A.edf9df96c92f4595.Pinnacle",
    events: [
      //"SeriesCreated",
      //"SeriesLocked",
      //"SeriesNameUpdated",
      //"SetCreated",
      //"SetLocked",
      //"SetNameUpdated",
      //"ShapeCreated",
      //"ShapeClosed",
      //"ShapeNameUpdated",
      //"ShapeCurrentPrintingIncremented",
      //"EditionCreated",
      //"EditionClosed",
      //"EditionDescriptionUpdated",
      //"EditionRenderIDUpdated",
      //"EditionRemoved",
      //"EditionTypeCreated",
      //"EditionTypeClosed",
      //"Withdraw",
      //"Deposit",
      //"PinNFTMinted",
      //"NFTXPUpdated",
      //"NFTInscriptionAdded",
      //"NFTInscriptionUpdated",
      //"NFTInscriptionRemoved",
      //"EntityReactivated",
      //"VariantInserted",
      //"Purchased",
      //"OpenEditionNFTBurned"
    ],
  },
  PACKNFT_PINNACLE: {
    address: "A.edf9df96c92f4595.PackNFT",
    events: [
      //"RevealRequest",
      //"OpenRequest",
      //"Revealed",
      //"Opened",
      //"Minted",
      //"Withdraw",
      //"Deposit",
      //"ContractInitialized"
    ],
  },
  NFT_STOREFRONT_V2: {
    address: "A.4eb8a10cb9f87357.NFTStorefrontV2",
    events: [
      // "StorefrontInitialized",
      // "ListingCompleted",
      // "UnpaidReceiver"
    ],
  },
  OFFERS_V2: {
    address: "A.b8ea91944fd51c43.OffersV2",
    events: [
      //  "OfferAvailable",
      // "OfferCompleted"
    ],
  },
  // Adding TopShotMarketV2 events
  TOPSHOT_MARKET_V2: {
    address: "A.c1e4f4f4c4257510.TopShotMarketV2",
    events: [
      // "MomentPriceChanged",
      // "MomentPurchased",
      // "MomentWithdrawn",
      // "CutPercentageChanged"
    ],
  },
  // Adding TopShotMarketV3 events
  TOPSHOT_MARKET_V3: {
    address: "A.c1e4f4f4c4257510.TopShotMarketV3",
    events: [
      // "MomentListed",
      //  "MomentPriceChanged",
      // "MomentPurchased",
      //  "MomentWithdrawn"
    ],
  },
};

// Generate event types for all contracts
function generateEventTypes() {
  const eventTypes = [];

  for (const contract of Object.values(contracts)) {
    for (const event of contract.events) {
      eventTypes.push(`${contract.address}.${event}`);
    }
  }

  return eventTypes;
}

// Get all event types to monitor
const eventTypes = generateEventTypes();

async function connectToMongo() {
  try {
    await mongoClient.connect();
    const database = mongoClient.db("flow_events");
    eventsCollection = database.collection("raw_events");
    processedBlocksCollection = database.collection("processed_blocks");
    console.log("Connected to MongoDB database: flow_events");

    // Create indexes for better query performance
    await eventsCollection.createIndex({ projectId: 1, blockHeight: 1 });
    await eventsCollection.createIndex({ transactionId: 1 });
    await eventsCollection.createIndex({ type: 1 });
    await eventsCollection.createIndex({ contractAddress: 1 });

    console.log("Database indexes created");
  } catch (error) {
    console.error("MongoDB connection error:", error);
    process.exit(1);
  }
}

async function getLatestSealedBlockHeight() {
  try {
    // Get the latest sealed block height from Flow
    const latestBlock = await fcl.send([fcl.getBlock(true)]).then(fcl.decode);

    // Make sure we're using the sealed (finalized) block height
    return latestBlock.height;
  } catch (error) {
    console.error("Error getting latest sealed block:", error);
    // If we can't determine the latest block, return null
    return null;
  }
}

async function getLatestProcessedBlockHeight() {
  try {
    // Check if we have a stored block height in MongoDB
    const latestDoc = await processedBlocksCollection.findOne({
      projectId: projectId,
    });

    if (latestDoc && latestDoc.blockHeight) {
      return latestDoc.blockHeight;
    }

    // If no record found, return null (we'll handle this case)
    return null;
  } catch (error) {
    console.error("Error getting latest processed block:", error);
    return null;
  }
}

async function storeEvent(event) {
  try {
    // Extract the contract address from the event type
    const typeComponents = event.type.split(".");
    const contractAddress = `A.${typeComponents[1]}.${typeComponents[2]}`;

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
    };

    await eventsCollection.insertOne(eventDoc);
    return true;
  } catch (error) {
    console.error("Error storing event:", error);
    return false;
  }
}

async function pollEvents() {
  try {
    // First, get the latest sealed block from Flow
    const latestSealedBlock = await getLatestSealedBlockHeight();
    if (!latestSealedBlock) {
      console.log(
        "Couldn't determine latest sealed block, retrying in 5 seconds..."
      );
      setTimeout(pollEvents, 5000);
      return;
    }

    // Next, get our latest processed block
    const lastProcessedBlock = await getLatestProcessedBlockHeight();

    // Determine where to start polling
    let fromBlockHeight;
    if (lastProcessedBlock) {
      // Continue from the next block after our last processed block
      fromBlockHeight = lastProcessedBlock + 1;
    } else {
      // If we've never processed blocks before, start from 20 blocks ago
      fromBlockHeight = Math.max(1, latestSealedBlock - 20);
    }

    // Make sure we're not trying to go beyond the latest block
    if (fromBlockHeight > latestSealedBlock) {
      console.log(
        `Waiting for new blocks. Latest sealed: ${latestSealedBlock}, Last processed: ${lastProcessedBlock}`
      );
      setTimeout(pollEvents, 2000); // Check for new blocks every 2 seconds
      return;
    }

    // Use a range of max 25 blocks to reduce server load but still process quickly
    const toBlockHeight = Math.min(fromBlockHeight + 24, latestSealedBlock);

    console.log(
      `Polling events from block ${fromBlockHeight} to ${toBlockHeight} (latest: ${latestSealedBlock})`
    );

    let totalEventsFound = 0;
    let totalEventsStored = 0;
    let eventTypeResults = {};

    // Poll for each event type separately to avoid issues with the Flow API
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
          eventTypeResults[eventType] = events.length;
          totalEventsFound += events.length;

          for (const event of events) {
            const stored = await storeEvent(event);
            if (stored) totalEventsStored++;
          }
        }
      } catch (error) {
        // Ignore "cannot get events" errors for nonexistent event types
        if (!error.message.includes("cannot get events")) {
          console.error(
            `Error querying events of type ${eventType}:`,
            error.message
          );
        }
      }
    }

    // Summary of events found
    if (totalEventsFound > 0) {
      console.log(
        `Found and processed ${totalEventsStored}/${totalEventsFound} events`
      );

      // Only show details if events were found
      Object.keys(eventTypeResults).forEach((type) => {
        console.log(`  - ${type}: ${eventTypeResults[type]} events`);
      });
    } else {
      console.log(
        `No events found in blocks ${fromBlockHeight} to ${toBlockHeight}`
      );
    }

    // Always update the processed block height
    await processedBlocksCollection.updateOne(
      { projectId: projectId },
      {
        $set: {
          blockHeight: toBlockHeight,
          updatedAt: new Date(),
          contractAddresses: Object.values(contracts).map((c) => c.address),
        },
      },
      { upsert: true }
    );

    // If we've reached the latest block, wait a short time before checking for new blocks
    if (toBlockHeight >= latestSealedBlock) {
      console.log("Caught up to latest block, waiting for new blocks...");
      setTimeout(pollEvents, 2000);
    } else {
      // Otherwise, continue polling immediately
      setTimeout(pollEvents, 100);
    }
  } catch (error) {
    console.error("Error in poll loop:", error);
    // If there's an error in the main polling loop, try again after a delay
    setTimeout(pollEvents, 5000);
  }
}

async function main() {
  console.log("Starting Flow event monitoring...");
  console.log(`Project ID: ${projectId}`);
  console.log(
    `Monitoring ${Object.keys(contracts).length} contracts with ${
      eventTypes.length
    } event types`
  );

  await connectToMongo();
  await pollEvents();
}

main().catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});
