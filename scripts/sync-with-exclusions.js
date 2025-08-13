#!/usr/bin/env node

import dotenv from "dotenv";
dotenv.config();

import { syncOrchestratorService } from "../src/services/syncOrchestrator.js";
import { logger } from "../src/utils/logger.js";

// Tables to exclude due to data complexity issues
const EXCLUDED_TABLES = ["admin_logs", "user"];

console.log("🚀 MetaExodus - Modified Sync (Excluding Problematic Tables)");
console.log("============================================================\n");

console.log(`⚠️  Excluding ${EXCLUDED_TABLES.length} problematic tables:`);
EXCLUDED_TABLES.forEach((table) => console.log(`   - ${table}`));
console.log("");

async function main() {
  try {
    const credentials = {
      username: process.env.DB_REMOTE_USERNAME,
      password: process.env.DB_REMOTE_PASSWORD,
    };

    if (!credentials.username || !credentials.password) {
      throw new Error(
        "Missing required environment variables: DB_REMOTE_USERNAME, DB_REMOTE_PASSWORD"
      );
    }

    // Configure the sync to exclude problematic tables
    syncOrchestratorService.configure({
      excludedTables: EXCLUDED_TABLES,
      continueOnErrors: true,
    });

    console.log("🔄 Starting synchronization...\n");

    await syncOrchestratorService.executeSync(credentials);

    console.log("\n✅ Synchronization completed successfully!");
    console.log(
      `📊 Note: ${EXCLUDED_TABLES.length} tables were excluded due to data complexity.`
    );
    console.log("💡 You can manually handle these tables later if needed.");

    process.exit(0);
  } catch (error) {
    logger.error("Fatal error", error);
    process.exit(1);
  }
}

main();
