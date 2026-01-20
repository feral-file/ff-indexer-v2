#!/usr/bin/env node

const fs = require('fs');
const path = require('path');

// Configuration
const CIDS_FILE = '/Users/jollyjoker992/Downloads/cids.json';
const GRAPHQL_ENDPOINT = 'https://indexer-v2.feralfile.com/graphql';
const BATCH_SIZE = 50;
const SLEEP_MS = 1000; // 1 second sleep between batches

// Sleep function
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// GraphQL mutation to trigger token indexing
const TRIGGER_TOKEN_INDEXING_MUTATION = `
  mutation TriggerMetadataIndexing($tokenCids: [String!]!) {
    triggerMetadataIndexing(token_cids: $tokenCids) {
      workflow_id
      run_id
    }
  }
`;

async function triggerIndexing(tokenCids) {
  const response = await fetch(GRAPHQL_ENDPOINT, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      query: TRIGGER_TOKEN_INDEXING_MUTATION,
      variables: {
        tokenCids: tokenCids,
      },
    }),
  });

  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`);
  }

  const result = await response.json();
  
  if (result.errors) {
    throw new Error(`GraphQL errors: ${JSON.stringify(result.errors)}`);
  }

  return result.data.triggerMetadataIndexing;
}

async function main() {
  try {
    console.log(`Reading CIDs from ${CIDS_FILE}...`);
    const fileContent = fs.readFileSync(CIDS_FILE, 'utf8');
    const cids = JSON.parse(fileContent);

    if (!Array.isArray(cids)) {
      throw new Error('Expected cids.json to be an array of strings');
    }

    console.log(`Found ${cids.length} CIDs to index`);
    console.log(`Batching into groups of ${BATCH_SIZE}...`);

    const batches = [];
    for (let i = 0; i < cids.length; i += BATCH_SIZE) {
      batches.push(cids.slice(i, i + BATCH_SIZE));
    }

    console.log(`Created ${batches.length} batches`);
    console.log(`Starting indexing with ${SLEEP_MS}ms delay between batches...\n`);

    let successCount = 0;
    let errorCount = 0;

    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];
      const batchNum = i + 1;

      console.log(`[Batch ${batchNum}/${batches.length}] Indexing ${batch.length} tokens...`);
      
      try {
        const result = await triggerIndexing(batch);
        console.log(`  ✓ Success: workflow_id=${result.workflow_id}, run_id=${result.run_id}`);
        successCount += batch.length;
      } catch (error) {
        console.error(`  ✗ Error: ${error.message}`);
        errorCount += batch.length;
      }

      // Sleep before next batch (except after the last one)
      if (i < batches.length - 1) {
        console.log(`  Waiting ${SLEEP_MS}ms before next batch...\n`);
        await sleep(SLEEP_MS);
      }
    }

    console.log(`\n=== Summary ===`);
    console.log(`Total CIDs: ${cids.length}`);
    console.log(`Successfully indexed: ${successCount}`);
    console.log(`Failed: ${errorCount}`);
    console.log(`Batches processed: ${batches.length}`);

  } catch (error) {
    console.error('Fatal error:', error.message);
    process.exit(1);
  }
}

main();
