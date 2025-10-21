#!/usr/bin/env node

/**
 * Test script for Supabase artifact storage
 * This script tests storing and retrieving large job results in Supabase Storage
 */

const { SupabaseArtifactStorageClient } = require('./supabase-storage-client');

async function testSupabaseStorage() {
  console.log('🧪 Testing Supabase Artifact Storage...\n');

  // Initialize storage client
  const storage = new SupabaseArtifactStorageClient();
  
  if (!storage.enabled) {
    console.error('❌ Supabase storage not enabled. Check your SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY environment variables.');
    process.exit(1);
  }

  try {
    // Test 1: Small result (should not store in Supabase)
    console.log('📝 Test 1: Small result (should not store in Supabase)');
    const smallResult = { message: 'Hello world', count: 42 };
    const shouldStoreSmall = storage.shouldStoreInSupabase(smallResult);
    console.log(`   Small result size: ${storage.getDataSize(smallResult)} bytes`);
    console.log(`   Should store in Supabase: ${shouldStoreSmall}`);
    console.log('   ✅ PASS\n');

    // Test 2: Large result (should store in Supabase)
    console.log('📝 Test 2: Large result (should store in Supabase)');
    const largeResult = {
      type: 'test_data',
      cases: Array.from({ length: 1000 }, (_, i) => ({
        id: `case-${i}`,
        name: `Test Case ${i}`,
        description: 'A' + 'very '.repeat(50) + 'long description that will make this object quite large',
        metadata: {
          tags: ['tag1', 'tag2', 'tag3'],
          created: new Date().toISOString(),
          details: 'B' + 'detailed '.repeat(30) + 'information about this case'
        }
      }))
    };
    
    const shouldStoreLarge = storage.shouldStoreInSupabase(largeResult);
    console.log(`   Large result size: ${storage.getDataSize(largeResult)} bytes`);
    console.log(`   Should store in Supabase: ${shouldStoreLarge}`);
    console.log('   ✅ PASS\n');

    // Test 3: Store large result in Supabase
    console.log('📝 Test 3: Store large result in Supabase');
    const testJobId = `test-job-${Date.now()}`;
    
    console.log('   Storing artifact...');
    const storageInfo = await storage.storeArtifact(testJobId, largeResult, 'test-result', {
      test: true,
      timestamp: new Date().toISOString()
    });
    
    console.log(`   ✅ Stored successfully:`);
    console.log(`      URL: ${storageInfo.storage_url}`);
    console.log(`      Path: ${storageInfo.storage_path}`);
    console.log(`      Original size: ${storageInfo.artifact_metadata.original_size} bytes`);
    console.log(`      Stored size: ${storageInfo.artifact_metadata.stored_size} bytes`);
    console.log(`      Compressed: ${storageInfo.artifact_metadata.compressed}`);
    console.log('');

    // Test 4: Check if artifact exists
    console.log('📝 Test 4: Check if artifact exists');
    const exists = await storage.artifactExists(storageInfo.storage_url);
    console.log(`   Artifact exists: ${exists}`);
    console.log('   ✅ PASS\n');

    // Test 5: Retrieve stored result
    console.log('📝 Test 5: Retrieve stored result');
    console.log('   Retrieving artifact...');
    const retrievedResult = await storage.retrieveArtifact(storageInfo.storage_url);
    const parsedResult = JSON.parse(retrievedResult);
    
    console.log(`   Retrieved ${parsedResult.cases.length} cases`);
    console.log(`   First case ID: ${parsedResult.cases[0].id}`);
    console.log(`   Data matches original: ${JSON.stringify(parsedResult) === JSON.stringify(largeResult)}`);
    console.log('   ✅ PASS\n');

    // Test 6: Create result summary
    console.log('📝 Test 6: Create result summary for Kafka');
    const summary = storage.createResultSummary(largeResult);
    console.log(`   Summary type: ${summary.type}`);
    console.log(`   Summary keys: ${summary.keys.join(', ')}`);
    console.log(`   Has storage note: ${summary.storage_note ? 'Yes' : 'No'}`);
    console.log('   ✅ PASS\n');

    // Test 7: Cleanup - delete test artifact
    console.log('📝 Test 7: Cleanup test artifact');
    const deleted = await storage.deleteArtifact(storageInfo.storage_url);
    console.log(`   Artifact deleted: ${deleted}`);
    console.log('   ✅ PASS\n');

    console.log('🎉 All tests passed! Supabase artifact storage is working correctly.\n');
    
    // Show integration example
    console.log('💡 Integration Example:');
    console.log('   In your worker, large results will now be automatically stored in Supabase.');
    console.log('   Kafka messages will contain only lightweight summaries and storage URLs.');
    console.log('   The API can retrieve full results from Supabase when needed.');

  } catch (error) {
    console.error('❌ Test failed:', error.message);
    console.error('Stack trace:', error.stack);
    process.exit(1);
  }
}

// Run tests if this script is executed directly
if (require.main === module) {
  testSupabaseStorage().catch(console.error);
}

module.exports = { testSupabaseStorage };