'use strict';

/**
 * Unit tests for dynamodb-stream-consumer/dynamo-processing.js
 * @author Byron du Preez
 */

const test = require('tape');

// The test subject
const dynamoProcessing = require('../dynamo-processing');

const streamProcessing = require('aws-stream-consumer/stream-processing');

const sequencing = require('aws-stream-consumer/sequencing');
const persisting = require('aws-stream-consumer/persisting');

// Other dependencies
const Settings = require('aws-stream-consumer/settings');
const StreamType = Settings.StreamType;

const Batch = require('aws-stream-consumer/batch');

// External dependencies
const logging = require('logging-utils');

const Promises = require('core-functions/promises');
const base64 = require('core-functions/base64');
const regions = require('aws-core-utils/regions');
const stages = require('aws-core-utils/stages');
const kinesisCache = require('aws-core-utils/kinesis-cache');
const dynamoDBDocClientCache = require('aws-core-utils/dynamodb-doc-client-cache');
const streamEvents = require('aws-core-utils/stream-events');

const samples = require('./samples');
const strings = require('core-functions/strings');
const stringify = strings.stringify;
const isNotBlank = strings.isNotBlank;
const isBlank = strings.isBlank;

function sampleAwsEvent(tableName, streamTimestamp, omitEventSourceARN) {
  const region = process.env.AWS_REGION;
  if (isBlank(tableName)) {
    tableName = 'TestTable_DEV';
  }
  if (isBlank(streamTimestamp)) {
    streamTimestamp = '2017-03-13T21:35:49';
  }
  const eventSourceARN = omitEventSourceARN ? undefined :
    samples.sampleDynamoDBEventSourceArn(region, tableName, streamTimestamp);
  return samples.awsDynamoDBUpdateSampleEvent(eventSourceARN);
}

function sampleAwsContext(functionVersion, functionAlias) {
  const region = process.env.AWS_REGION;
  const functionName = 'sample-lambda-function';
  const invokedFunctionArn = samples.sampleInvokedFunctionArn(region, functionName, functionAlias);
  return samples.sampleAwsContext(functionName, functionVersion, invokedFunctionArn);
}

function dummyKinesis(t, prefix, error, requestToResult) {
  return {
    putRecord(request) {
      return {
        promise() {
          return new Promise((resolve, reject) => {
            t.pass(`${prefix} simulated putRecord to Kinesis with request (${stringify(request)})`);
            if (error)
              reject(error);
            else {
              resolve(typeof requestToResult === 'function' ? requestToResult(request) : {});
            }
          })
        }
      }
    }
  };
}

function sampleMessage() {
  return {
    name: 'Sample Message',
    dob: new Date().toISOString(),
    num: 123,
    address: {
      lat: 123.456,
      lon: -67.890
    },
    tags: ['a', 'b']
  };
}

// Dummy extract messages from record functions
const extractMessagesFromRecord1 = (record, batch, extractMessageFromRecord, context) => record;
const extractMessagesFromRecord2 = (record, batch, extractMessageFromRecord, context) => record;

// Dummy extract message from record functions
const extractMessageFromRecord1 = (record, userRecord, context) => record; // for DynamoDB config
const extractMessageFromRecord2 = (record, userRecord, context) => record; // for DynamoDB config

// Dummy generate MD5s functions
let i = 1;
// noinspection JSUnusedLocalSymbols
const generateMD5s1 = (message, record, userRecord) => {
  ++i;
  return {msg: `${i}`, rec: `${i}`, userRec: `${i}`, data: `${i}`};
};
// noinspection JSUnusedLocalSymbols
const generateMD5s2 = (message, record, userRecord) => {
  ++i;
  return {msg: `${i}`, rec: `${i}`, userRec: `${i}`, data: `${i}`};
};

// Dummy resolve event id and sequence numbers functions
// noinspection JSUnusedLocalSymbols
const resolveEventIdAndSeqNos1 = (record, userRecord) => {
  ++i;
  return {eventID: `${i}`, eventSeqNo: `${i}`, eventSubSeqNo: `${i}`};
};
// noinspection JSUnusedLocalSymbols
const resolveEventIdAndSeqNos2 = (record, userRecord) => {
  ++i;
  return {eventID: `${i}`, eventSeqNo: `${i}`, eventSubSeqNo: `${i}`};
};

// Dummy resolve message ids and sequence numbers functions
// noinspection JSUnusedLocalSymbols
const resolveMessageIdsAndSeqNos1 = (message, record, userRecord, eventIdAndSeqNos, md5s, context) => {
  ++i;
  return {ids: [['i1', i]], keys: [['k1', i]], seqNos: [['s1', i]]};
};
// noinspection JSUnusedLocalSymbols
const resolveMessageIdsAndSeqNos2 = (message, record, userRecord, eventIdAndSeqNos, md5s, context) => {
  ++i;
  return {ids: [['i1', i]], keys: [['k1', i]], seqNos: [['s1', i]]};
};

// Dummy load batch state functions
const loadBatchState1 = (batch, context) => batch;
const loadBatchState2 = (batch, context) => batch;

// Dummy save batch state functions
const saveBatchState1 = (batch, context) => batch;
const saveBatchState2 = (batch, context) => batch;

// // Dummy handle incomplete messages functions
// const handleIncompleteMessages1 = (batch, incompleteMessages, context) => incompleteMessages;
// const handleIncompleteMessages2 = (batch, incompleteMessages, context) => incompleteMessages;

// Dummy discard unusable record functions
const discardUnusableRecord1 = (unusableRecord, batch, context) => unusableRecord;
const discardUnusableRecord2 = (unusableRecord, batch, context) => unusableRecord;

// Dummy discard rejected message functions
const discardRejectedMessage1 = (rejectedMessage, batch, context) => rejectedMessage;
const discardRejectedMessage2 = (rejectedMessage, batch, context) => rejectedMessage;

function toOptions(streamType, sequencingRequired, sequencingPerKey, batchKeyedOnEventID, consumerIdSuffix, consumerId,
  timeoutAtPercentageOfRemainingTime, maxNumberOfAttempts, batchStateTableName, deadRecordQueueName, deadMessageQueueName,
  idPropertyNames, keyPropertyNames, seqNoPropertyNames) {

  return {
    // generic settings
    streamType: streamType,
    sequencingRequired: sequencingRequired,
    sequencingPerKey: sequencingPerKey,
    batchKeyedOnEventID: batchKeyedOnEventID,
    consumerIdSuffix: consumerIdSuffix,
    consumerId: consumerId,
    timeoutAtPercentageOfRemainingTime: timeoutAtPercentageOfRemainingTime,
    maxNumberOfAttempts: maxNumberOfAttempts,

    // specialised settings needed by default implementations
    idPropertyNames: idPropertyNames,
    keyPropertyNames: keyPropertyNames,
    seqNoPropertyNames: seqNoPropertyNames,

    batchStateTableName: batchStateTableName,
    deadRecordQueueName: deadRecordQueueName,
    deadMessageQueueName: deadMessageQueueName
  };
}

function toSettings(streamType, sequencingRequired, sequencingPerKey, batchKeyedOnEventID, consumerIdSuffix, consumerId,
  timeoutAtPercentageOfRemainingTime, maxNumberOfAttempts, extractMessagesFromRecord, extractMessageFromRecord,
  generateMD5s, resolveEventIdAndSeqNos, resolveMessageIdsAndSeqNos, loadBatchState, saveBatchState,
  discardUnusableRecord, discardRejectedMessage, batchStateTableName, deadRecordQueueName,
  deadMessageQueueName, idPropertyNames, keyPropertyNames, seqNoPropertyNames) {

  return {
    // generic settings
    streamType: streamType,
    sequencingRequired: sequencingRequired,
    sequencingPerKey: sequencingPerKey,
    batchKeyedOnEventID: batchKeyedOnEventID,
    consumerIdSuffix: consumerIdSuffix,
    consumerId: consumerId,
    timeoutAtPercentageOfRemainingTime: timeoutAtPercentageOfRemainingTime,
    maxNumberOfAttempts: maxNumberOfAttempts,

    idPropertyNames: idPropertyNames,
    keyPropertyNames: keyPropertyNames,
    seqNoPropertyNames: seqNoPropertyNames,

    // functions
    extractMessagesFromRecord: extractMessagesFromRecord,
    extractMessageFromRecord: extractMessageFromRecord,
    generateMD5s: generateMD5s,
    resolveEventIdAndSeqNos: resolveEventIdAndSeqNos,
    resolveMessageIdsAndSeqNos: resolveMessageIdsAndSeqNos,
    loadBatchState: loadBatchState,
    saveBatchState: saveBatchState,
    discardUnusableRecord: discardUnusableRecord,
    discardRejectedMessage: discardRejectedMessage,

    // specialised settings needed by default implementations
    batchStateTableName: batchStateTableName,
    deadRecordQueueName: deadRecordQueueName,
    deadMessageQueueName: deadMessageQueueName
  };
}

function toSettingsWithFunctionsOnly(streamType, extractMessagesFromRecord, extractMessageFromRecord,
  generateMD5s, resolveEventIdAndSeqNos, resolveMessageIdsAndSeqNos, loadBatchState,
  saveBatchState, discardUnusableRecord, discardRejectedMessage) {
  return {
    // functions
    extractMessagesFromRecord: extractMessagesFromRecord,
    extractMessageFromRecord: extractMessageFromRecord,
    generateMD5s: generateMD5s,
    resolveEventIdAndSeqNos: resolveEventIdAndSeqNos,
    resolveMessageIdsAndSeqNos: resolveMessageIdsAndSeqNos,
    loadBatchState: loadBatchState,
    saveBatchState: saveBatchState,
    discardUnusableRecord: discardUnusableRecord,
    discardRejectedMessage: discardRejectedMessage
  };
}

function checkSettings(t, streamProcessing, context, before, mustChange, expectedSettings) {
  t.ok(streamProcessing.isStreamProcessingConfigured(context), `Default stream processing must be configured now`);

  const after = context.streamProcessing;

  const expectedStreamType = mustChange ? expectedSettings.streamType : before.streamType;

  const expectedSequencingRequired = mustChange ? expectedSettings.sequencingRequired : before.sequencingRequired;
  const expectedSequencingPerKey = mustChange ? expectedSettings.sequencingPerKey : before.sequencingPerKey;
  const expectedBatchKeyedOnEventID = mustChange ? expectedSettings.batchKeyedOnEventID : before.batchKeyedOnEventID;
  const expectedKplEncoded = mustChange ? expectedSettings.kplEncoded : before.kplEncoded;
  const expectedConsumerIdSuffix = mustChange ? expectedSettings.consumerIdSuffix : before.consumerIdSuffix;
  const expectedConsumerId = mustChange ? expectedSettings.consumerId : before.consumerId;
  const expectedTimeoutAtPercentageOfRemainingTime = mustChange ? expectedSettings.timeoutAtPercentageOfRemainingTime : before.timeoutAtPercentageOfRemainingTime;
  const expectedMaxNumberOfAttempts = mustChange ? expectedSettings.maxNumberOfAttempts : before.maxNumberOfAttempts;

  const expectedIdPropertyNames = mustChange ? expectedSettings.idPropertyNames : before.idPropertyNames;
  const expectedKeyPropertyNames = mustChange ? expectedSettings.keyPropertyNames : before.keyPropertyNames;
  const expectedSeqNoPropertyNames = mustChange ? expectedSettings.seqNoPropertyNames : before.seqNoPropertyNames;

  const expectedExtractMessagesFromRecord = mustChange ? expectedSettings.extractMessagesFromRecord : before.extractMessagesFromRecord;
  const expectedExtractMessageFromRecord = mustChange ? expectedSettings.extractMessageFromRecord : before.extractMessageFromRecord;
  const expectedGenerateMD5s = mustChange ? expectedSettings.generateMD5s : before.generateMD5s;
  const expectedResolveEventIdAndSeqNos = mustChange ? expectedSettings.resolveEventIdAndSeqNos : before.resolveEventIdAndSeqNos;
  const expectedResolveMessageIdsAndSeqNos = mustChange ? expectedSettings.resolveMessageIdsAndSeqNos : before.resolveMessageIdsAndSeqNos;
  const expectedLoadState = mustChange ? expectedSettings.loadBatchState : before.loadBatchState;
  const expectedSaveState = mustChange ? expectedSettings.saveBatchState : before.saveBatchState;
  const expectedDiscardUnusableRecord = mustChange ? expectedSettings.discardUnusableRecord : before.discardUnusableRecord;
  const expectedDiscardRejectedMessage = mustChange ? expectedSettings.discardRejectedMessage : before.discardRejectedMessage;

  const expectedBatchStateTableName = mustChange ? expectedSettings.batchStateTableName : before.batchStateTableName;
  const expectedDeadRecordQueueName = mustChange ? expectedSettings.deadRecordQueueName : before.deadRecordQueueName;
  const expectedDeadMessageQueueName = mustChange ? expectedSettings.deadMessageQueueName : before.deadMessageQueueName;

  t.equal(after.streamType, expectedStreamType, `streamType must be ${expectedStreamType}`);
  t.equal(after.sequencingRequired, expectedSequencingRequired, `sequencingRequired must be ${expectedSequencingRequired}`);
  t.equal(after.sequencingPerKey, expectedSequencingPerKey, `sequencingPerKey must be ${expectedSequencingPerKey}`);
  t.equal(after.batchKeyedOnEventID, expectedBatchKeyedOnEventID, `batchKeyedOnEventID must be ${expectedBatchKeyedOnEventID}`);
  t.equal(after.kplEncoded, expectedKplEncoded, `kplEncoded must be ${expectedKplEncoded}`);
  t.equal(after.consumerIdSuffix, expectedConsumerIdSuffix, `consumerIdSuffix must be ${expectedConsumerIdSuffix}`);
  t.equal(after.consumerId, expectedConsumerId, `consumerId must be ${expectedConsumerId}`);
  t.equal(after.timeoutAtPercentageOfRemainingTime, expectedTimeoutAtPercentageOfRemainingTime, `timeoutAtPercentageOfRemainingTime must be ${expectedTimeoutAtPercentageOfRemainingTime}`);
  t.equal(after.maxNumberOfAttempts, expectedMaxNumberOfAttempts, `maxNumberOfAttempts must be ${expectedMaxNumberOfAttempts}`);

  t.deepEqual(after.idPropertyNames, expectedIdPropertyNames, `idPropertyNames must be ${stringify(expectedIdPropertyNames)}`);
  t.deepEqual(after.keyPropertyNames, expectedKeyPropertyNames, `keyPropertyNames must be ${stringify(expectedKeyPropertyNames)}`);
  t.deepEqual(after.seqNoPropertyNames, expectedSeqNoPropertyNames, `seqNoPropertyNames must be ${stringify(expectedSeqNoPropertyNames)}`);

  t.equal(after.extractMessagesFromRecord, expectedExtractMessagesFromRecord, `extractMessagesFromRecord must be ${stringify(expectedExtractMessagesFromRecord)}`);
  t.equal(after.extractMessageFromRecord, expectedExtractMessageFromRecord, `extractMessageFromRecord must be ${stringify(expectedExtractMessageFromRecord)}`);
  t.equal(after.generateMD5s, expectedGenerateMD5s, `generateMD5s must be ${stringify(expectedGenerateMD5s)}`);
  t.equal(after.resolveEventIdAndSeqNos, expectedResolveEventIdAndSeqNos, `resolveEventIdAndSeqNos must be ${stringify(expectedResolveEventIdAndSeqNos)}`);
  t.equal(after.resolveMessageIdsAndSeqNos, expectedResolveMessageIdsAndSeqNos, `resolveMessageIdsAndSeqNos must be ${stringify(expectedResolveMessageIdsAndSeqNos)}`);
  t.equal(after.loadBatchState, expectedLoadState, `loadBatchState must be ${stringify(expectedLoadState)}`);
  t.equal(after.saveBatchState, expectedSaveState, `saveBatchState must be ${stringify(expectedSaveState)}`);
  t.equal(after.discardUnusableRecord, expectedDiscardUnusableRecord, `discardUnusableRecord must be ${stringify(expectedDiscardUnusableRecord)}`);
  t.equal(after.discardRejectedMessage, expectedDiscardRejectedMessage, `discardRejectedMessage must be ${stringify(expectedDiscardRejectedMessage)}`);

  t.equal(after.batchStateTableName, expectedBatchStateTableName, `batchStateTableName must be ${expectedBatchStateTableName}`);
  t.equal(after.deadRecordQueueName, expectedDeadRecordQueueName, `deadRecordQueueName must be ${expectedDeadRecordQueueName}`);
  t.equal(after.deadMessageQueueName, expectedDeadMessageQueueName, `deadMessageQueueName must be ${expectedDeadMessageQueueName}`);
}

function checkDependencies(t, context, stdSettings, stdOptions, event, awsContext, expectedStage) {
  t.ok(logging.isLoggingConfigured(context), `logging must be configured`);
  t.ok(stages.isStageHandlingConfigured(context), `stage handling must be configured`);
  t.ok(context.custom && typeof context.custom === 'object', `context.custom must be configured`);

  const kinesisOptions = stdSettings && stdSettings.kinesisOptions ? stdSettings.kinesisOptions :
    stdOptions && stdOptions.kinesisOptions ? stdOptions.kinesisOptions : undefined;

  const dynamoDBDocClientOptions = stdSettings && stdSettings.dynamoDBDocClientOptions ? stdSettings.dynamoDBDocClientOptions :
    stdOptions && stdOptions.dynamoDBDocClientOptions ? stdOptions.dynamoDBDocClientOptions : undefined;

  // Check Kinesis instance is also configured
  const region = regions.getRegion();
  if (kinesisOptions) {
    t.ok(context.kinesis && typeof context.kinesis === 'object', 'context.kinesis must be configured');
    t.equal(context.kinesis.config.region, region, `context.kinesis.config.region (${context.kinesis.config.region}) must be ${region}`);
    t.equal(context.kinesis.config.maxRetries, kinesisOptions.maxRetries, `context.kinesis.config.maxRetries (${context.kinesis.config.maxRetries}) must be ${kinesisOptions.maxRetries}`);
  } else {
    t.notOk(context.kinesis, 'context.kinesis must not be configured');
  }

  // Check DynamoDB DocumentClient instance is also configured
  if (dynamoDBDocClientOptions) {
    // Check DynamoDB.DocumentClient is also configured
    t.ok(context.dynamoDBDocClient && typeof context.dynamoDBDocClient === 'object', 'context.dynamoDBDocClient must be configured');
    t.equal(context.dynamoDBDocClient.service.config.region, region, `context.dynamoDBDocClient.service.config.region (${context.dynamoDBDocClient.service.config.region}) must be ${region}`);
    t.equal(context.dynamoDBDocClient.service.config.maxRetries, dynamoDBDocClientOptions.maxRetries,
      `context.dynamoDBDocClient.service.config.maxRetries (${context.dynamoDBDocClient.service.config.maxRetries}) must be ${dynamoDBDocClientOptions.maxRetries}`);
  } else {
    t.notOk(context.dynamoDBDocClient, 'context.dynamoDBDocClient must not be configured');
  }

  if (event && awsContext) {
    t.equal(context.region, region, `context.region must be ${region}`);
    t.equal(context.stage, expectedStage, `context.stage must be ${expectedStage}`);
    t.equal(context.awsContext, awsContext, 'context.awsContext must be given AWS context');
  }
}

function checkConfigureStreamProcessingWithSettings(t, streamProcessing, context, settings, stdSettings, stdOptions, event, awsContext, forceConfiguration, expectedSettings, expectedStage) {
  const before = context.streamProcessing;
  const mustChange = forceConfiguration || !streamProcessing.isStreamProcessingConfigured(context);

  const c = streamProcessing.configureStreamProcessingWithSettings(context, settings, stdSettings, stdOptions, event, awsContext, forceConfiguration);

  t.ok(c === context, `Context returned must be given context`);
  checkSettings(t, streamProcessing, context, before, mustChange, expectedSettings);
  checkDependencies(t, context, stdSettings, stdOptions, event, awsContext, expectedStage);
}

function checkConfigureStreamProcessing(t, streamProcessing, context, settings, options, stdSettings, stdOptions, event, awsContext, forceConfiguration, expectedSettings, expectedStage) {
  const before = context.streamProcessing;
  const mustChange = forceConfiguration || !streamProcessing.isStreamProcessingConfigured(context);

  const c = streamProcessing.configureStreamProcessing(context, settings, options, stdSettings, stdOptions, event, awsContext, forceConfiguration);

  t.ok(c === context, `Context returned must be given context`);
  checkSettings(t, streamProcessing, context, before, mustChange, expectedSettings);
  checkDependencies(t, context, stdSettings, stdOptions, event, awsContext, expectedStage);
}

function setRegionStageAndDeleteCachedInstances(region, stage) {
  // Set up region
  process.env.AWS_REGION = region;
  // Set up stage
  process.env.STAGE = stage;
  // Remove any cached entries before configuring
  deleteCachedInstances();
  return region;
}

function deleteCachedInstances() {
  const region = regions.getRegion();
  kinesisCache.deleteKinesis(region);
  dynamoDBDocClientCache.deleteDynamoDBDocClient(region);
}

// =====================================================================================================================
// isStreamProcessingConfigured
// =====================================================================================================================

test('isStreamProcessingConfigured for DynamoDB with default (i.e. minimalist sequenced) options', t => {
  const context = {};
  t.notOk(dynamoProcessing.isStreamProcessingConfigured(context), `DynamoDB stream processing must NOT be configured yet`);

  dynamoProcessing.configureDefaultDynamoDBStreamProcessing(context);

  t.ok(dynamoProcessing.isStreamProcessingConfigured(context), `DynamoDB stream processing must be configured now`);
  t.equal(context.streamProcessing.sequencingRequired, true, `sequencingRequired must be true`);
  t.equal(context.streamProcessing.sequencingPerKey, false, `sequencingPerKey must be false`);

  t.end();
});

test('isStreamProcessingConfigured for DynamoDB with minimalist sequencing per key options', t => {
  const context = {};
  t.notOk(dynamoProcessing.isStreamProcessingConfigured(context), `DynamoDB stream processing must NOT be configured yet`);

  const minimalistSequencePerKeyOptions = {sequencingPerKey: true};
  dynamoProcessing.configureDefaultDynamoDBStreamProcessing(context, minimalistSequencePerKeyOptions);

  t.ok(dynamoProcessing.isStreamProcessingConfigured(context), `DynamoDB stream processing must be configured now`);
  t.equal(context.streamProcessing.sequencingRequired, true, `sequencingRequired must be true`);
  t.equal(context.streamProcessing.sequencingPerKey, true, `sequencingPerKey must be true`);

  t.end();
});

test('isStreamProcessingConfigured for DynamoDB with minimalist unsequenced options', t => {
  const context = {};
  t.notOk(dynamoProcessing.isStreamProcessingConfigured(context), `DynamoDB stream processing must NOT be configured yet`);

  const minimalistUnsequencedOptions = {sequencingRequired: false};
  // t.throws(() => dynamoProcessing.configureDefaultDynamoDBStreamProcessing(context, minimalistUnsequencedOptions));
  dynamoProcessing.configureDefaultDynamoDBStreamProcessing(context, minimalistUnsequencedOptions);

  t.ok(dynamoProcessing.isStreamProcessingConfigured(context), `DynamoDB stream processing must be configured now`);
  t.equal(context.streamProcessing.sequencingRequired, false, `sequencingRequired must be false`);
  t.equal(context.streamProcessing.sequencingPerKey, false, `sequencingPerKey must be false`);

  t.end();
});

// =====================================================================================================================
// configureStreamProcessingWithSettings without event & awsContext
// =====================================================================================================================

test('configureStreamProcessingWithSettings without event & awsContext', t => {
  function check(streamProcessing, context, settings, stdSettings, stdOptions, forceConfiguration, expectedSettings) {
    return checkConfigureStreamProcessingWithSettings(t, streamProcessing, context, settings, stdSettings, stdOptions, undefined, undefined, forceConfiguration, expectedSettings, undefined);
  }

  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', 'dev99');

    const context = {};

    t.notOk(dynamoProcessing.isStreamProcessingConfigured(context), `Stream processing must NOT be configured yet`);

    // Configure for the first time
    const settings1 = toSettings(StreamType.dynamodb, true, true, true, 'consumerIdSuffix1', 'myTasks1', 0.75, 2, extractMessagesFromRecord1, extractMessageFromRecord1, generateMD5s1, resolveEventIdAndSeqNos1, resolveMessageIdsAndSeqNos1, loadBatchState1, saveBatchState1, discardUnusableRecord1, discardRejectedMessage1, 'batchStateTableName1', 'DRQ1', 'DMQ1', [], ["a", "b"], []);
    const expectedSettings1 = toSettings(StreamType.dynamodb, true, true, true, 'consumerIdSuffix1', 'myTasks1', 0.75, 2, extractMessagesFromRecord1, extractMessageFromRecord1, generateMD5s1, resolveEventIdAndSeqNos1, resolveMessageIdsAndSeqNos1, loadBatchState1, saveBatchState1, discardUnusableRecord1, discardRejectedMessage1, 'batchStateTableName1', 'DRQ1', 'DMQ1', [], ["a", "b"], []);
    check(dynamoProcessing, context, settings1, undefined, dynamoProcessing.loadDynamoDBDefaultOptions(), false, expectedSettings1);

    // Don't force a different configuration
    context.kinesis = undefined;
    context.dynamoDBDocClient = undefined;
    deleteCachedInstances();
    const settings2 = toSettings(StreamType.dynamodb, false, false, false, 'consumerIdSuffix_NoOverride2', 'myTasks_NoOverride2', 0.81, 77, extractMessagesFromRecord2, extractMessageFromRecord2, generateMD5s2, resolveEventIdAndSeqNos2, resolveMessageIdsAndSeqNos2, loadBatchState2, saveBatchState2, discardUnusableRecord2, discardRejectedMessage2, 'batchStateTableName2', 'DRQ2', 'DMQ2', [], ["c", "d"], []);
    check(dynamoProcessing, context, settings2, undefined, dynamoProcessing.loadDynamoDBDefaultOptions(), false, expectedSettings1);

    // Force a new configuration
    context.kinesis = undefined;
    context.dynamoDBDocClient = undefined;
    deleteCachedInstances();
    const settings3 = toSettings(StreamType.dynamodb, true, false, true, 'consumerIdSuffix_Override3', 'myTasks_Override3', 0.91, 7, extractMessagesFromRecord2, extractMessageFromRecord2, generateMD5s2, resolveEventIdAndSeqNos2, resolveMessageIdsAndSeqNos2, loadBatchState2, saveBatchState2, discardUnusableRecord2, discardRejectedMessage2, 'batchStateTableName3', 'DRQ3', 'DMQ3', [], ["c", "d"], []);
    const expectedSettings3 = toSettings(StreamType.dynamodb, true, false, true, 'consumerIdSuffix_Override3', 'myTasks_Override3', 0.91, 7, extractMessagesFromRecord2, extractMessageFromRecord2, generateMD5s2, resolveEventIdAndSeqNos2, resolveMessageIdsAndSeqNos2, loadBatchState2, saveBatchState2, discardUnusableRecord2, discardRejectedMessage2, 'batchStateTableName3', 'DRQ3', 'DMQ3', [], ["c", "d"], []);
    check(dynamoProcessing, context, settings3, undefined, dynamoProcessing.loadDynamoDBDefaultOptions(), true, expectedSettings3);

  } finally {
    process.env.STAGE = undefined;
    process.env.AWS_REGION = undefined;
  }
  t.end();
});

// =====================================================================================================================
// configureStreamProcessingWithSettings with event & awsContext
// =====================================================================================================================

test('configureStreamProcessingWithSettings with event & awsContext', t => {
  function check(streamProcessing, context, settings, stdSettings, stdOptions, event, awsContext, forceConfiguration, expectedSettings, expectedStage) {
    return checkConfigureStreamProcessingWithSettings(t, streamProcessing, context, settings, stdSettings, stdOptions, event, awsContext, forceConfiguration, expectedSettings, expectedStage);
  }

  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");
    const expectedStage = 'dev99';

    const context = {};

    t.notOk(dynamoProcessing.isStreamProcessingConfigured(context), `Stream processing must NOT be configured yet`);

    // Generate a sample AWS event
    const event = sampleAwsEvent('TestTable_DEV2', undefined, false);

    // Generate a sample AWS context
    const awsContext = sampleAwsContext('1.0.1', 'dev1');

    // Configure for the first time
    const settings1 = toSettings(StreamType.dynamodb, true, true, true, 'consumerIdSuffix1', 'myTasks1', 0.75, 2, extractMessagesFromRecord1, extractMessageFromRecord1, generateMD5s1, resolveEventIdAndSeqNos1, resolveMessageIdsAndSeqNos1, loadBatchState1, saveBatchState1, discardUnusableRecord1, discardRejectedMessage1, 'batchStateTableName1', 'DRQ1', 'DMQ1', [], ["a", "b"], []);
    const expectedSettings1 = toSettings(StreamType.dynamodb, true, true, true, 'consumerIdSuffix1', 'myTasks1', 0.75, 2, extractMessagesFromRecord1, extractMessageFromRecord1, generateMD5s1, resolveEventIdAndSeqNos1, resolveMessageIdsAndSeqNos1, loadBatchState1, saveBatchState1, discardUnusableRecord1, discardRejectedMessage1, 'batchStateTableName1', 'DRQ1', 'DMQ1', [], ["a", "b"], []);
    check(dynamoProcessing, context, settings1, undefined, dynamoProcessing.loadDynamoDBDefaultOptions(), event, awsContext, false, expectedSettings1, expectedStage);

    // Don't force a different configuration
    context.kinesis = undefined;
    context.dynamoDBDocClient = undefined;
    deleteCachedInstances();
    const settings2 = toSettings(StreamType.dynamodb, false, false, false, 'consumerIdSuffix_NoOverride2', 'myTasks_NoOverride2', 0.81, 77, extractMessagesFromRecord2, extractMessageFromRecord2, generateMD5s2, resolveEventIdAndSeqNos2, resolveMessageIdsAndSeqNos2, loadBatchState2, saveBatchState2, discardUnusableRecord2, discardRejectedMessage2, 'batchStateTableName2', 'DRQ2', 'DMQ2', [], ["a", "b"], []);
    check(dynamoProcessing, context, settings2, undefined, dynamoProcessing.loadDynamoDBDefaultOptions(), event, awsContext, false, expectedSettings1, expectedStage);

    // Force a new configuration
    context.kinesis = undefined;
    context.dynamoDBDocClient = undefined;
    deleteCachedInstances();
    const settings3 = toSettings(StreamType.dynamodb, true, false, true, 'consumerIdSuffix_Override3', 'myTasks_Override3', 0.91, 7, extractMessagesFromRecord2, extractMessageFromRecord2, generateMD5s2, resolveEventIdAndSeqNos2, resolveMessageIdsAndSeqNos2, loadBatchState2, saveBatchState2, discardUnusableRecord2, discardRejectedMessage2, 'batchStateTableName3', 'DRQ3', 'DMQ3', [], ["a", "b"], []);
    const expectedSettings3 = toSettings(StreamType.dynamodb, true, false, true, 'consumerIdSuffix_Override3', 'myTasks_Override3', 0.91, 7, extractMessagesFromRecord2, extractMessageFromRecord2, generateMD5s2, resolveEventIdAndSeqNos2, resolveMessageIdsAndSeqNos2, loadBatchState2, saveBatchState2, discardUnusableRecord2, discardRejectedMessage2, 'batchStateTableName3', 'DRQ3', 'DMQ3', [], ["a", "b"], []);
    check(dynamoProcessing, context, settings3, undefined, dynamoProcessing.loadDynamoDBDefaultOptions(), event, awsContext, true, expectedSettings3, expectedStage);

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

// =====================================================================================================================
// configureStreamProcessing with options only
// =====================================================================================================================

test('configureStreamProcessing with options only', t => {
  function check(streamProcessing, context, settings, options, stdSettings, stdOptions, event, awsContext, forceConfiguration, expectedSettings, expectedStage) {
    return checkConfigureStreamProcessing(t, streamProcessing, context, settings, options, stdSettings, stdOptions, event, awsContext, forceConfiguration, expectedSettings, expectedStage);
  }

  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");

    const context = {};

    t.notOk(dynamoProcessing.isStreamProcessingConfigured(context), `Stream processing must NOT be configured yet`);

    // Configure for the first time to DynamoDB consumer
    const options1 = toOptions(StreamType.dynamodb, true, true, true, 'consumerIdSuffix1', 'myTasks1', 0.75, 2, 'batchStateTableName1', 'DRQ1', 'DMQ1', [], ["a", "b"], []);
    const expectedSettings1 = toSettings(StreamType.dynamodb, true, true, true, 'consumerIdSuffix1', 'myTasks1', 0.75, 2, dynamoProcessing.extractMessagesFromDynamoDBRecord, dynamoProcessing.extractMessageFromDynamoDBRecord, dynamoProcessing.generateDynamoMD5s, dynamoProcessing.resolveDynamoEventIdAndSeqNos, dynamoProcessing.resolveDynamoMessageIdsAndSeqNos, persisting.loadBatchStateFromDynamoDB, persisting.saveBatchStateToDynamoDB, dynamoProcessing.discardUnusableRecordToDRQ, dynamoProcessing.discardRejectedMessageToDMQ, 'batchStateTableName1', 'DRQ1', 'DMQ1', [], ["a", "b"], []);
    check(dynamoProcessing, context, undefined, options1, undefined, dynamoProcessing.loadDynamoDBDefaultOptions(), undefined, undefined, false, expectedSettings1, undefined);

    // Don't force a different configuration to Kinesis unsequenced
    context.kinesis = undefined;
    context.dynamoDBDocClient = undefined;
    deleteCachedInstances();
    const options2 = toOptions(StreamType.dynamodb, true, false, true, 'consumerIdSuffix_NoOverride2', 'myTasks_NoOverride2', 0.81, 77, 'batchStateTableName2', 'DRQ2', 'DMQ2', [], ["c", "d"], []);
    const stdOptions = dynamoProcessing.loadDynamoDBDefaultOptions();
    check(dynamoProcessing, context, undefined, options2, undefined, stdOptions, undefined, undefined, false, expectedSettings1, undefined);

    // Force a new configuration to Kinesis unsequenced consumer
    context.kinesis = undefined;
    context.dynamoDBDocClient = undefined;
    deleteCachedInstances();
    const options3 = toOptions(StreamType.dynamodb, false, false, true, 'consumerIdSuffix_Override3', 'myTasks_Override3', 0.91, 7, 'batchStateTableName3', 'DRQ3', 'DMQ3', [], [], []);
    const expectedSettings3 = toSettings(StreamType.dynamodb, false, false, true, 'consumerIdSuffix_Override3', 'myTasks_Override3', 0.91, 7, dynamoProcessing.extractMessagesFromDynamoDBRecord, dynamoProcessing.extractMessageFromDynamoDBRecord, dynamoProcessing.generateDynamoMD5s, dynamoProcessing.resolveDynamoEventIdAndSeqNos, dynamoProcessing.resolveDynamoMessageIdsAndSeqNos, persisting.loadBatchStateFromDynamoDB, persisting.saveBatchStateToDynamoDB, dynamoProcessing.discardUnusableRecordToDRQ, dynamoProcessing.discardRejectedMessageToDMQ, 'batchStateTableName3', 'DRQ3', 'DMQ3', [], [], []);
    check(dynamoProcessing, context, undefined, options3, undefined, stdOptions, undefined, undefined, true, expectedSettings3, undefined);

    // Force another new configuration to Kinesis sequenced consumer
    context.kinesis = undefined;
    context.dynamoDBDocClient = undefined;
    deleteCachedInstances();
    const options4 = toOptions(StreamType.dynamodb, true, true, true, 'consumerIdSuffix_Override4', 'myTasks_Override4', 0.92, 8, 'batchStateTableName4', 'DRQ4', 'DMQ4', [], ["e"], []);
    const expectedSettings4 = toSettings(StreamType.dynamodb, true, true, true, 'consumerIdSuffix_Override4', 'myTasks_Override4', 0.92, 8, dynamoProcessing.extractMessagesFromDynamoDBRecord, dynamoProcessing.extractMessageFromDynamoDBRecord, dynamoProcessing.generateDynamoMD5s, dynamoProcessing.resolveDynamoEventIdAndSeqNos, dynamoProcessing.resolveDynamoMessageIdsAndSeqNos, persisting.loadBatchStateFromDynamoDB, persisting.saveBatchStateToDynamoDB, dynamoProcessing.discardUnusableRecordToDRQ, dynamoProcessing.discardRejectedMessageToDMQ, 'batchStateTableName4', 'DRQ4', 'DMQ4', [], ["e"], []);
    check(dynamoProcessing, context, undefined, options4, undefined, stdOptions, undefined, undefined, true, expectedSettings4, undefined);

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

// =====================================================================================================================
// configureStreamProcessing with settings only
// =====================================================================================================================

test('configureStreamProcessing with settings only', t => {

  function check(streamProcessing, context, settings, options, stdSettings, stdOptions, event, awsContext, forceConfiguration, expectedSettings, expectedStage) {
    return checkConfigureStreamProcessing(t, streamProcessing, context, settings, options, stdSettings, stdOptions, event, awsContext, forceConfiguration, expectedSettings, expectedStage);
  }

  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");

    const context = {};

    t.notOk(dynamoProcessing.isStreamProcessingConfigured(context), `Stream processing must NOT be configured yet`);

    // Configure for the first time
    const settings1 = toSettings(StreamType.dynamodb, true, true, true, 'consumerIdSuffix1', 'myTasks1', 0.75, 2, extractMessagesFromRecord1, extractMessageFromRecord1, generateMD5s1, resolveEventIdAndSeqNos1, resolveMessageIdsAndSeqNos1, loadBatchState1, saveBatchState1, discardUnusableRecord1, discardRejectedMessage1, 'batchStateTableName1', 'DRQ1', 'DMQ1', [], ["a", "b"], []);
    const expectedSettings1 = toSettings(StreamType.dynamodb, true, true, true, 'consumerIdSuffix1', 'myTasks1', 0.75, 2, extractMessagesFromRecord1, extractMessageFromRecord1, generateMD5s1, resolveEventIdAndSeqNos1, resolveMessageIdsAndSeqNos1, loadBatchState1, saveBatchState1, discardUnusableRecord1, discardRejectedMessage1, 'batchStateTableName1', 'DRQ1', 'DMQ1', [], ["a", "b"], []);
    check(dynamoProcessing, context, settings1, undefined, undefined, dynamoProcessing.loadDynamoDBDefaultOptions(), undefined, undefined, false, expectedSettings1, undefined);

    // Don't force a different configuration
    context.kinesis = undefined;
    context.dynamoDBDocClient = undefined;
    deleteCachedInstances();
    const settings2 = toSettings(StreamType.dynamodb, true, false, true, 'consumerIdSuffix_NoOverride2', 'myTasks_NoOverride2', 0.81, 77, extractMessagesFromRecord2, extractMessageFromRecord2, generateMD5s2, resolveEventIdAndSeqNos2, resolveMessageIdsAndSeqNos2, loadBatchState2, saveBatchState2, discardUnusableRecord2, discardRejectedMessage2, 'batchStateTableName2', 'DRQ2', 'DMQ2', [], ["c", "d"], []);
    check(dynamoProcessing, context, settings2, undefined, undefined, dynamoProcessing.loadDynamoDBDefaultOptions(), undefined, undefined, false, expectedSettings1, undefined);

    // Force a new configuration
    context.kinesis = undefined;
    context.dynamoDBDocClient = undefined;
    deleteCachedInstances();
    const settings3 = toSettings(StreamType.dynamodb, true, false, true, 'consumerIdSuffix_Override3', 'myTasks_Override3', 0.91, 7, extractMessagesFromRecord2, extractMessageFromRecord2, generateMD5s2, resolveEventIdAndSeqNos2, resolveMessageIdsAndSeqNos2, loadBatchState2, saveBatchState2, discardUnusableRecord2, discardRejectedMessage2, 'batchStateTableName3', 'DRQ3', 'DMQ3', [], ["c", "d"], []);
    const expectedSettings3 = toSettings(StreamType.dynamodb, true, false, true, 'consumerIdSuffix_Override3', 'myTasks_Override3', 0.91, 7, extractMessagesFromRecord2, extractMessageFromRecord2, generateMD5s2, resolveEventIdAndSeqNos2, resolveMessageIdsAndSeqNos2, loadBatchState2, saveBatchState2, discardUnusableRecord2, discardRejectedMessage2, 'batchStateTableName3', 'DRQ3', 'DMQ3', [], ["c", "d"], []);
    check(dynamoProcessing, context, settings3, undefined, undefined, dynamoProcessing.loadDynamoDBDefaultOptions(), undefined, undefined, true, expectedSettings3, undefined);

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

// =====================================================================================================================
// configureStreamProcessing with settings and options
// =====================================================================================================================

test('configureStreamProcessing with settings and options', t => {
  function check(streamProcessing, context, settings, options, stdSettings, stdOptions, event, awsContext, forceConfiguration, expectedSettings, expectedStage) {
    return checkConfigureStreamProcessing(t, streamProcessing, context, settings, options, stdSettings, stdOptions, event, awsContext, forceConfiguration, expectedSettings, expectedStage);
  }

  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");

    const context = {};

    t.notOk(dynamoProcessing.isStreamProcessingConfigured(context), `Stream processing must NOT be configured yet`);

    // Configure for the first time
    const settings1 = toSettingsWithFunctionsOnly(StreamType.dynamodb, extractMessagesFromRecord1, extractMessageFromRecord1, generateMD5s1, resolveEventIdAndSeqNos1, resolveMessageIdsAndSeqNos1, loadBatchState1, saveBatchState1, discardUnusableRecord1, discardRejectedMessage1);
    const options1 = toOptions(StreamType.dynamodb, true, true, true, 'consumerIdSuffix1', 'myTasks1', 0.75, 2, 'batchStateTableName1', 'DRQ1', 'DMQ1', [], ["a", "b"], []);
    const expectedSettings1 = toSettings(StreamType.dynamodb, true, true, true, 'consumerIdSuffix1', 'myTasks1', 0.75, 2, extractMessagesFromRecord1, extractMessageFromRecord1, generateMD5s1, resolveEventIdAndSeqNos1, resolveMessageIdsAndSeqNos1, loadBatchState1, saveBatchState1, discardUnusableRecord1, discardRejectedMessage1, 'batchStateTableName1', 'DRQ1', 'DMQ1', [], ["a", "b"], []);

    check(dynamoProcessing, context, settings1, options1, undefined, dynamoProcessing.loadDynamoDBDefaultOptions(), undefined, undefined, false, expectedSettings1, undefined);

    // Don't force a different configuration
    context.kinesis = undefined;
    context.dynamoDBDocClient = undefined;
    deleteCachedInstances();
    const settings2 = toSettingsWithFunctionsOnly(StreamType.dynamodb, extractMessagesFromRecord2, extractMessageFromRecord2, generateMD5s2, resolveEventIdAndSeqNos2, resolveMessageIdsAndSeqNos2, loadBatchState2, saveBatchState2, discardUnusableRecord2, discardRejectedMessage2);
    const options2 = toOptions(StreamType.dynamodb, true, false, true, 'consumerIdSuffix_NoOverride2', 'myTasks_NoOverride2', 0.81, 77, 'batchStateTableName2', 'DRQ2', 'DMQ2', [], ["a", "b"], []);
    check(dynamoProcessing, context, settings2, options2, undefined, dynamoProcessing.loadDynamoDBDefaultOptions(), undefined, undefined, false, expectedSettings1, undefined);

    // Force a new configuration
    context.kinesis = undefined;
    context.dynamoDBDocClient = undefined;
    deleteCachedInstances();
    const settings3 = toSettingsWithFunctionsOnly(StreamType.dynamodb, extractMessagesFromRecord2, extractMessageFromRecord2, generateMD5s2, resolveEventIdAndSeqNos2, resolveMessageIdsAndSeqNos2, loadBatchState2, saveBatchState2, discardUnusableRecord2, discardRejectedMessage2);
    const options3 = toOptions(StreamType.dynamodb, true, false, true, 'consumerIdSuffix_Override3', 'myTasks_Override3', 0.91, 7, 'batchStateTableName3', 'DRQ3', 'DMQ3', [], ["a", "b"], []);
    const expectedSettings3 = toSettings(StreamType.dynamodb, true, false, true, 'consumerIdSuffix_Override3', 'myTasks_Override3', 0.91, 7, extractMessagesFromRecord2, extractMessageFromRecord2, generateMD5s2, resolveEventIdAndSeqNos2, resolveMessageIdsAndSeqNos2, loadBatchState2, saveBatchState2, discardUnusableRecord2, discardRejectedMessage2, 'batchStateTableName3', 'DRQ3', 'DMQ3', [], ["a", "b"], []);
    check(dynamoProcessing, context, settings3, options3, undefined, dynamoProcessing.loadDynamoDBDefaultOptions(), undefined, undefined, true, expectedSettings3, undefined);

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

// =====================================================================================================================
// configureStreamProcessing with settings, options, event and awsContext
// =====================================================================================================================

test('configureStreamProcessing with settings, options, event and awsContext', t => {
  function check(streamProcessing, context, settings, options, stdSettings, stdOptions, event, awsContext, forceConfiguration, expectedSettings, expectedStage) {
    return checkConfigureStreamProcessing(t, streamProcessing, context, settings, options, stdSettings, stdOptions, event, awsContext, forceConfiguration, expectedSettings, expectedStage);
  }

  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");
    const expectedStage = 'dev99';

    const context = {};

    t.notOk(dynamoProcessing.isStreamProcessingConfigured(context), `Stream processing must NOT be configured yet`);

    // Generate a sample AWS event
    const event = sampleAwsEvent('TestTable_DEV2', undefined, false);

    // Generate a sample AWS context
    const awsContext = sampleAwsContext('1.0.1', 'dev1');

    // Configure for the first time
    const settings1 = toSettings(StreamType.dynamodb, true, true, true, 'consumerIdSuffix1', 'myTasks1', 0.75, 2, extractMessagesFromRecord1, extractMessageFromRecord1, generateMD5s1, resolveEventIdAndSeqNos1, resolveMessageIdsAndSeqNos1, loadBatchState1, saveBatchState1, discardUnusableRecord1, discardRejectedMessage1, 'batchStateTableName1', 'DRQ1', 'DMQ1', [], ["a", "b"], []);
    const options1 = toOptions(StreamType.dynamodb, false, false, false, 'consumerIdSuffix1B', 'myTasks1B', 0.74, 1, 'batchStateTableName1B', 'DRQ1B', 'DMQ1B', [], ["a", "b"], []);
    const expectedSettings1 = toSettings(StreamType.dynamodb, true, true, true, 'consumerIdSuffix1', 'myTasks1', 0.75, 2, extractMessagesFromRecord1, extractMessageFromRecord1, generateMD5s1, resolveEventIdAndSeqNos1, resolveMessageIdsAndSeqNos1, loadBatchState1, saveBatchState1, discardUnusableRecord1, discardRejectedMessage1, 'batchStateTableName1', 'DRQ1', 'DMQ1', [], ["a", "b"], []);

    check(dynamoProcessing, context, settings1, options1, undefined, dynamoProcessing.loadDynamoDBDefaultOptions(), event, awsContext, false, expectedSettings1, expectedStage);

    // Don't force a different configuration
    context.kinesis = undefined;
    context.dynamoDBDocClient = undefined;
    deleteCachedInstances();
    const settings2 = toSettings(StreamType.dynamodb, true, false, true, 'consumerIdSuffix_NoOverride2', 'myTasks_NoOverride2', 0.81, 77, extractMessagesFromRecord2, extractMessageFromRecord2, generateMD5s2, resolveEventIdAndSeqNos2, resolveMessageIdsAndSeqNos2, loadBatchState2, saveBatchState2, discardUnusableRecord2, discardRejectedMessage2, 'batchStateTableName2', 'DRQ2', 'DMQ2', [], ["aS", "bS"], []);
    const options2 = toOptions(StreamType.dynamodb, true, true, true, 'consumerIdSuffix_NoOverride2B', 'myTasks_NoOverride2B', 0.80, 76, 'batchStateTableName2B', 'DRQ2B', 'DMQ2B', [], ["aO", "bO"], []);
    check(dynamoProcessing, context, settings2, options2, undefined, dynamoProcessing.loadDynamoDBDefaultOptions(), event, awsContext, false, expectedSettings1, expectedStage);

    // Force a new configuration
    context.kinesis = undefined;
    context.dynamoDBDocClient = undefined;
    deleteCachedInstances();
    const settings3 = toSettings(StreamType.dynamodb, true, false, true, 'consumerIdSuffix_Override3', 'myTasks_Override3', 0.91, 7, extractMessagesFromRecord2, extractMessageFromRecord2, generateMD5s2, resolveEventIdAndSeqNos2, resolveMessageIdsAndSeqNos2, loadBatchState2, saveBatchState2, discardUnusableRecord2, discardRejectedMessage2, 'batchStateTableName3', 'DRQ3', 'DMQ3', [], ["cS", "dS"], []);
    const options3 = toOptions(StreamType.dynamodb, true, true, true, 'consumerIdSuffix_Override3B', 'myTasks_Override3B', 0.90, 6, 'batchStateTableName3B', 'DRQ3B', 'DMQ3B', [], ["cO", "dO"], []);
    const expectedSettings3 = toSettings(StreamType.dynamodb, true, false, true, 'consumerIdSuffix_Override3', 'myTasks_Override3', 0.91, 7, extractMessagesFromRecord2, extractMessageFromRecord2, generateMD5s2, resolveEventIdAndSeqNos2, resolveMessageIdsAndSeqNos2, loadBatchState2, saveBatchState2, discardUnusableRecord2, discardRejectedMessage2, 'batchStateTableName3', 'DRQ3', 'DMQ3', [], ["cS", "dS"], []);
    check(dynamoProcessing, context, settings3, options3, undefined, dynamoProcessing.loadDynamoDBDefaultOptions(), event, awsContext, true, expectedSettings3, expectedStage);

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

// =====================================================================================================================
// configureDefaultDynamoDBStreamProcessing without event & awsContext
// =====================================================================================================================

test('configureDefaultDynamoDBStreamProcessing without event & awsContext', t => {

  function checkConfigureDefaultDynamoDBStreamProcessing(context, options, stdSettings, stdOptions, forceConfiguration, expectedSettings) {
    const before = context.streamProcessing;
    const mustChange = forceConfiguration || !dynamoProcessing.isStreamProcessingConfigured(context);

    const c = dynamoProcessing.configureDefaultDynamoDBStreamProcessing(context, options, stdSettings, stdOptions, undefined, undefined, forceConfiguration);

    t.ok(c === context, `Context returned must be given context`);
    checkSettings(t, dynamoProcessing, context, before, mustChange, expectedSettings);
    checkDependencies(t, context, stdSettings, stdOptions, undefined, undefined, undefined);
  }

  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");

    const context = {};

    t.notOk(dynamoProcessing.isStreamProcessingConfigured(context), `Stream processing must NOT be configured yet`);

    // Configure defaults for the first time
    const stdOptions = dynamoProcessing.loadDynamoDBDefaultOptions();
    const options = stdOptions.streamProcessingOptions;

    const expectedSettings = toSettings(options.streamType, true, false, true, options.consumerIdSuffix, options.consumerId, options.timeoutAtPercentageOfRemainingTime, options.maxNumberOfAttempts, dynamoProcessing.extractMessagesFromDynamoDBRecord, dynamoProcessing.extractMessageFromDynamoDBRecord, dynamoProcessing.generateDynamoMD5s, dynamoProcessing.resolveDynamoEventIdAndSeqNos, dynamoProcessing.resolveDynamoMessageIdsAndSeqNos, persisting.loadBatchStateFromDynamoDB, persisting.saveBatchStateToDynamoDB, dynamoProcessing.discardUnusableRecordToDRQ, dynamoProcessing.discardRejectedMessageToDMQ, options.batchStateTableName, options.deadRecordQueueName, options.deadMessageQueueName, [], [], []);

    checkConfigureDefaultDynamoDBStreamProcessing(context, options, undefined, stdOptions, false, expectedSettings);

    // Force a totally different configuration to overwrite the default DynamoDB configuration
    context.kinesis = undefined;
    context.dynamoDBDocClient = undefined;
    deleteCachedInstances();
    const otherSettings = toSettings(StreamType.dynamodb, false, false, true, 'consumerIdSuffix_Override', 'myTasks_Override', 0.91, 7, extractMessagesFromRecord2, extractMessageFromRecord2, generateMD5s2, resolveEventIdAndSeqNos2, resolveMessageIdsAndSeqNos2, loadBatchState1, saveBatchState1, discardUnusableRecord1, discardRejectedMessage1, 'batchStateTableName', 'DRQ', 'DMQ', [], [], []);
    checkConfigureStreamProcessingWithSettings(t, dynamoProcessing, context, otherSettings, undefined, dynamoProcessing.loadDynamoDBDefaultOptions(), undefined, undefined, true, otherSettings, undefined);

    // Don't force the default configuration back again
    checkConfigureDefaultDynamoDBStreamProcessing(context, options, undefined, stdOptions, false, otherSettings);

    // Force the default configuration back again
    context.kinesis = undefined;
    context.dynamoDBDocClient = undefined;
    deleteCachedInstances();
    checkConfigureDefaultDynamoDBStreamProcessing(context, options, undefined, stdOptions, true, expectedSettings);

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

// =====================================================================================================================
// configureDefaultDynamoDBStreamProcessing with event & awsContext
// =====================================================================================================================

test('configureDefaultDynamoDBStreamProcessing with event & awsContext', t => {

  function checkConfigureDefaultDynamoDBStreamProcessing(context, options, stdSettings, stdOptions, event, awsContext, forceConfiguration, expectedSettings, expectedStage) {
    const before = context.streamProcessing;
    const mustChange = forceConfiguration || !dynamoProcessing.isStreamProcessingConfigured(context);

    const c = dynamoProcessing.configureDefaultDynamoDBStreamProcessing(context, options, stdSettings, stdOptions, event, awsContext, forceConfiguration);

    t.ok(c === context, `Context returned must be given context`);
    checkSettings(t, dynamoProcessing, context, before, mustChange, expectedSettings);
    checkDependencies(t, context, stdSettings, stdOptions, event, awsContext, expectedStage);
  }

  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");
    const expectedStage = 'dev99';

    const context = {};

    t.notOk(dynamoProcessing.isStreamProcessingConfigured(context), `Stream processing must NOT be configured yet`);

    // Generate a sample AWS event
    const event = sampleAwsEvent('TestTable_DEV2', undefined, false);

    // Generate a sample AWS context
    const awsContext = sampleAwsContext('1.0.1', 'dev1');

    // Configure defaults for the first time
    const stdOptions = dynamoProcessing.loadDynamoDBDefaultOptions();
    const options = stdOptions.streamProcessingOptions;
    const consumerId = `sample-lambda-function:dev1`;

    const expectedSettings = toSettings(options.streamType, true, false, true, options.consumerIdSuffix, consumerId, options.timeoutAtPercentageOfRemainingTime, options.maxNumberOfAttempts, dynamoProcessing.extractMessagesFromDynamoDBRecord, dynamoProcessing.extractMessageFromDynamoDBRecord, dynamoProcessing.generateDynamoMD5s, dynamoProcessing.resolveDynamoEventIdAndSeqNos, dynamoProcessing.resolveDynamoMessageIdsAndSeqNos, persisting.loadBatchStateFromDynamoDB, persisting.saveBatchStateToDynamoDB, dynamoProcessing.discardUnusableRecordToDRQ, dynamoProcessing.discardRejectedMessageToDMQ, options.batchStateTableName, options.deadRecordQueueName, options.deadMessageQueueName, [], [], []);

    checkConfigureDefaultDynamoDBStreamProcessing(context, options, undefined, stdOptions, event, awsContext, false, expectedSettings, expectedStage);

    // Force a totally different configuration to overwrite the default DynamoDB configuration
    context.kinesis = undefined;
    context.dynamoDBDocClient = undefined;
    deleteCachedInstances();
    const otherSettings = toSettings(StreamType.dynamodb, false, false, true, 'consumerIdSuffix_Override', 'myTasks_Override', 0.91, 7, extractMessagesFromRecord2, extractMessageFromRecord2, generateMD5s2, resolveEventIdAndSeqNos2, resolveMessageIdsAndSeqNos2, loadBatchState1, saveBatchState1, discardUnusableRecord1, discardRejectedMessage1, 'batchStateTableName', 'DRQ', 'DMQ', [], [], []);
    checkConfigureStreamProcessingWithSettings(t, dynamoProcessing, context, otherSettings, undefined, dynamoProcessing.loadDynamoDBDefaultOptions(), event, awsContext, true, otherSettings, expectedStage);

    // Don't force the default configuration back again
    checkConfigureDefaultDynamoDBStreamProcessing(context, options, undefined, stdOptions, event, awsContext, false, otherSettings, expectedStage);

    // Force the default configuration back again
    context.kinesis = undefined;
    context.dynamoDBDocClient = undefined;
    deleteCachedInstances();
    checkConfigureDefaultDynamoDBStreamProcessing(context, options, undefined, stdOptions, event, awsContext, true, expectedSettings, expectedStage);

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

// =====================================================================================================================
// extractMessageFromDynamoDBRecord
// =====================================================================================================================

test('extractMessageFromDynamoDBRecord', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");

    const context = {};
    dynamoProcessing.configureDefaultDynamoDBStreamProcessing(context, undefined, undefined, dynamoProcessing.loadDynamoDBDefaultOptions(), undefined, undefined, true);

    const record0 = {};

    // Use an undefined record and expect an error
    t.throws(() => dynamoProcessing.extractMessageFromDynamoDBRecord(undefined, undefined, context), Error, `using an undefined record must throw an error`);
    t.throws(() => dynamoProcessing.extractMessageFromDynamoDBRecord(null, undefined, context), Error, `using a null record must throw an error`);

    t.throws(() => dynamoProcessing.extractMessageFromDynamoDBRecord(record0, undefined, context), Error, `using an empty object record must throw an error`);

    const eventSourceARN = samples.sampleDynamoDBEventSourceArn('us-west-2', 'TestTable_DEV');
    const record1 = samples.awsDynamoDBUpdateSampleEvent(eventSourceARN).Records[0];

    const message1 = dynamoProcessing.extractMessageFromDynamoDBRecord(record1, undefined, context);

    t.ok(message1, 'message1 must exist');
    t.equal(message1.eventID, record1.eventID, 'message1.eventID must match record1');
    t.equal(message1.eventSourceARN, record1.eventSourceARN, 'message1.eventSourceARN must match record1');
    t.equal(message1.dynamodb.SequenceNumber, record1.dynamodb.SequenceNumber, 'message1.dynamodb.SequenceNumber must match record1');

    if (record1.dynamodb.Keys) {
      // t.deepEqual(message1.dynamodb.Keys, record1.dynamodb.Keys, 'message1.dynamodb.Keys must match record1');
      t.notOk(message1.dynamodb.Keys, 'message1.dynamodb.Keys must not exist');
      t.ok(message1.dynamodb.keys, 'message1.dynamodb.keys must exist');
      t.notDeepEqual(message1.dynamodb.keys, record1.dynamodb.Keys, 'message1.dynamodb.keys must NOT match record1.dynamodb.Keys');
    }
    if (record1.dynamodb.NewImage) {
      //t.deepEqual(message1.dynamodb.NewImage, record1.dynamodb.NewImage, 'message1.dynamodb.NewImage must match record1');
      t.notOk(message1.dynamodb.NewImage, 'message1.dynamodb.NewImage must not exist');
      t.ok(message1.dynamodb.newImage, 'message1.dynamodb.newImage must exist');
      t.notDeepEqual(message1.dynamodb.newImage, record1.dynamodb.NewImage, 'message1.dynamodb.newImage must NOT match record1.dynamodb.NewImage');
    }
    if (record1.dynamodb.OldImage) {
      // t.deepEqual(message1.dynamodb.OldImage, record1.dynamodb.OldImage, 'message1.dynamodb.OldImage must match record1');
      t.notOk(message1.dynamodb.OldImage, 'message1.dynamodb.OldImage must not exist');
      t.ok(message1.dynamodb.oldImage, 'message1.dynamodb.oldImage must exist');
      t.notDeepEqual(message1.dynamodb.oldImage, record1.dynamodb.OldImage, 'message1.dynamodb.oldImage must NOT match record1.dynamodb.OldImage');
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

// =====================================================================================================================
// useStreamEventRecordAsMessage
// =====================================================================================================================

test('useStreamEventRecordAsMessage - with undefined as record', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");

    const context = {};
    dynamoProcessing.configureDefaultDynamoDBStreamProcessing(context, undefined, undefined,
      dynamoProcessing.loadDynamoDBDefaultOptions(), undefined, undefined, true);

    const rec = undefined;

    const batch = new Batch([rec], [], [], context);

    // Use an undefined record -> get an undefined message
    dynamoProcessing.useStreamEventRecordAsMessage(rec, batch, undefined, context).then(
      results => {
        // console.log(`##### results = ${JSON.stringify(results)}`);
        t.equal(results.length, 1, `results.length must be 1`);
        const unusableRecord = results[0].unusableRec;
        t.ok(unusableRecord, `unusableRecord must be defined`);
        t.ok(batch.unusableRecords.includes(unusableRecord), `batch.unusableRecords must include unusableRecord`);
        const state = batch.getState(unusableRecord);
        t.equal(state.record, rec, `state.record must be ${rec}`);

        t.end();
      },
      err => {
        t.end(`Should NOT have failed with error ${err}`);
      }
    );

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('useStreamEventRecordAsMessage - with null as record', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");

    const context = {};
    dynamoProcessing.configureDefaultDynamoDBStreamProcessing(context, undefined, undefined,
      dynamoProcessing.loadDynamoDBDefaultOptions(), undefined, undefined, true);

    const rec = null;

    const batch = new Batch([rec], [], [], context);

    // Use an undefined record -> get an undefined message
    dynamoProcessing.useStreamEventRecordAsMessage(rec, batch, undefined, context).then(
      results => {
        t.equal(results.length, 1, `results.length must be 1`);
        const unusableRecord = results[0].unusableRec;
        t.ok(unusableRecord, `unusableRecord must be defined`);
        t.ok(batch.unusableRecords.includes(unusableRecord), `batch.unusableRecords must include unusableRecord`);
        const state = batch.getState(unusableRecord);
        t.equal(state.record, rec, `state.record must be ${rec}`);

        t.end();
      },
      err => {
        t.end(`Should NOT have failed with error ${err}`);
      }
    );

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('useStreamEventRecordAsMessage - with single empty object record', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");

    const context = {};
    dynamoProcessing.configureDefaultDynamoDBStreamProcessing(context, undefined, undefined,
      dynamoProcessing.loadDynamoDBDefaultOptions(), undefined, undefined, true);

    const record0 = {};
    const batch = new Batch([record0], [], [], context);

    dynamoProcessing.useStreamEventRecordAsMessage(record0, batch, undefined, context).then(
      results => {
        // console.log(`##### results = ${JSON.stringify(results)}`);
        t.equal(results.length, 1, `results.length must be 1`);
        const rejectedMessage = results[0].rejectedMsg;
        t.ok(rejectedMessage, `rejectedMessage must be defined`);
        t.ok(batch.rejectedMessages.includes(rejectedMessage), `batch.rejectedMessages must include rejectedMessage`);
        t.notOk(batch.messages.includes(rejectedMessage), `batch.messages does NOT include rejectedMessage`);

        const state = batch.getState(rejectedMessage);
        t.equal(state.record, record0, `state.record must be record0`);
        t.ok(isNotBlank(state.reasonRejected), `state.reasonRejected must not be blank`);

        t.end();
      },
      err => {
        t.end(`Should NOT have failed with error ${err}`);
      }
    );

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('useStreamEventRecordAsMessage - with sample DynamoDB update event record', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");

    const context = {};
    dynamoProcessing.configureDefaultDynamoDBStreamProcessing(context, undefined, undefined,
      dynamoProcessing.loadDynamoDBDefaultOptions(), undefined, undefined, true);

    const eventSourceARN = samples.sampleDynamoDBEventSourceArn('us-west-2', 'TestTable_DEV');
    const record = samples.awsDynamoDBUpdateSampleEvent(eventSourceARN).Records[0];
    const batch = new Batch([record], [], [], context);

    dynamoProcessing.useStreamEventRecordAsMessage(record, batch, undefined, context).then(
      results => {
        t.equal(results.length, 1, `results.length must be 1`);
        const message = results[0].msg;
        t.ok(message, 'message must be extracted');
        t.ok(batch.messages.includes(message), `batch.messages must include message`);
        t.notEqual(message, record, 'message must NOT be record');
        t.deepEqual(message, record, `message must be copy of record`);

        const msgState = batch.getState(message);

        t.equal(msgState.record, record, 'msgState.record must be record');
        t.equal(msgState.userRecord, undefined, 'msgState.userRecord must be undefined');

        t.end();
      },
      err => {
        t.end(`Should NOT have failed with error ${err}`);
      }
    );

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

// =====================================================================================================================
// discardUnusableRecordToDRQ
// =====================================================================================================================

test('discardUnusableRecordToDRQ with 0 records', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");

    const context = {
      kinesis: dummyKinesis(t, 'discardUnusableRecordToDRQ', undefined)
    };

    const eventSourceARN = samples.sampleDynamoDBEventSourceArn('us-west-2', 'TestTable_DEV', '2017-03-13T21:35:49');
    const record1 = samples.sampleDynamoDBRecord('E001', '10000000000000000', eventSourceARN, '123', 456, 'ABC', 10, 1, 2, 3);
    const record = undefined;
    const records = [record, record1];

    const awsContext = samples.sampleAwsContext('test-lambda-function-name', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'test-lambda-function-name', 'dev98'));
    const event = samples.sampleDynamoDBEventWithRecords(records);

    dynamoProcessing.configureDefaultDynamoDBStreamProcessing(context, undefined, undefined, undefined, event, awsContext, true);

    const batch = new Batch(records, [], [], context);
    const unusableRecord = batch.addUnusableRecord(record, undefined, 'invalid record', context);
    const state = batch.getState(unusableRecord);
    t.equal(state.record, undefined, `state.record must be undefined`);

    dynamoProcessing.discardUnusableRecordToDRQ(record, batch, context)
      .then(result => {
        t.equal(result, undefined, `discardUnusableRecordToDRQ result must be undefined`);
        t.end();
      })
      .catch(err => {
        t.fail(`discardUnusableRecordToDRQ expected no failure - error: ${err}`);
        t.end(err);
      });

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('discardUnusableRecordToDRQ with 1 unusable record', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");

    const context = {
      kinesis: dummyKinesis(t, 'discardUnusableRecordToDRQ', undefined, request => request.PartitionKey)
    };

    const eventSourceARN = samples.sampleDynamoDBEventSourceArn('us-west-2', 'TestTable_DEV', '2017-03-13T21:35:49');
    const record = samples.sampleDynamoDBRecord('E001', '10000000000000000', eventSourceARN, '123', 456, 'ABC', 10, 1, 2, 3);

    const awsContext = samples.sampleAwsContext('test-lambda-function-name', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'test-lambda-function-name', 'dev98'));
    const records = [record];
    const event = samples.sampleDynamoDBEventWithRecords(records);

    dynamoProcessing.configureDefaultDynamoDBStreamProcessing(context, undefined, undefined, undefined,
      event, awsContext, true);

    const batch = new Batch(records, [], [], context);
    const unusableRecords = records.map((record, i) => batch.addUnusableRecord(record, undefined, `Dud wreck-ord ${i}`, context));

    const promises = unusableRecords.map(unusableRecord =>
      dynamoProcessing.discardUnusableRecordToDRQ(unusableRecord, batch, context)
    );

    Promises.every(promises, undefined, context).then(
      outcomes => {
        t.equal(outcomes.length, 1, `outcomes.length must be 1`);
        t.ok(outcomes[0].isSuccess(), `outcomes[0] must be Success`);
        t.deepEqual(outcomes[0].value, batch.streamConsumerId, `outcomes[0].value must be ${batch.streamConsumerId}`);
        t.end();
      },
      err => {
        t.end(err);
      }
    );

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('discardUnusableRecordToDRQ with 2 unusable records', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");

    const context = {
      kinesis: dummyKinesis(t, 'discardUnusableRecordToDRQ', undefined, request => request.PartitionKey)
    };

    const eventSourceARN = samples.sampleDynamoDBEventSourceArn('us-west-2', 'TestTable_DEV', '2017-03-13T21:35:49');
    const record1 = samples.sampleDynamoDBRecord('E001', '10000000000000001', eventSourceARN, '123', 456, 'ABC', 11, 1, 2, 3);
    const record2 = samples.sampleDynamoDBRecord('E002', '10000000000000002', eventSourceARN, '123', 457, 'DEF', 22, 4, 5, 6);

    const awsContext = samples.sampleAwsContext('test-lambda-function-name', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'test-lambda-function-name', '1.0.1'));
    const records = [record1, record2];
    const event = samples.sampleDynamoDBEventWithRecords(records);

    dynamoProcessing.configureDefaultDynamoDBStreamProcessing(context, undefined, undefined, undefined, event, awsContext, true);

    const batch = new Batch(records, [], [], context);
    const unusableRecords = records.map((record, i) => batch.addUnusableRecord(record, undefined, `Dud wreck-ord ${i}`, context));

    const promises = unusableRecords.map(unusableRecord =>
      dynamoProcessing.discardUnusableRecordToDRQ(unusableRecord, batch, context)
    );
    Promises.every(promises, undefined, context).then(
      outcomes => {
        t.equal(outcomes.length, 2, `outcomes.length must be 2`);
        t.ok(outcomes[0].isSuccess(), `outcomes[0] must be Success`);
        t.ok(outcomes[1].isSuccess(), `outcomes[1] must be Success`);
        t.deepEqual(outcomes[0].value, batch.streamConsumerId, `outcomes[0].value must be ${batch.streamConsumerId}`);
        t.deepEqual(outcomes[1].value, batch.streamConsumerId, `outcomes[1].value must be ${batch.streamConsumerId}`);
        t.end();
      },
      err => {
        t.end(err);
      }
    );

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('discardUnusableRecordToDRQ with 3 nasty unusable records', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");

    const context = {
      kinesis: dummyKinesis(t, 'discardUnusableRecordToDRQ', undefined, request => request.PartitionKey)
    };
    logging.configureLogging(context);
    dynamoProcessing.configureDefaultDynamoDBStreamProcessing(context);

    const eventSourceARN = samples.sampleDynamoDBEventSourceArn('us-west-2', 'TestTable_DEV', '2017-03-13T21:35:49');

    // Record with nothing useful at all
    const record0 = {bad: 'apple'}; // should fallback to LAST_RESORT_KEY

    // Record with no dynamodb.Keys
    const record1 = samples.sampleDynamoDBRecord('E001', '10000000000000001', eventSourceARN, '123', 456, 'ABC', 10, 1, 2, 3);
    delete record1.dynamodb.Keys;

    const record2 = samples.sampleDynamoDBRecord('E002', '10000000000000002', eventSourceARN, '123', 457, 'ABC', 10, 4, 5, 6);

    const awsContext = samples.sampleAwsContext('test-lambda-function-name', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'test-lambda-function-name', '1.0.1'));
    const records = [record0, record1, record2];
    const event = samples.sampleDynamoDBEventWithRecords(records);

    dynamoProcessing.configureDefaultDynamoDBStreamProcessing(context, undefined, undefined, undefined, event, awsContext, true);

    const batch = new Batch(records, [], [], context);
    const unusableRecords = records.map((record, i) =>
      batch.addUnusableRecord(record, undefined, `Dud wreck-ord ${i}`, context)
    );

    const partitionKey = batch.streamConsumerId.substring(0, streamEvents.MAX_PARTITION_KEY_SIZE);

    const promises = unusableRecords.map(unusableRecord =>
      dynamoProcessing.discardUnusableRecordToDRQ(unusableRecord, batch, context)
    );
    Promises.every(promises, undefined, context).then(
      outcomes => {
        t.equal(outcomes.length, 3, `outcomes.length must be 3`);
        outcomes.forEach((outcome, i) => {
          t.ok(outcome.isSuccess(), `outcomes[${i}] must be Success`);
        });
        t.deepEqual(outcomes[0].value, partitionKey, `outcomes[0].value must be ${partitionKey}`);
        t.deepEqual(outcomes[1].value, partitionKey, `outcomes[2].value must be ${partitionKey}`);
        t.deepEqual(outcomes[2].value, partitionKey, `outcomes[3].value must be ${partitionKey}`);
        t.end();
      },
      err => {
        t.end(err);
      }
    );

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('discardUnusableRecordToDRQ with 2 totally useless unusable records', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");

    const context = {
      kinesis: dummyKinesis(t, 'discardUnusableRecordToDRQ', undefined, request => request.PartitionKey)
    };

    const eventSourceARN = samples.sampleDynamoDBEventSourceArn('us-west-2', 'TestTable_DEV', '2017-03-13T21:35:49');

    // Record with nothing useful at all
    const record0 = {bad: 'apple'};

    // Record with no eventID
    const record1 = samples.sampleDynamoDBRecord('E004', '10000000000000004', eventSourceARN, '123', 456, 'ABC', 10, 1, 2, 3);
    delete record1.eventID;

    const awsContext = samples.sampleAwsContext('test-lambda-function-name', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'test-lambda-function-name', '1.0.1'));
    const records = [record0, record1];
    const event = samples.sampleDynamoDBEventWithRecords(records);

    dynamoProcessing.configureDefaultDynamoDBStreamProcessing(context, undefined, undefined, undefined, event, awsContext, true);

    const batch = new Batch(records, [], [], context);
    const unusableRecords = records.map((record, i) =>
      batch.addUnusableRecord(record, undefined, `Dud wreck-ord ${i}`, context)
    );

    const promises = unusableRecords.map(unusableRecord =>
      dynamoProcessing.discardUnusableRecordToDRQ(unusableRecord, batch, context)
    );
    Promises.every(promises, undefined, context).then(
      outcomes => {
        t.equal(outcomes.length, 2, `outcomes.length must be 2`);
        outcomes.forEach((outcome, i) => {
          t.ok(outcome.isFailure(), `outcomes[${i}] must be Failure`);
        });
        t.ok(outcomes[0].error.message.startsWith('Missing valid batch key'), `outcomes[0].error must start with 'Missing valid batch key'`);
        t.ok(outcomes[1].error.message.startsWith('Missing valid batch key'), `outcomes[1].error must start with 'Missing valid batch key'`);
        t.end();
      },
      err => {
        t.end(err);
      }
    );

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('discardUnusableRecordToDRQ with 1 unusable record & simulated failure during discard', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");

    const error = new Error('Planned failure');
    const context = {
      kinesis: dummyKinesis(t, 'discardUnusableRecordToDRQ', error)
    };

    const eventSourceARN = samples.sampleDynamoDBEventSourceArn('us-west-2', 'TestTable_DEV', '2017-03-13T21:35:49');
    const record = samples.sampleDynamoDBRecord('E001', '10000000000000001', eventSourceARN, '123', 456, 'ABC', 10, 1, 2, 3);

    const awsContext = samples.sampleAwsContext('test-lambda-function-name', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'test-lambda-function-name', '1.0.1'));
    const records = [record];
    const event = samples.sampleDynamoDBEventWithRecords(records);

    dynamoProcessing.configureDefaultDynamoDBStreamProcessing(context, undefined, undefined, undefined, event, awsContext, true);

    const batch = new Batch(records, [], [], context);
    const unusableRecords = records.map((record, i) =>
      batch.addUnusableRecord(record, undefined, `Dud wreck-ord ${i}`, context)
    );

    const promises = unusableRecords.map(unusableRecord =>
      dynamoProcessing.discardUnusableRecordToDRQ(unusableRecord, batch, context)
    );
    Promises.every(promises, undefined, context).then(
      outcomes => {
        t.equal(outcomes.length, 1, `outcomes.length must be 1`);
        t.ok(outcomes[0].isFailure(), `outcomes[0] must be Failure`);
        t.deepEqual(outcomes[0].error, error, `outcomes[0].error must be ${error}`);
        t.end();
      },
      err => {
        t.end(err);
      }
    );

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

// =====================================================================================================================
// discardRejectedMessageToDMQ
// =====================================================================================================================

test('discardRejectedMessageToDMQ with undefined rejected message', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");

    const context = {
      kinesis: dummyKinesis(t, 'discardRejectedMessageToDMQ', undefined)
    };

    const eventSourceARN = samples.sampleDynamoDBEventSourceArn('us-west-2', 'TestTable_DEV', '2017-03-13T21:35:49');
    const record = samples.sampleDynamoDBRecord('E001', '10000000000000001', eventSourceARN, '123', 456, 'ABC', 10, 1, 2, 3);

    const awsContext = samples.sampleAwsContext('test-lambda-function-name', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'test-lambda-function-name', 'dev66'));
    const event = samples.sampleDynamoDBEventWithRecords([record]);

    dynamoProcessing.configureDefaultDynamoDBStreamProcessing(context, undefined, undefined, undefined, event, awsContext, true);

    const records = [record];
    const batch = new Batch(records, [], [], context);

    dynamoProcessing.discardRejectedMessageToDMQ(undefined, batch, context)
      .then(result => {
        t.equal(result, undefined, `discardRejectedMessageToDMQ result (${result}) must be undefined`);
        t.end();
      })
      .catch(err => {
        t.fail(`discardRejectedMessageToDMQ expected no failure - error: ${err}`);
        t.end(err);
      });

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('discardRejectedMessageToDMQ with 1 message', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");

    const context = {
      kinesis: dummyKinesis(t, 'discardRejectedMessageToDMQ', undefined, request => request.PartitionKey)
    };
    dynamoProcessing.configureDefaultDynamoDBStreamProcessing(context, {
      sequencingPerKey: true,
      keyPropertyNames: ['k1', 'k2']
    });

    const eventSourceARN = samples.sampleDynamoDBEventSourceArn('us-west-2', 'TestTable_DEV', '2017-03-13T21:35:49');

    const [msg, record] = samples.sampleDynamoDBMessageAndRecord('E001', '10000000000000001', eventSourceARN, '123', 456, 'ABC', undefined, 1, 2, 3);

    const awsContext = samples.sampleAwsContext('test-lambda-function-name', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'test-lambda-function-name', 'dev66'));
    const records = [record];
    const event = samples.sampleDynamoDBEventWithRecords(records);

    streamProcessing.configureEventAwsContextAndStage(context, event, awsContext);

    const messages = [msg];
    const batch = new Batch(records, [], [], context);
    batch.addMessage(msg, record, undefined, context);

    const promises = messages.map(message =>
      dynamoProcessing.discardRejectedMessageToDMQ(message, batch, context)
    );

    Promises.every(promises, undefined, context).then(
      outcomes => {
        t.equal(outcomes.length, 1, `outcomes.length must be 1`);
        t.ok(outcomes[0].isSuccess(), `outcomes[0] must be Success`);
        t.deepEqual(outcomes[0].value, batch.streamConsumerId, `outcomes[0].value must be ${batch.streamConsumerId}`);
        t.end();
      },
      err => {
        t.fail(`discardRejectedMessageToDMQ expected no failure - error: ${err}`);
        t.end(err);
      }
    );

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('discardRejectedMessageToDMQ with 2 messages', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");

    const context = {
      kinesis: dummyKinesis(t, 'discardRejectedMessageToDMQ', undefined, request => request.PartitionKey)
    };
    dynamoProcessing.configureDefaultDynamoDBStreamProcessing(context, {
      sequencingPerKey: true,
      keyPropertyNames: ['k1', 'k2']
    });

    const eventSourceARN = samples.sampleDynamoDBEventSourceArn('us-west-2', 'TestTable_DEV', '2017-03-13T21:35:49');

    const [msg1, record1] = samples.sampleDynamoDBMessageAndRecord('E001', '10000000000000001', eventSourceARN, '123', 456, 'ABC', 10, 1, 2, 3);
    const [msg2, record2] = samples.sampleDynamoDBMessageAndRecord('E002', '10000000000000002', eventSourceARN, '123', 457, 'ABC', 10, 4, 5, 6);

    const awsContext = samples.sampleAwsContext('test-lambda-function-name', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'test-lambda-function-name', '1.0.1'));
    const records = [record1, record2];
    const event = samples.sampleDynamoDBEventWithRecords(records);

    streamProcessing.configureEventAwsContextAndStage(context, event, awsContext);

    const messages = [msg1, msg2];
    const batch = new Batch(records, [], [], context);
    batch.addMessage(msg1, record1, undefined, context);
    batch.addMessage(msg2, record2, undefined, context);

    const promises = messages.map(message =>
      dynamoProcessing.discardRejectedMessageToDMQ(message, batch, context)
    );
    Promises.every(promises, undefined, context).then(
      outcomes => {
        t.equal(outcomes.length, 2, `outcomes.length must be 2`);
        t.ok(outcomes[0].isSuccess(), `outcomes[0] must be Success`);
        t.ok(outcomes[1].isSuccess(), `outcomes[1] must be Success`);
        t.deepEqual(outcomes[0].value, batch.streamConsumerId, `outcomes[0].value must be ${batch.streamConsumerId}`);
        t.deepEqual(outcomes[1].value, batch.streamConsumerId, `outcomes[1].value must be ${batch.streamConsumerId}`);
        t.end();
      },
      err => {
        t.fail(`discardRejectedMessageToDMQ expected no failure - error: ${err}`);
        t.end(err);
      }
    );

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('discardRejectedMessageToDMQ with 1 record and simulated failure during discard', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");

    const error = new Error('Planned failure');
    const context = {
      kinesis: dummyKinesis(t, 'discardRejectedMessageToDMQ', error)
    };
    dynamoProcessing.configureDefaultDynamoDBStreamProcessing(context, {
      sequencingPerKey: true,
      keyPropertyNames: ['k1', 'k2']
    });

    const eventSourceARN = samples.sampleDynamoDBEventSourceArn('us-west-2', 'TestTable_DEV', '2017-03-13T21:35:49');

    const [msg, record] = samples.sampleDynamoDBMessageAndRecord('E001', '10000000000000001', eventSourceARN, '123', 456, 'ABC', 10, 1, 2, 3);

    const awsContext = samples.sampleAwsContext('test-lambda-function-name', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'test-lambda-function-name', 'dev42'));
    const records = [record];
    const event = samples.sampleDynamoDBEventWithRecords(records);

    streamProcessing.configureEventAwsContextAndStage(context, event, awsContext);

    const messages = [msg];
    const batch = new Batch(records, [], [], context);
    batch.addMessage(msg, record, undefined, context);

    const promises = messages.map(message =>
      dynamoProcessing.discardRejectedMessageToDMQ(message, batch, context)
    );
    Promises.every(promises, undefined, context).then(
      outcomes => {
        t.equal(outcomes.length, 1, `outcomes.length must be 1`);
        t.ok(outcomes[0].isFailure(), `outcomes[0] must be Failure`);
        t.deepEqual(outcomes[0].error, error, `outcomes[0].error must be ${error}`);
        t.end();
      },
      err => {
        t.fail(`discardRejectedMessageToDMQ expected no failure - error: ${err}`);
        t.end(err);
      }
    );

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});
