
'use strict';

/**
 * Unit tests for dynamodb-stream-consumer/dynamo-processing.js & aws-stream-consumer/settings.js
 * @author Byron du Preez
 */

const test = require('tape');

// The test subject
const Settings = require('aws-stream-consumer/settings');
const StreamType = Settings.StreamType;
const names = Settings.names;
const getStreamProcessingSetting = Settings.getStreamProcessingSetting;
const getStreamProcessingFunction = Settings.getStreamProcessingFunction;

// Convenience accessors for specific stream processing settings
const getStreamType = Settings.getStreamType;
const isKinesisStreamType = Settings.isKinesisStreamType;
const isDynamoDBStreamType = Settings.isDynamoDBStreamType;
const isSequencingRequired = Settings.isSequencingRequired;
const isSequencingPerKey = Settings.isSequencingPerKey;
const isBatchKeyedOnEventID = Settings.isBatchKeyedOnEventID;
const getConsumerIdSuffix = Settings.getConsumerIdSuffix;
const getConsumerId = Settings.getConsumerId;
const getMaxNumberOfAttempts = Settings.getMaxNumberOfAttempts;

const getIdPropertyNames = Settings.getIdPropertyNames;
const getKeyPropertyNames = Settings.getKeyPropertyNames;
const getSeqNoPropertyNames = Settings.getSeqNoPropertyNames;

// Convenience accessors for specific stream processing functions
const getGenerateMD5sFunction = Settings.getGenerateMD5sFunction;
const getResolveEventIdAndSeqNosFunction = Settings.getResolveEventIdAndSeqNosFunction;
const getResolveMessageIdsAndSeqNosFunction = Settings.getResolveMessageIdsAndSeqNosFunction;
const getExtractMessagesFromRecordFunction = Settings.getExtractMessagesFromRecordFunction;
const getExtractMessageFromRecordFunction = Settings.getExtractMessageFromRecordFunction;
const getLoadBatchStateFunction = Settings.getLoadBatchStateFunction;
const getPreProcessBatchFunction = Settings.getPreProcessBatchFunction;

const getPreFinaliseBatchFunction = Settings.getPreFinaliseBatchFunction;
const getSaveBatchStateFunction = Settings.getSaveBatchStateFunction;
const getDiscardUnusableRecordFunction = Settings.getDiscardUnusableRecordFunction;
const getDiscardRejectedMessageFunction = Settings.getDiscardRejectedMessageFunction;
const getPostFinaliseBatchFunction = Settings.getPostFinaliseBatchFunction;

const dynamoIdentify = require('../dynamo-identify');

const dynamoProcessing = require('../dynamo-processing');
const persisting = require('aws-stream-consumer/persisting');

// Default generateMD5s function
const generateDynamoMD5s = dynamoIdentify.generateDynamoMD5s;

// Default resolveEventIdAndSeqNos function
const resolveDynamoEventIdAndSeqNos = dynamoIdentify.resolveDynamoEventIdAndSeqNos;

// Default resolveMessageIdsAndSeqNos function
const resolveDynamoMessageIdsAndSeqNos = dynamoIdentify.resolveDynamoMessageIdsAndSeqNos;

// Default extractMessagesFromRecord function
const extractMessagesFromDynamoDBRecord = dynamoProcessing.extractMessagesFromDynamoDBRecord;

// Default extractMessageFromDynamoDBRecord function
const extractMessageFromDynamoDBRecord = dynamoProcessing.extractMessageFromDynamoDBRecord;

// Default loadBatchState function
const loadBatchStateFromDynamoDB = persisting.loadBatchStateFromDynamoDB;

// Default saveBatchState function
const saveBatchStateToDynamoDB = persisting.saveBatchStateToDynamoDB;

// External dependencies
// const logging = require('logging-utils');
// const base64 = require('core-functions/base64');
const regions = require('aws-core-utils/regions');
// const stages = require('aws-core-utils/stages');
const kinesisCache = require('aws-core-utils/kinesis-cache');
const dynamoDBDocClientCache = require('aws-core-utils/dynamodb-doc-client-cache');

// const samples = require('./samples');
const strings = require('core-functions/strings');
const stringify = strings.stringify;

const errors = require('core-functions/errors');
const FatalError = errors.FatalError;

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

function checkConfig(t, context, expected, sequencingRequired, sequencingPerKey) {
  // Generic accessors
  t.equal(getStreamProcessingSetting(context, names.streamType), expected.streamType, `streamType setting must be ${expected.streamType}`);
  t.equal(getStreamProcessingSetting(context, names.sequencingRequired), expected.sequencingRequired, `sequencingRequired setting must be ${expected.sequencingRequired}`);
  t.equal(getStreamProcessingSetting(context, names.sequencingPerKey), expected.sequencingPerKey, `sequencingPerKey setting must be ${expected.sequencingPerKey}`);
  t.equal(getStreamProcessingSetting(context, names.batchKeyedOnEventID), expected.batchKeyedOnEventID, `batchKeyedOnEventID setting must be ${expected.batchKeyedOnEventID}`);
  t.equal(getStreamProcessingSetting(context, names.consumerIdSuffix), expected.consumerIdSuffix, `consumerIdSuffix setting must be ${expected.consumerIdSuffix}`);
  t.equal(getStreamProcessingSetting(context, names.consumerId), expected.consumerId, `consumerId setting must be ${expected.consumerId}`);
  t.equal(getStreamProcessingSetting(context, names.timeoutAtPercentageOfRemainingTime), expected.timeoutAtPercentageOfRemainingTime, `timeoutAtPercentageOfRemainingTime setting must be ${expected.timeoutAtPercentageOfRemainingTime}`);
  t.equal(getStreamProcessingSetting(context, names.maxNumberOfAttempts), expected.maxNumberOfAttempts, `maxNumberOfAttempts setting must be ${expected.maxNumberOfAttempts}`);

  t.deepEqual(getStreamProcessingSetting(context, names.idPropertyNames), expected.idPropertyNames, `idPropertyNames setting must be ${stringify(expected.idPropertyNames)}`);
  t.deepEqual(getStreamProcessingSetting(context, names.keyPropertyNames), expected.keyPropertyNames, `keyPropertyNames setting must be ${stringify(expected.keyPropertyNames)}`);
  t.deepEqual(getStreamProcessingSetting(context, names.seqNoPropertyNames), expected.seqNoPropertyNames, `seqNoPropertyNames setting must be ${stringify(expected.seqNoPropertyNames)}`);

  // Generic function accessors
  t.equal(getStreamProcessingFunction(context, names.extractMessagesFromRecord), extractMessagesFromDynamoDBRecord, `extractMessagesFromRecord function must be ${stringify(extractMessagesFromDynamoDBRecord)}`);
  t.equal(getStreamProcessingFunction(context, names.extractMessageFromRecord), extractMessageFromDynamoDBRecord, `extractMessageFromRecord function must be ${stringify(extractMessageFromDynamoDBRecord)}`);
  t.equal(getStreamProcessingFunction(context, names.generateMD5s), generateDynamoMD5s, `generateMD5s function must be ${stringify(generateDynamoMD5s)}`);
  t.equal(getStreamProcessingFunction(context, names.resolveEventIdAndSeqNos), resolveDynamoEventIdAndSeqNos, `resolveEventIdAndSeqNos function must be ${stringify(resolveDynamoEventIdAndSeqNos)}`);
  t.equal(getStreamProcessingFunction(context, names.resolveMessageIdsAndSeqNos), resolveDynamoMessageIdsAndSeqNos, `resolveMessageIdsAndSeqNos function must be ${stringify(resolveDynamoMessageIdsAndSeqNos)}`);
  t.equal(getStreamProcessingFunction(context, names.loadBatchState), loadBatchStateFromDynamoDB, `loadBatchState function must be ${stringify(loadBatchStateFromDynamoDB)}`);
  t.equal(getStreamProcessingFunction(context, names.preProcessBatch), undefined, `preProcessBatch function must be ${stringify(undefined)}`);

  t.equal(getStreamProcessingFunction(context, names.preFinaliseBatch), undefined, `preFinaliseBatch function must be ${stringify(undefined)}`);
  t.equal(getStreamProcessingFunction(context, names.saveBatchState), saveBatchStateToDynamoDB, `saveBatchState function must be ${stringify(saveBatchStateToDynamoDB)}`);
  t.equal(getStreamProcessingFunction(context, names.discardUnusableRecord), dynamoProcessing.discardUnusableRecordToDRQ, `discardUnusableRecord function must be ${stringify(dynamoProcessing.discardUnusableRecordToDRQ)}`);
  t.equal(getStreamProcessingFunction(context, names.discardRejectedMessage), dynamoProcessing.discardRejectedMessageToDMQ, `discardRejectedMessage function must be ${stringify(dynamoProcessing.discardRejectedMessageToDMQ)}`);
  t.equal(getStreamProcessingFunction(context, names.postFinaliseBatch), undefined, `postFinaliseBatch function must be ${stringify(undefined)}`);

  t.equal(getStreamProcessingSetting(context, names.batchStateTableName), expected.batchStateTableName, `batchStateTableName setting must be ${expected.batchStateTableName}`);
  t.equal(getStreamProcessingSetting(context, names.deadRecordQueueName), expected.deadRecordQueueName, `deadRecordQueueName setting must be ${expected.deadRecordQueueName}`);
  t.equal(getStreamProcessingSetting(context, names.deadMessageQueueName), expected.deadMessageQueueName, `deadMessageQueueName setting must be ${expected.deadMessageQueueName}`);

  // Specific setting accessors
  t.equal(getStreamType(context), StreamType.dynamodb, `streamType setting must be ${StreamType.dynamodb}`);
  t.equal(getStreamType(context), expected.streamType, `streamType setting must be ${expected.streamType}`);
  t.notOk(isKinesisStreamType(context), `isKinesisStreamType must be ${false}`);
  t.ok(isDynamoDBStreamType(context), `isDynamoDBStreamType must be ${true}`);

  t.equal(isSequencingRequired(context), sequencingRequired, `sequencingRequired setting must be ${sequencingRequired}`);
  t.equal(isSequencingPerKey(context), sequencingPerKey, `sequencingPerKey setting must be ${sequencingPerKey}`);

  t.equal(isSequencingRequired(context), expected.sequencingRequired, `sequencingRequired setting must be ${expected.sequencingRequired}`);
  t.equal(isSequencingPerKey(context), expected.sequencingPerKey, `sequencingPerKey setting must be ${expected.sequencingPerKey}`);
  t.equal(isBatchKeyedOnEventID(context), expected.batchKeyedOnEventID, `batchKeyedOnEventID setting must be ${expected.batchKeyedOnEventID}`);
  t.equal(getConsumerIdSuffix(context), expected.consumerIdSuffix, `consumerIdSuffix setting must be ${expected.consumerIdSuffix}`);
  t.equal(getConsumerId(context), expected.consumerId, `consumerId setting must be ${expected.consumerId}`);
  //t.equal(getTimeoutAtPercentageOfRemainingTime(context), defaultSettings.timeoutAtPercentageOfRemainingTime, `timeoutAtPercentageOfRemainingTime setting must be ${defaultSettings.timeoutAtPercentageOfRemainingTime}`);
  t.equal(getMaxNumberOfAttempts(context), expected.maxNumberOfAttempts, `maxNumberOfAttempts setting must be ${expected.maxNumberOfAttempts}`);

  t.deepEqual(getIdPropertyNames(context), expected.idPropertyNames, `idPropertyNames setting must be ${stringify(expected.idPropertyNames)}`);
  t.deepEqual(getKeyPropertyNames(context), expected.keyPropertyNames, `keyPropertyNames setting must be ${stringify(expected.keyPropertyNames)}`);
  t.deepEqual(getSeqNoPropertyNames(context), expected.seqNoPropertyNames, `seqNoPropertyNames setting must be ${stringify(expected.seqNoPropertyNames)}`);

  // Specific function accessors
  t.equal(getExtractMessagesFromRecordFunction(context), extractMessagesFromDynamoDBRecord, `extractMessagesFromRecord function must be ${stringify(extractMessagesFromDynamoDBRecord)}`);
  t.equal(getExtractMessageFromRecordFunction(context), extractMessageFromDynamoDBRecord, `extractMessageFromRecord function must be ${stringify(extractMessageFromDynamoDBRecord)}`);
  t.equal(getGenerateMD5sFunction(context), generateDynamoMD5s, `generateMD5s function must be ${stringify(generateDynamoMD5s)}`);
  t.equal(getResolveEventIdAndSeqNosFunction(context), resolveDynamoEventIdAndSeqNos, `resolveEventIdAndSeqNos function must be ${stringify(resolveDynamoEventIdAndSeqNos)}`);
  t.equal(getResolveMessageIdsAndSeqNosFunction(context), resolveDynamoMessageIdsAndSeqNos, `resolveMessageIdsAndSeqNos function must be ${stringify(resolveDynamoMessageIdsAndSeqNos)}`);
  t.equal(getLoadBatchStateFunction(context), loadBatchStateFromDynamoDB, `loadBatchState function must be ${stringify(loadBatchStateFromDynamoDB)}`);
  t.equal(getPreProcessBatchFunction(context), undefined, `preProcessBatch function must be ${stringify(undefined)}`);

  t.equal(getPreFinaliseBatchFunction(context), undefined, `preFinaliseBatch function must be ${stringify(undefined)}`);
  t.equal(getSaveBatchStateFunction(context), saveBatchStateToDynamoDB, `saveBatchState function must be ${stringify(saveBatchStateToDynamoDB)}`);
  t.equal(getDiscardUnusableRecordFunction(context), dynamoProcessing.discardUnusableRecordToDRQ, `discardUnusableRecord function must be ${stringify(dynamoProcessing.discardUnusableRecordToDRQ)}`);
  t.equal(getDiscardRejectedMessageFunction(context), dynamoProcessing.discardRejectedMessageToDMQ, `discardRejectedMessage function must be ${stringify(dynamoProcessing.discardRejectedMessageToDMQ)}`);
  t.equal(getPostFinaliseBatchFunction(context), undefined, `postFinaliseBatch function must be ${stringify(undefined)}`);
}

// =====================================================================================================================
// Accessors of settings and functions on default DynamoDB configuration
// =====================================================================================================================

test('Accessors of settings and functions on default DynamoDB configuration', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', 'dev99');

    // Configure default stream processing settings
    const context = dynamoProcessing.configureDefaultDynamoDBStreamProcessing({});

    const options = dynamoProcessing.loadDynamoDBDefaultOptions();
    const expected = dynamoProcessing.getDefaultDynamoDBStreamProcessingSettings(options.streamProcessingOptions);

    checkConfig(t, context, expected, true, false);

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

test('Accessors of settings and functions on default DynamoDB configuration with minimalistic sequencing per key configuration', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', 'dev99');

    // Configure stream processing settings
    // Using minimalist options for sequenced per key
    const settings = {sequencingPerKey: true, keyPropertyNames: ['k']};
    const context = dynamoProcessing.configureDefaultDynamoDBStreamProcessing({}, settings);

    const options = dynamoProcessing.loadDynamoDBDefaultOptions();
    options.streamProcessingOptions.sequencingPerKey = true;
    const expected = dynamoProcessing.getDefaultDynamoDBStreamProcessingSettings(options.streamProcessingOptions);
    expected.keyPropertyNames = ['k'];

    checkConfig(t, context, expected, true, true);

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

test('Accessors of settings and functions on default DynamoDB configuration with conflicting sequencing per key configuration - case 1', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', 'dev99');

    // Configure stream processing settings
    const settings = {sequencingRequired: false, sequencingPerKey: true};
    t.throws(() => dynamoProcessing.configureDefaultDynamoDBStreamProcessing({}, settings), FatalError, 'conflict case 1 must throw FatalError');

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

test('Accessors of settings and functions on default DynamoDB configuration with conflicting sequencing per key configuration - case 2', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', 'dev99');

    // Configure stream processing settings
    const settings = {sequencingPerKey: false, keyPropertyNames: ['k']};
    t.throws(() => dynamoProcessing.configureDefaultDynamoDBStreamProcessing({}, settings), FatalError, 'conflict case 2 must throw FatalError');

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

test('Accessors of settings and functions on default DynamoDB configuration with conflicting sequencing per key configuration - case 3', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', 'dev99');

    // Configure stream processing settings
    const settings = {sequencingPerKey: true, keyPropertyNames: []};
    // t.throws(() => dynamoProcessing.configureDefaultDynamoDBStreamProcessing({}, settings), FatalError, 'conflict case 3 must throw FatalError');
    const context = dynamoProcessing.configureDefaultDynamoDBStreamProcessing({}, settings);

    const options = dynamoProcessing.loadDynamoDBDefaultOptions();
    options.streamProcessingOptions.sequencingPerKey = true;
    const expected = dynamoProcessing.getDefaultDynamoDBStreamProcessingSettings(options.streamProcessingOptions);
    expected.keyPropertyNames = [];

    checkConfig(t, context, expected, true, true);

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

test('Accessors of settings and functions on default DynamoDB configuration with conflicting sequencing per key configuration - case 4', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', 'dev99');

    // Configure stream processing settings
    const settings = {sequencingRequired: false, sequencingPerKey: false, keyPropertyNames: ['k']};
    t.throws(() => dynamoProcessing.configureDefaultDynamoDBStreamProcessing({}, settings), FatalError, 'conflict case 4 must throw FatalError');

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

// =====================================================================================================================
// Accessors of settings and functions on UNSEQUENCED DynamoDB configuration
// =====================================================================================================================

test('Accessors of settings and functions on UNSEQUENCED DynamoDB configuration', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', 'dev99');

    // Configure UNSEQUENCED stream processing settings
    // Using minimalist options for unsequenced
    const settings = {sequencingRequired: false};
    const context = dynamoProcessing.configureDefaultDynamoDBStreamProcessing({}, settings);

    const options = dynamoProcessing.loadDynamoDBDefaultOptions();
    options.streamProcessingOptions.sequencingRequired = false;
    const expected = dynamoProcessing.getDefaultDynamoDBStreamProcessingSettings(options.streamProcessingOptions);

    checkConfig(t, context, expected, false, false);

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});
