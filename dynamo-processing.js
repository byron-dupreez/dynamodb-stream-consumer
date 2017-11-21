'use strict';

const regions = require('aws-core-utils/regions');
const stages = require('aws-core-utils/stages');
const contexts = require('aws-core-utils/contexts');
const arns = require('aws-core-utils/arns');

const streamEvents = require('aws-core-utils/stream-events');
const MAX_PARTITION_KEY_SIZE = streamEvents.MAX_PARTITION_KEY_SIZE;

const kinesisCache = require('aws-core-utils/kinesis-cache');

const dynamoDBUtils = require('aws-core-utils/dynamodb-utils');
const toStorableObject = dynamoDBUtils.toStorableObject;

// const kinesisUtils = require('aws-core-utils/kinesis-utils');

const Booleans = require('core-functions/booleans');
const isTrueOrFalse = Booleans.isTrueOrFalse;

const tries = require('core-functions/tries');
const Try = tries.Try;
const Failure = tries.Failure;
const Promises = require('core-functions/promises');

const copying = require('core-functions/copying');
const copy = copying.copy;
const merging = require('core-functions/merging');
const merge = merging.merge;
const deep = {deep: true};

const errors = require('core-functions/errors');
const FatalError = errors.FatalError;

const Strings = require('core-functions/strings');
const isBlank = Strings.isBlank;
const isNotBlank = Strings.isNotBlank;
const trim = Strings.trim;
const trimOrEmpty = Strings.trimOrEmpty;
const stringify = Strings.stringify;

const logging = require('logging-utils');

const taskUtils = require('task-utils');
const Task = require('task-utils/tasks');

// Setting-related utilities
const Settings = require('aws-stream-consumer/settings');
const defaults = Settings.defaults;
const StreamType = Settings.StreamType;
const isSequencingRequired = Settings.isSequencingRequired;

const Batch = require('aws-stream-consumer/batch');

const streamProcessing = require('aws-stream-consumer/stream-processing');

// Utilities for loading and saving tracked state of the current batch being processed from and to an external store
const persisting = require('aws-stream-consumer/persisting');

const dynamoIdentify = require('./dynamo-identify');

const LAST_RESORT_KEY = 'LAST_RESORT_KEY';

/**
 * Utilities for configuring stream processing, which configures and determines the processing behaviour of a stream
 * consumer.
 * @module aws-stream-consumer/stream-processing-config
 * @author Byron du Preez
 */
exports._$_ = '_$_'; //IDE workaround

exports.LAST_RESORT_KEY = LAST_RESORT_KEY;

// Stream processing configuration - configures and determines the processing behaviour of a stream consumer
exports.isStreamProcessingConfigured = isStreamProcessingConfigured;

exports.configureStreamProcessing = configureStreamProcessing;
exports.configureStreamProcessingWithSettings = configureStreamProcessingWithSettings;

exports.validateDynamoDBStreamProcessingConfiguration = validateDynamoDBStreamProcessingConfiguration;

exports.configureDefaultDynamoDBStreamProcessing = configureDefaultDynamoDBStreamProcessing;
exports.getDefaultDynamoDBStreamProcessingSettings = getDefaultDynamoDBStreamProcessingSettings;
exports.getDefaultDynamoDBStreamProcessingOptions = getDefaultDynamoDBStreamProcessingOptions;

// Local json file defaults for configuration purposes
exports.loadDynamoDBDefaultOptions = loadDynamoDBDefaultOptions;
// Static defaults for configuration purposes
exports.getDynamoDBStaticDefaults = getDynamoDBStaticDefaults;

/**
 * Default implementations of the stream processing functions, which are NOT meant to be used directly and are ONLY
 * exposed to facilitate re-using some of these functions if needed in a customised stream processing configuration.
 */

// Default DynamoDB stream processing functions
// ============================================

// Default extractMessagesFromRecord function for extracting from DynamoDB stream event records
exports.extractMessagesFromDynamoDBRecord = extractMessagesFromDynamoDBRecord;

// Default extractMessageFromRecord function for extracting from DynamoDB stream event records
exports.extractMessageFromDynamoDBRecord = extractMessageFromDynamoDBRecord;

// Default generateMD5s function (re-exported for convenience)
exports.generateDynamoMD5s = dynamoIdentify.generateDynamoMD5s;

// Default resolveEventIdAndSeqNos function (re-exported for convenience)
exports.resolveDynamoEventIdAndSeqNos = dynamoIdentify.resolveDynamoEventIdAndSeqNos;

// Default resolveMessageIdsAndSeqNos function (re-exported for convenience)
exports.resolveDynamoMessageIdsAndSeqNos = dynamoIdentify.resolveDynamoMessageIdsAndSeqNos;

// Default common Kinesis and DynamoDB stream processing functions
// ===============================================================

// Default loadBatchState function (re-exported for convenience)
exports.loadBatchStateFromDynamoDB = persisting.loadBatchStateFromDynamoDB;

// Default saveBatchState function (re-exported for convenience)
exports.saveBatchStateToDynamoDB = persisting.saveBatchStateToDynamoDB;

// Default discardUnusableRecord function
exports.discardUnusableRecordToDRQ = discardUnusableRecordToDRQ;

// Default discardRejectedMessage function
exports.discardRejectedMessageToDMQ = discardRejectedMessageToDMQ;

// Other default stream processing functions
exports.useStreamEventRecordAsMessage = streamProcessing.useStreamEventRecordAsMessage;

// =====================================================================================================================
// Stream processing configuration - configures and determines the processing behaviour of a stream consumer
// =====================================================================================================================

/**
 * Returns true if stream processing is already configured on the given context; false otherwise.
 * @param {Object|StreamProcessing} context - the context to check
 * @returns {boolean} true if configured; false otherwise
 */
function isStreamProcessingConfigured(context) {
  return streamProcessing.isStreamProcessingConfigured(context);
  // return context && typeof context === 'object' && context.streamProcessing && typeof context.streamProcessing === 'object';
}

/**
 * Configures the given context as a standard context with the given standard settings and standard options and with
 * EITHER the given stream processing settings (if any) OR the default stream processing settings partially overridden
 * by the given stream processing options (if any), but only if stream processing is not already configured on the given
 * context OR if forceConfiguration is true.
 *
 * Note that if either the given event or AWS context are undefined, then everything other than the event, AWS context &
 * stage will be configured. This missing configuration can be configured at a later point in your code by invoking
 * {@linkcode module:aws-core-utils/contexts#configureEventAwsContextAndStage}. This separation of configuration is
 * primarily useful for unit testing.
 *
 * @param {Object|StreamProcessing|StandardContext} context - the context to configure
 * @param {StreamProcessingSettings|undefined} [settings] - optional stream processing settings to use to configure stream processing
 * @param {StreamProcessingOptions|undefined} [options] - optional stream processing options to use to override default options
 * @param {StandardSettings|undefined} [standardSettings] - optional standard settings to use to configure dependencies
 * @param {StandardOptions|undefined} [standardOptions] - optional other options to use to configure dependencies
 * @param {AWSEvent|undefined} [event] - the AWS event, which was passed to your lambda
 * @param {AWSContext|undefined} [awsContext] - the AWS context, which was passed to your lambda
 * @param {boolean|undefined} [forceConfiguration] - whether or not to force configuration of the given settings, which
 * will override any previously configured stream processing settings on the given context
 * @returns {StreamProcessing} the given context configured with stream processing settings, stage handling settings and
 * logging functionality
 */
function configureStreamProcessing(context, settings, options, standardSettings, standardOptions, event, awsContext, forceConfiguration) {
  const settingsAvailable = settings && typeof settings === 'object';
  const optionsAvailable = options && typeof options === 'object';

  // Check if stream processing was already configured
  const streamProcessingWasConfigured = streamProcessing.isStreamProcessingConfigured(context);

  // Determine the stream processing settings to be used
  const defaultSettings = getDefaultDynamoDBStreamProcessingSettings(options);

  const streamProcessingSettings = settingsAvailable ? merge(defaultSettings, settings) : defaultSettings;

  // Configure stream processing with the given or derived stream processing settings
  configureStreamProcessingWithSettings(context, streamProcessingSettings, standardSettings, standardOptions,
    event, awsContext, forceConfiguration);

  // Log a warning if no settings and no options were provided and the default settings were applied
  if (!settingsAvailable && !optionsAvailable && (forceConfiguration || !streamProcessingWasConfigured)) {
    context.warn(`Stream processing was configured without settings or options - used default stream processing configuration (${stringify(streamProcessingSettings)})`);
  }
  return context;
}

/**
 * Configures the given context with the given stream processing settings, but only if stream processing is not already
 * configured on the given context OR if forceConfiguration is true, and with the given standard settings and options.
 *
 * Note that if either the given event or AWS context are undefined, then everything other than the event, AWS context &
 * stage will be configured. This missing configuration can be configured at a later point in your code by invoking
 * {@linkcode module:aws-core-utils/contexts#configureEventAwsContextAndStage}. This separation of configuration is
 * primarily useful for unit testing.
 *
 * @param {Object|StreamProcessing|StandardContext} context - the context onto which to configure the given stream processing settings and standard settings
 * @param {StreamProcessingSettings} settings - the stream processing settings to use
 * @param {StandardSettings|undefined} [standardSettings] - optional standard settings to use to configure dependencies
 * @param {StandardOptions|undefined} [standardOptions] - optional standard options to use to configure dependencies
 * @param {AWSEvent|undefined} [event] - the AWS event, which was passed to your lambda
 * @param {AWSContext|undefined} [awsContext] - the AWS context, which was passed to your lambda
 * @param {boolean|undefined} [forceConfiguration] - whether or not to force configuration of the given settings and
 * options, which will override any previously configured stream processing and stage handling settings on the given context
 * @return {StreamProcessing} the context object configured with stream processing (either existing or new) and standard settings
 */
function configureStreamProcessingWithSettings(context, settings, standardSettings, standardOptions, event, awsContext, forceConfiguration) {
  return streamProcessing.configureStreamProcessingWithSettings(context, settings, standardSettings, standardOptions,
    event, awsContext, forceConfiguration, validateDynamoDBStreamProcessingConfiguration);
}

/**
 * Configures the given context as a standard context with the given standard settings and standard options and ALSO
 * with the default DynamoDB stream processing settings partially overridden by the given stream processing options (if
 * any), but ONLY if stream processing is not already configured on the given context OR if forceConfiguration is true.
 *
 * Default DynamoDB stream processing assumes the following:
 * - The stream event is a DynamoDB stream event, which contains one or more records in its `Records` property.
 * - A single "message" will be extracted from EACH of these stream event records using the configured `extractMessagesFromRecord`
 *   function.
 *   - See {@link extractMessagesFromDynamoDBRecord} for the default DynamoDB {@link ExtractMessagesFromRecord} implementation.
 *   - Note: This default behaviour can be changed by configuring a different `extractMessagesFromRecord` function.
 *   - The default DynamoDB `extractMessagesFromRecord` function simply delegates to the configured
 *     `extractMessageFromDynamoDBRecord` function to extract an individual message from an individual DynamoDB stream
 *     event record.
 *     - The default DynamoDB `extractMessageFromDynamoDBRecord` function assumes that:
 *       - The message is a copy of the DynamoDB stream event record (other than replacing its "Keys", "NewImage" &
 *         "OldImage" properties with new simple object forms named as "keys", "newImage" & "oldImage" properties)
 *       - See {@link extractMessageFromDynamoDBRecord} for the default {@link ExtractMessageFromDynamoDBRecord} implementation
 *       - Note: This default behaviour can be changed by configuring a different `ExtractMessageFromDynamoDBRecord` function.
 *
 * Note that if either the given event or AWS context are undefined, then everything other than the event, AWS context &
 * stage will be configured. This missing configuration can be configured at a later point in your code by invoking
 * {@linkcode module:aws-core-utils/contexts#configureEventAwsContextAndStage}. This separation of configuration is
 * primarily useful for unit testing.
 *
 * @param {Object|StreamProcessing|StandardContext} context - the context onto which to configure the default stream processing settings
 * @param {StreamProcessingOptions|undefined} [options] - optional stream processing options to use
 * @param {StandardSettings|undefined} [standardSettings] - optional standard settings to use to configure dependencies
 * @param {StandardOptions|undefined} [standardOptions] - optional standard options to use to configure dependencies
 * @param {AWSEvent|undefined} [event] - the AWS event, which was passed to your lambda
 * @param {AWSContext|undefined} [awsContext] - the AWS context, which was passed to your lambda
 * @param {boolean|undefined} [forceConfiguration] - whether or not to force configuration of the given settings, which
 * will override any previously configured stream processing and stage handling settings on the given context
 * @return {StreamProcessing} the context object configured with DynamoDB stream processing settings (either existing or defaults)
 */
function configureDefaultDynamoDBStreamProcessing(context, options, standardSettings, standardOptions, event, awsContext, forceConfiguration) {
  // Get the default DynamoDB stream processing settings from the local options file
  const settings = getDefaultDynamoDBStreamProcessingSettings(options);

  // Configure the context with the default stream processing settings defined above
  return configureStreamProcessingWithSettings(context, settings, standardSettings, standardOptions,
    event, awsContext, forceConfiguration);
}

/**
 * Returns the default DynamoDB stream processing settings partially overridden by the given stream processing options
 * (if any).
 *
 * This function is used internally by {@linkcode configureDefaultDynamoDBStreamProcessing}, but could also be used in
 * custom configurations to get the default settings as a base to be overridden with your custom settings before calling
 * {@linkcode configureStreamProcessing}.
 *
 * @param {StreamProcessingOptions} [options] - optional stream processing options to use to override the default options
 * @returns {StreamProcessingSettings} a stream processing settings object (including both property and function settings)
 */
function getDefaultDynamoDBStreamProcessingSettings(options) {
  const settings = options && typeof options === 'object' ? copy(options, deep) : {};

  // Load defaults from local default-dynamo-options.json file
  const defaultOptions = getDefaultDynamoDBStreamProcessingOptions();
  merge(defaultOptions, settings);

  const defaultSettings = {
    // Configurable processing functions
    extractMessagesFromRecord: extractMessagesFromDynamoDBRecord,
    extractMessageFromRecord: extractMessageFromDynamoDBRecord,
    generateMD5s: dynamoIdentify.generateDynamoMD5s,
    resolveEventIdAndSeqNos: dynamoIdentify.resolveDynamoEventIdAndSeqNos,
    resolveMessageIdsAndSeqNos: dynamoIdentify.resolveDynamoMessageIdsAndSeqNos,
    loadBatchState: persisting.loadBatchStateFromDynamoDB,
    preProcessBatch: undefined,

    preFinaliseBatch: undefined,
    saveBatchState: persisting.saveBatchStateToDynamoDB,
    discardUnusableRecord: discardUnusableRecordToDRQ,
    discardRejectedMessage: discardRejectedMessageToDMQ,
    postFinaliseBatch: undefined
  };
  return merge(defaultSettings, settings);
}

// =====================================================================================================================
// Default options for configuration purposes
// =====================================================================================================================

/**
 * Loads a copy of the default DynamoDB stream processing options from the local default-dynamo-options.json file and
 * fills in any missing options with the static default options.
 * @returns {StreamProcessingOptions} the default stream processing options
 */
function getDefaultDynamoDBStreamProcessingOptions() {
  const stdOptions = loadDynamoDBDefaultOptions();

  const defaultOptions = stdOptions && stdOptions.streamProcessingOptions && typeof stdOptions.streamProcessingOptions === 'object' ?
    stdOptions.streamProcessingOptions : {};

  const staticDefaults = getDynamoDBStaticDefaults();
  return merge(staticDefaults, defaultOptions);
}

/**
 * Loads a copy of the default DynamoDB stream processing options from the local default-dynamo-options.json file.
 * @returns {StreamProcessingOptions} the default stream processing options
 */
function loadDynamoDBDefaultOptions() {
  return copy(require('./default-dynamo-options.json'), deep);
}

/**
 * Gets the last-resort, static defaults for DynamoDB stream processing options.
 * @returns {StreamProcessingOptions} last-resort, static defaults for DynamoDB stream processing options
 */
function getDynamoDBStaticDefaults() {
  return {
    // Generic settings
    streamType: StreamType.dynamodb,
    sequencingRequired: true,
    sequencingPerKey: false,
    batchKeyedOnEventID: true,
    consumerIdSuffix: undefined,
    timeoutAtPercentageOfRemainingTime: defaults.timeoutAtPercentageOfRemainingTime,
    maxNumberOfAttempts: defaults.maxNumberOfAttempts,
    avoidEsmCache: false,

    idPropertyNames: undefined, // Optional, but RECOMMENDED - otherwise complicates duplicate elimination & batch state loading/saving if not specified
    keyPropertyNames: undefined, // Optional, since will derive keys from dynamodb.Keys if NOT specified
    seqNoPropertyNames: undefined, // Optional, since will derive seqNos from dynamodb.SequenceNumber if NOT specified

    batchStateTableName: defaults.batchStateTableName,
    // Specialised settings needed by default implementations - e.g. DRQ and DMQ stream names
    deadRecordQueueName: defaults.deadRecordQueueName,
    deadMessageQueueName: defaults.deadMessageQueueName
  };
}

function validateDynamoDBStreamProcessingConfiguration(context) {
  // Run the common validations
  streamProcessing.validateStreamProcessingConfiguration(context);

  const sequencingRequired = context.streamProcessing.sequencingRequired;
  const sequencingPerKey = context.streamProcessing.sequencingPerKey;
  const batchKeyedOnEventID = context.streamProcessing.batchKeyedOnEventID;

  const logger = context.error ? context : console;
  // const warn = context.warn || console.warn;

  const extractMessagesFromRecordFunction = context.streamProcessing.extractMessagesFromRecord;
  const extractMessageFromRecord = context.streamProcessing.extractMessageFromRecord;

  if (extractMessagesFromRecordFunction === extractMessagesFromDynamoDBRecord && typeof extractMessageFromRecord !== 'function') {
    const errMsg = `FATAL - Mis-configured with extractMessagesFromDynamoDBRecord, but WITHOUT an extractMessageFromRecord function - cannot extract any message from any DynamoDB stream event record! Fix by configuring one and redeploy ASAP, since this issue is blocking all of your stream's shards!`;
    logger.error(errMsg);
    throw new FatalError(errMsg);
  }

  // Validate message sequencing configuration
  if (!sequencingRequired) { // Sequencing is NOT required, but ...
    // logger.warn(`Configured with sequencingRequired (${sequencingRequired}) - messages will be processed out of sequence!`);
    if (sequencingPerKey) {
      // logger.warn(`Configured with sequencingRequired (${sequencingRequired}), but sequencingPerKey (${sequencingPerKey}) - messages will be processed out of sequence & sequencingPerKey will be ignored`);
      const errMsg = `FATAL - Mis-configured with sequencingRequired (${sequencingRequired}), but sequencingPerKey (${sequencingPerKey}). Fix conflict ASAP by setting sequencingRequired to true (for sequencing per key per shard) or sequencingPerKey to false (for unsequenced)!`;
      logger.error(errMsg);
      throw new FatalError(errMsg);
    }
  }
  // else { // Sequencing is required, but ...
  //   if (!sequencingPerKey) {
  //     logger.warn(`Configured with sequencingRequired (${sequencingRequired}), but sequencingPerKey (${sequencingPerKey}) - messages will be processed one after another in a SINGLE sequence per shard (instead of in a sequence per key per shard)`);
  //   }
  // }

  if (!batchKeyedOnEventID) {
    const errMsg = `FATAL - Mis-configured with batchKeyedOnEventID (${batchKeyedOnEventID}), but CANNOT currently resolve shard IDs for DynamoDB batches and hence HAVE to key them on event ID!`;
    logger.error(errMsg);
    throw new FatalError(errMsg);
  }

  // Only check id/key/seqNo property names if configured with the default resolveDynamoMessageIdsAndSeqNos
  if (context.streamProcessing.resolveMessageIdsAndSeqNos === dynamoIdentify.resolveDynamoMessageIdsAndSeqNos) {
    // const idPropertyNames = context.streamProcessing.idPropertyNames; // Settings.getIdPropertyNames(context);

    const keyPropertyNames = context.streamProcessing.keyPropertyNames; // Settings.getKeyPropertyNames(context);
    const hasKeyPropertyNames = keyPropertyNames && keyPropertyNames.length > 0;

    // const seqNoPropertyNames = context.streamProcessing.seqNoPropertyNames; // Settings.getSeqNoPropertyNames(context);
    // const hasSeqNoPropertyNames = seqNoPropertyNames && seqNoPropertyNames.length > 0;

    // if (!idPropertyNames || idPropertyNames.length <= 0) {
    //   logger.warn(`Configured WITHOUT idPropertyNames (${JSON.stringify(idPropertyNames)}), which complicates duplicate elimination & loading/saving of batch state`);
    // }

    if (hasKeyPropertyNames) { // Key property names configured, but ...
      if (!sequencingRequired || !sequencingPerKey) {
        // const impact = sequencingRequired ?
        //   'messages will be processed one after another in a SINGLE sequence per shard (instead of in a sequence per key per shard)' :
        //   'messages will be processed out of sequence';
        // logger.warn(`Configured with keyPropertyNames (${JSON.stringify(keyPropertyNames)}), but with sequencingRequired (${sequencingRequired}) & sequencingPerKey (${sequencingPerKey}) - keyPropertyNames will be ignored - ${impact}. Fix by either removing keyPropertyNames or by setting both sequencingRequired & sequencingPerKey to true`);
        const errMsg = `FATAL - Mis-configured with keyPropertyNames (${JSON.stringify(keyPropertyNames)}), but with sequencingRequired (${sequencingRequired}) & sequencingPerKey (${sequencingPerKey}). Fix conflict ASAP by setting both sequencingRequired & sequencingPerKey to true (for sequencing per key per shard) OR by removing keyPropertyNames (for ${sequencingRequired ? 'sequencing per shard' : 'unsequenced'})!`;
        logger.error(errMsg);
        throw new FatalError(errMsg);
      }
      // if (!hasSeqNoPropertyNames) {
      //   logger.warn(`Configured with keyPropertyNames (${JSON.stringify(keyPropertyNames)}), but WITHOUT seqNoPropertyNames (${JSON.stringify(seqNoPropertyNames)}). Fix by either configuring both or removing both`);
      // }
    }
    // else { // No key property names configured, but ... (not such an issue, since DynamoDB event records have their own Keys)
    //   if (sequencingRequired && sequencingPerKey) {
    //     const errMsg = `FATAL - Mis-configured with sequencingPerKey (${sequencingPerKey}), but WITHOUT keyPropertyNames (${JSON.stringify(keyPropertyNames)}). Fix conflict ASAP by setting keyPropertyNames (for sequencing per key per shard) or setting sequencingPerKey to false (for sequencing per shard)!`;
    //     logger.error(errMsg);
    //     throw new FatalError(errMsg);
    //   }
    //   // if (hasSeqNoPropertyNames) {
    //   //   logger.warn(`Configured with seqNoPropertyNames (${JSON.stringify(seqNoPropertyNames)}), but WITHOUT keyPropertyNames (${JSON.stringify(keyPropertyNames)}). Fix by either configuring both or removing both`);
    //   // }
    // }

    // if (!hasSeqNoPropertyNames) {
    //   logger.warn(`Configured WITHOUT seqNoPropertyNames (${JSON.stringify(seqNoPropertyNames)}), which is NOT recommended (forces use of dynamodb.SequenceNumber properties to sequence messages). Fix by configuring seqNoPropertyNames.`);
    // }
  }
}

/**
 * A default `extractMessagesFromRecord` function for extracting a single message from the given DynamoDB stream event
 * record that uses the given `extractMessageFromRecord` function to convert the record into a corresponding message and
 * then adds the successfully extracted message, rejected message or unusable record to the given batch.
 * @see {@link ExtractMessagesFromRecord}
 * @see {@link ExtractMessageFromRecord}
 * @param {DynamoDBEventRecord} record - a DynamoDB stream event record
 * @param {Batch} batch - the batch to which to add the extracted message or unusable record
 * @param {ExtractMessageFromRecord|undefined} [extractMessageFromRecord] - the actual function to use to extract a message from the record or user record (if any)
 * @param {StreamProcessing} context - the context to use
 * @return {Promise.<MsgOrUnusableRec[]>} a promise of an array containing one message, rejected message or unusable record
 */
function extractMessagesFromDynamoDBRecord(record, batch, extractMessageFromRecord, context) {
  const messageOutcome = Try.try(() => extractMessageFromRecord(record, undefined, context));

  // Add the message (if any) or unusable record to the batch
  return messageOutcome.map(
    message => {
      return [batch.addMessage(message, record, undefined, context)];
    },
    err => {
      return [{unusableRec: batch.addUnusableRecord(record, undefined, err.message, context)}];
    }
  ).toPromise();
}

// noinspection JSUnusedLocalSymbols
/**
 * A default `extractMessageFromRecord` function that creates a "message" from a copy of the original DynamoDB stream
 * event record with its DynamoDB attribute type & value format "Keys", "NewImage" and "OldImage" properties replaced
 * with simple object format "keys", "newImage" & "oldImage" properties.
 * @see {@link ExtractMessageFromRecord}
 * @param {DynamoDBEventRecord} record - a DynamoDB stream event record
 * @param {undefined} [userRecord] - always undefined, since DynamoDB stream event records do not currently support Kinesis Producer Library encoding & aggregation
 * @param {StreamProcessing} context - the context
 * @return {Message} the message object (if successfully extracted)
 * @throws {Error} an error if the given stream event record has no dynamodb.Keys property
 */
function extractMessageFromDynamoDBRecord(record, userRecord, context) {
  if (!record.dynamodb) {
    const errMsg = `Missing dynamodb property for record (${record.eventID})`;
    context.error(errMsg);
    throw new Error(errMsg);
  }
  if (!record.dynamodb.Keys) {
    const errMsg = `Missing dynamodb.Keys property for record (${record.eventID})`;
    context.error(errMsg);
    throw new Error(errMsg);
  }

  // First create the message as a copy of the original record
  const message = copy(record, {deep: true});

  // Then convert the Keys, NewImage and OldImage properties from DynamoDB format into simple objects
  dynamoDBUtils.simplifyKeysNewImageAndOldImage(message.dynamodb);

  if (context.traceEnabled) context.trace(`Extracted message (${stringify(message)}) from record (${record.eventID})`);
  return message;
}

/**
 * Discards the given unusable stream event record to the DRQ (i.e. Dead Record Queue).
 * Default implementation of a {@link DiscardUnusableRecord} function.
 * @param {UnusableRecord|Record} unusableRecord - the unusable record to discard
 * @param {Batch} batch - the batch being processed
 * @param {StreamProcessing} context - the context to use
 * @return {Promise} a promise that will complete when the unusable record is discarded
 */
function discardUnusableRecordToDRQ(unusableRecord, batch, context) {
  return streamProcessing.discardUnusableRecordToDRQ(unusableRecord, batch, toDRQPutRequestFromDynamoDBUnusableRecord, context);
}

function toDRQPutRequestFromDynamoDBUnusableRecord(unusableRecord, batch, deadRecordQueueName, context) {
  const recState = batch.states.get(unusableRecord);

  const record = recState.record;
  // const dynamodb = (record && record.dynamodb) || (unusableRecord && unusableRecord.dynamodb);

  // const eventID = recState.eventID || (record && record.eventID) || unusableRecord.eventID;
  // const eventSeqNo = recState.eventSeqNo || (dynamodb && dynamodb.SequenceNumber);
  const reasonUnusable = recState.reasonUnusable;

  const state = copy(recState, deep);
  delete state.record;

  // delete state.eventID;
  // delete state.eventSeqNo;
  // delete state.reasonUnusable;

  const deadRecord = {
    streamConsumerId: batch.streamConsumerId,
    shardOrEventID: batch.shardOrEventID,
    ver: 'DR|D|2.0',
    // eventID: eventID,
    // eventSeqNo: eventSeqNo,
    unusableRecord: unusableRecord,
    record: record !== unusableRecord ? record : undefined, // don't need BOTH unusableRecord & record if same
    state: state,
    reasonUnusable: reasonUnusable,
    discardedAt: new Date().toISOString()
  };

  // Generate a partition key to use for the DRQ request
  const partitionKey = generatePartitionKey(batch);

  return {
    StreamName: deadRecordQueueName,
    PartitionKey: partitionKey,
    Data: JSON.stringify(deadRecord)
  };
}

/**
 * Routes the given rejected message to the DMQ (i.e. Dead Message Queue).
 * Default implementation of a {@link DiscardRejectedMessage} function.
 * @param {Message} rejectedMessage - the rejected message to discard
 * @param {Batch} batch - the batch being processed
 * @param {StreamProcessing} context the context to use
 * @return {Promise}
 */
function discardRejectedMessageToDMQ(rejectedMessage, batch, context) {
  return streamProcessing.discardRejectedMessageToDMQ(rejectedMessage, batch,
    toDMQPutRequestFromDynamoDBRejectedMessage, context);
}

// noinspection JSUnusedLocalSymbols
function toDMQPutRequestFromDynamoDBRejectedMessage(message, batch, deadMessageQueueName, context) {
  const msgState = batch.states.get(message);

  const record = msgState.record;
  // const dynamodb = (record && record.dynamodb) || (message && message.dynamodb);

  // const eventID = msgState.eventID || record.eventID;
  // const eventSeqNo = msgState.eventSeqNo || (dynamodb && dynamodb.SequenceNumber);

  const state = copy(msgState, deep);
  delete state.record;

  // delete state.eventID;
  // delete state.eventSeqNo;
  // delete state.reasonRejected;

  // // Replace ids, keys & seqNos with enumerable versions
  // delete state.ids;
  // delete state.keys;
  // delete state.seqNos;
  // state.ids = msgState.ids;
  // state.keys = msgState.keys;
  // state.seqNos = msgState.seqNos;

  // Wrap the message in a rejected message "envelope" with metadata
  const rejectedMessage = {
    streamConsumerId: batch.streamConsumerId,
    shardOrEventID: batch.shardOrEventID,
    ver: 'DM|D|2.0',
    // eventID: eventID,
    // eventSeqNo: eventSeqNo,
    // ids: state.ids,
    // keys: state.keys,
    // seqNos: state.seqNos,
    message: message,
    record: record,
    state: state,
    reasonRejected: batch.findReasonRejected(message),
    discardedAt: new Date().toISOString()
  };

  // Generate a partition key to use for the DMQ request
  const partitionKey = generatePartitionKey(batch);

  return {
    StreamName: deadMessageQueueName,
    PartitionKey: partitionKey,
    Data: JSON.stringify(rejectedMessage)
  };
}

// function extractStreamName(batchKey) {
//   const streamName = batchKey && batchKey.components && batchKey.components.streamName;
//   if (isNotBlank(streamName)) return streamName;
//
//   const streamConsumerId = batchKey && batchKey.streamConsumerId;
//   if (isNotBlank(streamConsumerId)) {
//     // Extract the source stream name from the batch key's streamConsumerId
//     const ss = streamConsumerId.indexOf('|') + 1; // find start of source stream name
//     return (ss !== -1 && streamConsumerId.substring(ss, streamConsumerId.indexOf('|', ss))) || '';
//   }
//   return '';
// }
//
// function extractDynamodbKeysAndValues(dynamodb) {
//   const keys = dynamodb && dynamodb.Keys;
//   return (keys && dynamoDBUtils.toKeyValueStrings(keys).join('|')) || '';
// }
//
// function generatePartitionKey(batchKey, dynamodb) {
//   // Resolve the source stream's name
//   const sourceStreamName = extractStreamName(batchKey);
//
//   // Combine all of the record's dynamodb Keys into a single string
//   const keysAndValues = extractDynamodbKeysAndValues(dynamodb);
//
//   // Generate a partition key to use for the DMQ request
//   return isNotBlank(sourceStreamName) || isNotBlank(keysAndValues) ?
//     `${sourceStreamName}|${keysAndValues}`.substring(0, MAX_PARTITION_KEY_SIZE) : LAST_RESORT_KEY;
// }

function generatePartitionKey(batch) {
  return isNotBlank(batch.streamConsumerId) ?
    batch.streamConsumerId.substring(0, MAX_PARTITION_KEY_SIZE) : LAST_RESORT_KEY;
}
