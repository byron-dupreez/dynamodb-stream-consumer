'use strict';

const objects = require('core-functions/objects');
const hasOwnPropertyWithCompoundName = objects.hasOwnPropertyWithCompoundName;
const getPropertyValueByCompoundName = objects.getPropertyValueByCompoundName;
const kvOpts = {onlyEnumerable: true, skipArrayLike: false, omitSymbols: true};

const sorting = require('core-functions/sorting');

const settings = require('aws-stream-consumer/settings');

const crypto = require('crypto');

const dynamoDBUtils = require('aws-core-utils/dynamodb-utils');
const toObjectFromDynamoDBMap = dynamoDBUtils.toObjectFromDynamoDBMap;

/**
 * Utilities and functions to be used by a DynamoDB stream consumer to identify messages.
 * @module dynamodb-stream-consumer/dynamo-identify
 * @author Byron du Preez
 */
exports._$_ = '_$_'; //IDE workaround

exports.generateDynamoMD5s = generateDynamoMD5s;
exports.resolveDynamoEventIdAndSeqNos = resolveDynamoEventIdAndSeqNos;
exports.resolveDynamoMessageIdsAndSeqNos = resolveDynamoMessageIdsAndSeqNos;

/**
 * Generates MD5 message digest(s) for the given message and/or its given record.
 * An implementation of {@link GenerateMD5s}.
 * @param {Message|undefined} [message] - the message for which to generate MD5 message digest(s)
 * @param {Record} record - the record for which to generate MD5 message digest(s)
 * @return {MD5s} the generated MD5 message digest(s)
 */
function generateDynamoMD5s(message, record) {
  return {
    msg: (message && md5Sum(JSON.stringify(message))) || undefined,
    rec: record && md5Sum(JSON.stringify(record))
  };
}

/**
 * Attempts to resolve the event ID, event sequence number and event sub-sequence number of the given DynamoDB stream
 * event record. An implementation of {@link ResolveEventIdAndSeqNos}.
 *
 * @param {Record} record - the record from which to extract an eventID and event sequence number(s)
 * @returns {EventIdAndSeqNos} the given record's resolved: eventID; and eventSeqNo, but NO eventSubSeqNo (since its always undefined for DynamoDB event records)
 */
function resolveDynamoEventIdAndSeqNos(record) {
  // Resolve the record's eventID
  const eventID = record && record.eventID;

  // Resolve the record's dynamodb property
  const dynamodb = record && record.dynamodb;

  // Resolve the record's "event" sequence number (i.e. dynamodb.SequenceNumber)
  const eventSeqNo = (dynamodb && dynamodb.SequenceNumber) || undefined;

  return {eventID, eventSeqNo};
}

// noinspection JSUnusedLocalSymbols
/**
 * Attempts to resolve the ids, keys and sequence numbers of the given DynamoDB stream event message. If message
 * sequencing is required (default) then: EITHER keyPropertyNames must be non-empty OR dynamodb.Keys must be defined;
 * and the resolved keys and seqNos must BOTH be non-empty and must not contain any undefined values.
 * An implementation of {@link ResolveMessageIdsAndSeqNos}.
 * @param {Message} message - the message for which to resolve its ids, keys & sequence numbers
 * @param {Record} record - the record from which the message originated
 * @param {UserRecord|undefined} [userRecord] - not applicable for DynamoDB stream records
 * @param {EventIdAndSeqNos} eventIdAndSeqNos - the message's record's eventID & eventSeqNo
 * @param {MD5s} md5s - the MD5 message digests generated for the message & its record
 * @param {StreamConsumerContext} context - the context to use
 * @returns {MessageIdsAndSeqNos} the given message's resolved: id(s); key(s); and sequence number(s)
 * @throws {Error} an error if no usable keys, ids or sequence numbers can be resolved
 */
function resolveDynamoMessageIdsAndSeqNos(message, record, userRecord, eventIdAndSeqNos, md5s, context) {
  const desc = JSON.stringify(eventIdAndSeqNos);
  const sequencingRequired = settings.isSequencingRequired(context);
  const sequencingPerKey = sequencingRequired && settings.isSequencingPerKey(context);

  // Resolve the configured id, key & sequence property names (if any)
  const idPropertyNames = settings.getIdPropertyNames(context);
  const keyPropertyNames = settings.getKeyPropertyNames(context);
  const seqNoPropertyNames = settings.getSeqNoPropertyNames(context);

  // Resolve the message's dynamodb property (if any)
  const msgDynamodb = message.dynamodb && typeof message.dynamodb === 'object' ? message.dynamodb : undefined;

  // Resolve the record's dynamodb property
  const recDynamodb = record && record.dynamodb;

  // Collect simple objects for the keys, new image and/or old image properties of the message's dynamodb property (if present)
  // Note: msg.dynamodb.keys/newImage/oldImage will exist and msg.dynamodb.Keys/NewImage/OldImage will NOT exist, if
  // you are using the default `extractMessageFromDynamoDBRecord` function or `aws-core-utils/dynamodb-utils` module's
  // `simplifyKeysNewImageAndOldImage` function
  const recDynamodbKeys = recDynamodb && recDynamodb.Keys;
  const msgDynamodbKeys = msgDynamodb && msgDynamodb.Keys;
  const keysMap = (msgDynamodb && msgDynamodb.keys) ||
    (recDynamodbKeys && toObjectFromDynamoDBMap(recDynamodbKeys)) ||
    (msgDynamodbKeys && toObjectFromDynamoDBMap(msgDynamodbKeys));

  const newImageMap = (msgDynamodb && msgDynamodb.newImage) ||
    (recDynamodb && recDynamodb.NewImage && toObjectFromDynamoDBMap(recDynamodb.NewImage)) ||
    (msgDynamodb && msgDynamodb.NewImage && toObjectFromDynamoDBMap(msgDynamodb.NewImage));

  const oldImageMap = (msgDynamodb && msgDynamodb.oldImage) ||
    (recDynamodb && recDynamodb.OldImage && toObjectFromDynamoDBMap(recDynamodb.OldImage)) ||
    (msgDynamodb && msgDynamodb.OldImage && toObjectFromDynamoDBMap(msgDynamodb.OldImage));

  // Resolve the message's id(s): using idPropertyNames if configured; otherwise an empty array
  const ids = idPropertyNames.length > 0 ?
    getPropertyValues(idPropertyNames, message, record, keysMap, newImageMap, oldImageMap, false, desc, 'ids', context) : [];

  // Resolve the message's key(s) using keyPropertyNames if configured; otherwise using dynamodb.Keys (converted into
  // an array of key value pairs and sorted into alphabetical key sequence for consistency) if present
  const hasKeyPropertyNames = keyPropertyNames.length > 0;
  const keys = hasKeyPropertyNames ?
    getPropertyValues(keyPropertyNames, message, record, keysMap, newImageMap, oldImageMap, sequencingPerKey, desc, 'keys', context) :
    msgDynamodb && msgDynamodb.keys ? sorting.sortKeyValuePairsByKey(objects.toKeyValuePairs(msgDynamodb.keys, kvOpts), false) :
      recDynamodbKeys ? sorting.sortKeyValuePairsByKey(dynamoDBUtils.toKeyValuePairs(recDynamodbKeys), false) :
        msgDynamodbKeys ? sorting.sortKeyValuePairsByKey(dynamoDBUtils.toKeyValuePairs(msgDynamodbKeys), false) : [];

  if (sequencingPerKey && keys.length <= 0) {
    const errMsg = `Failed to resolve any keys for message (${desc}) - ${hasKeyPropertyNames ? `keyPropertyNames [${keyPropertyNames.join(', ')}]` : `Keys ${JSON.stringify(recDynamodbKeys || msgDynamodbKeys)}`}`;
    context.error(errMsg);
    throw errorWithReason(new Error(errMsg), 'Sequencing per key, but failed to resolve any keys');
  }

  // Resolve the message's sequence number(s) using seqNoPropertyNames if configured; otherwise using dynamodb.SequenceNumber if present
  const hasSeqNoPropertyNames = seqNoPropertyNames.length > 0;
  const seqNos = hasSeqNoPropertyNames ?
    getPropertyValues(seqNoPropertyNames, message, record, keysMap, newImageMap, oldImageMap, sequencingRequired, desc, 'seqNos', context) :
    eventIdAndSeqNos && eventIdAndSeqNos.eventSeqNo ? [['eventSeqNo', eventIdAndSeqNos.eventSeqNo]] : [];

  if (sequencingRequired && seqNos.length <= 0) {
    const errMsg = `Failed to resolve any seqNos for message (${desc}) - ${hasSeqNoPropertyNames ? `seqNoPropertyNames [${seqNoPropertyNames.join(', ')}]` : `eventIdAndSeqNos ${JSON.stringify(eventIdAndSeqNos)}`}`;
    context.error(errMsg);
    throw errorWithReason(new Error(errMsg), 'Sequencing is required, but failed to resolve any seqNos');
  }

  return {ids, keys, seqNos};
}

function md5Sum(data) {
  return crypto.createHash('md5').update(data).digest("hex");
}

/**
 * Attempts to get the values for each of the given property names from the given message & other sources.
 * @param {string[]} propertyNames
 * @param {Message} message
 * @param {Record} record
 * @param {Object} keysMap
 * @param {Object} newImageMap
 * @param {Object} oldImageMap
 * @param {boolean} throwErrorIfPropertyMissing
 * @param {string} desc
 * @param {string} forName
 * @param {StreamConsumerContext} context
 * @returns {KeyValuePair[]} an array of key-value pairs
 */
function getPropertyValues(propertyNames, message, record, keysMap, newImageMap, oldImageMap, throwErrorIfPropertyMissing, desc, forName, context) {
  const missingProperties = [];
  const keyValuePairs = propertyNames.map(propertyName => {
    const value = getPropertyValue(propertyName, message, record, keysMap, newImageMap, oldImageMap, missingProperties);
    return [propertyName, value];
  });

  if (missingProperties.length > 0) {
    const reason = `${missingProperties.length !== 1 ? 'Missing properties' : 'Missing property'} [${missingProperties.join(', ')}] for ${forName}`;
    const errMsg = `${reason} for message (${desc})`;
    if (throwErrorIfPropertyMissing) {
      context.error(errMsg);
      throw errorWithReason(new Error(errMsg), reason);
    }
    context.warn(errMsg);
  }
  return keyValuePairs;
}

function getPropertyValue(propertyName, message, record, keysMap, newImageMap, oldImageMap, missingProperties) {
  // First look for the property on the message
  if (hasOwnPropertyWithCompoundName(message, propertyName)) {
    return getPropertyValueByCompoundName(message, propertyName);
  }

  // Next look for the property on the normalized Keys "map"
  if (keysMap && hasOwnPropertyWithCompoundName(keysMap, propertyName)) {
    return getPropertyValueByCompoundName(keysMap, propertyName);
  }

  // Next look for the property on the normalized NewImage "map" (if any)
  if (newImageMap && hasOwnPropertyWithCompoundName(newImageMap, propertyName)) {
    return getPropertyValueByCompoundName(newImageMap, propertyName);
  }

  // Next look for the property on the normalized OldImage "map" (if any)
  if (oldImageMap && hasOwnPropertyWithCompoundName(oldImageMap, propertyName)) {
    return getPropertyValueByCompoundName(oldImageMap, propertyName);
  }

  // Next look for the property on the message.dynamodb object
  if (message.dynamodb && hasOwnPropertyWithCompoundName(message.dynamodb, propertyName)) {
    return getPropertyValueByCompoundName(message.dynamodb, propertyName);
  }

  // Next look for the property on the record.dynamodb object
  if (record.dynamodb && hasOwnPropertyWithCompoundName(record.dynamodb, propertyName)) {
    return getPropertyValueByCompoundName(record.dynamodb, propertyName);
  }

  // Last look for the property on the record
  if (record && hasOwnPropertyWithCompoundName(record, propertyName)) {
    return getPropertyValueByCompoundName(record, propertyName);
  }

  // Add the missing property's name to the list
  missingProperties.push(propertyName);
  return undefined;
}

function errorWithReason(error, reason) {
  Object.defineProperty(error, 'reason', {value: reason, enumerable: false, writable: true, configurable: true});
  return error;
}