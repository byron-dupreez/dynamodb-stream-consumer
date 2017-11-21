'use strict';

/**
 * Unit tests for aws-stream-consumer/identify.js
 * @author Byron du Preez
 */

const test = require('tape');

// The test subject
const dynamoIdentify = require('../dynamo-identify');

const samples = require('./samples');
const sampleDynamoDBMessageAndRecord = samples.sampleDynamoDBMessageAndRecord;

const strings = require('core-functions/strings');
const stringify = strings.stringify;

const logging = require('logging-utils');
const LogLevel = logging.LogLevel;

const eventSourceARN = samples.sampleDynamoDBEventSourceArn('us-west-2', 'TestTable_DEV', '2017-03-13T21:33:45');

const crypto = require('crypto');

function md5Sum(data) {
  return crypto.createHash('md5').update(data).digest("hex");
}

function createContext(streamType, idPropertyNames, keyPropertyNames, seqNoPropertyNames) {
  const context = {
    streamProcessing: {
      streamType: streamType,
      sequencingRequired: true,
      sequencingPerKey: false,
      consumerIdSuffix: undefined,
      idPropertyNames: idPropertyNames,
      keyPropertyNames: keyPropertyNames,
      seqNoPropertyNames: seqNoPropertyNames,
      generateMD5s: dynamoIdentify.generateDynamoMD5s,
      resolveEventIdAndSeqNos: dynamoIdentify.resolveDynamoEventIdAndSeqNos,
      resolveMessageIdsAndSeqNos: dynamoIdentify.resolveDynamoMessageIdsAndSeqNos
    }
  };
  logging.configureLogging(context, {logLevel: LogLevel.TRACE});
  return context;
}

function checkEventIdAndSeqNos(t, eventIdAndSeqNos, expectedEventID, expectedEventSeqNo) {
  t.equal(eventIdAndSeqNos.eventID, expectedEventID, `eventID must be '${expectedEventID}'`);
  t.equal(eventIdAndSeqNos.eventSeqNo, expectedEventSeqNo, `eventSeqNo must be '${expectedEventSeqNo}'`);
}

function checkMsgIdsKeysAndSeqNos(t, messageIdsAndSeqNos, expectedIds, expectedKeys, expectedSeqNos) {
  t.deepEqual(messageIdsAndSeqNos.ids, expectedIds, `ids must be ${stringify(expectedIds)}`);
  t.deepEqual(messageIdsAndSeqNos.keys, expectedKeys, `keys must be ${stringify(expectedKeys)}`);
  t.deepEqual(messageIdsAndSeqNos.seqNos, expectedSeqNos, `seqNos must be ${stringify(expectedSeqNos)}`);
}

function resolveMessageIdsAndSeqNos(msg, rec, context) {
  const md5s = dynamoIdentify.generateDynamoMD5s(msg, rec);
  const eventIdAndSeqNos = dynamoIdentify.resolveDynamoEventIdAndSeqNos(rec);
  return dynamoIdentify.resolveDynamoMessageIdsAndSeqNos(msg, rec, undefined, eventIdAndSeqNos, md5s, context);
}

// =====================================================================================================================
// generateDynamoMD5s - dynamodb
// =====================================================================================================================

test(`generateDynamoMD5s for dynamodb`, t => {
  const eventID = 'E001';
  const eventSeqNo = '10000000000000000';

  const [msg, rec] = sampleDynamoDBMessageAndRecord(eventID, eventSeqNo, eventSourceARN, '123', 456, 'ABC', 10, 1, 2, 3);

  const md5s = dynamoIdentify.generateDynamoMD5s(msg, rec);

  let md5 = md5Sum(JSON.stringify(msg));
  t.equal(md5s.msg, md5, `md5s.msg must be ${md5}`);

  md5 = md5Sum(JSON.stringify(rec));
  t.equal(md5s.rec, md5, `md5s.rec must be ${md5}`);

  t.end();
});

test(`generateDynamoMD5s for dynamodb with no message`, t => {
  const eventID = 'E002';
  const eventSeqNo = '10000000000000000';

  const [, rec] = sampleDynamoDBMessageAndRecord(eventID, eventSeqNo, eventSourceARN, '123', 456, 'ABC', 10, 1, 2, 3);

  const md5s = dynamoIdentify.generateDynamoMD5s(undefined, rec);

  let md5 = undefined;
  t.equal(md5s.msg, md5, `md5s.msg must be ${md5}`);

  md5 = md5Sum(JSON.stringify(rec));
  t.equal(md5s.rec, md5, `md5s.rec must be ${md5}`);

  t.end();
});

test(`generateDynamoMD5s for dynamodb with no message & no record`, t => {
  const md5s = dynamoIdentify.generateDynamoMD5s(undefined, undefined);

  let md5 = undefined;
  t.equal(md5s.msg, md5, `md5s.msg must be ${md5}`);

  md5 = undefined;
  t.equal(md5s.rec, md5, `md5s.rec must be ${md5}`);

  t.end();
});

// =====================================================================================================================
// resolveEventIdAndSeqNos - dynamodb
// =====================================================================================================================

test(`resolveKinesisEventIdAndSeqNos for dynamodb`, t => {
  const eventID = 'E001';
  const eventSeqNo = '10000000000000000';

  const [, rec] = sampleDynamoDBMessageAndRecord(eventID, eventSeqNo, eventSourceARN, '123', 456, 'ABC', 10, 1, 2, 3);

  const eventIdAndSeqNos = dynamoIdentify.resolveDynamoEventIdAndSeqNos(rec);

  checkEventIdAndSeqNos(t, eventIdAndSeqNos, rec.eventID, rec.dynamodb.SequenceNumber);

  t.end();
});

test(`resolveKinesisEventIdAndSeqNos for dynamodb with no record`, t => {
  const eventIdAndSeqNos = dynamoIdentify.resolveDynamoEventIdAndSeqNos(undefined);

  checkEventIdAndSeqNos(t, eventIdAndSeqNos, undefined, undefined);

  t.end();
});

// =====================================================================================================================
// resolveMessageIdsAndSeqNos - dynamodb
// =====================================================================================================================

test(`resolveMessageIdsAndSeqNos for dynamodb WITHOUT configured property names (all empty arrays)`, t => {
  const context = createContext('dynamodb', [], [], []);
  const eventID = 'E001';
  const eventSeqNo = '10000000000000000';

  const [msg, rec] = sampleDynamoDBMessageAndRecord(eventID, eventSeqNo, eventSourceARN, '123', 456, 'ABC', 10, 1, 2, 3);

  const messageIdsAndSeqNos = resolveMessageIdsAndSeqNos(msg, rec, context);

  const expectedIds = []; //[['k1', 'ABC'], ['k2', 10], ['eventSeqNo', eventSeqNo]];
  const expectedKeys = [['k1', 'ABC'], ['k2', 10]];
  const expectedSeqNos = [['eventSeqNo', eventSeqNo]];
  checkMsgIdsKeysAndSeqNos(t, messageIdsAndSeqNos, expectedIds, expectedKeys, expectedSeqNos);

  t.end();
});

test(`resolveMessageIdsAndSeqNos for dynamodb WITHOUT configured property names (all undefined)`, t => {
  const context = createContext('dynamodb', ['id1', 'id2'], ['k1', 'k2'], ['n1', 'n2', 'n3']);
  const eventID = 'E001';
  const eventSeqNo = '10000000000000000';

  const [msg, rec] = sampleDynamoDBMessageAndRecord(eventID, eventSeqNo, eventSourceARN, '123', 456, 'ABC', 10, 1, 2, 3);

  const messageIdsAndSeqNos = resolveMessageIdsAndSeqNos(msg, rec, context);

  const expectedIds = [['id1', '123'], ['id2', '456']];
  const expectedKeys = [['k1', 'ABC'], ['k2', 10]];
  const expectedSeqNos = [['n1', 1], ['n2', 2], ['n3', 3]];
  checkMsgIdsKeysAndSeqNos(t, messageIdsAndSeqNos, expectedIds, expectedKeys, expectedSeqNos);

  t.end();
});

test(`resolveMessageIdsAndSeqNos for dynamodb with all property names configured & all properties present`, t => {
  const context = createContext('dynamodb', ['id2', 'id1'], ['k2', 'k1'], ['n3','n2','n1'] );
  const eventID = 'E001';
  const eventSeqNo = '10000000000000000';

  const [msg, rec] = sampleDynamoDBMessageAndRecord(eventID, eventSeqNo, eventSourceARN, '123', '456', 'ABC', 10, 1, 2, 3);

  const messageIdsAndSeqNos = resolveMessageIdsAndSeqNos(msg, rec, context);

  const expectedIds = [['id2', '456'], ['id1', '123']];
  const expectedKeys = [['k2', 10], ['k1', 'ABC']];
  const expectedSeqNos = [['n3', 3], ['n2', 2], ['n1', 1]];
  checkMsgIdsKeysAndSeqNos(t, messageIdsAndSeqNos, expectedIds, expectedKeys, expectedSeqNos, eventID, eventSeqNo);

  t.end();
});

test(`resolveMessageIdsAndSeqNos for dynamodb with all property names NOT configured, missing Keys & keys and message sequencing required AND sequencingPerKey is true`, t => {
  const context = createContext('dynamodb', [], [], []);
  context.streamProcessing.sequencingPerKey = true;

  const eventID = 'E001';
  const eventSeqNo = '10000000000000000';

  const [msg, rec] = sampleDynamoDBMessageAndRecord(eventID, eventSeqNo, eventSourceARN, '123', '456', 'ABC', 10, 1, 2, 3);

  // Remove any trace of dynamodb.Keys information
  delete msg.dynamodb.keys;
  delete rec.dynamodb.Keys;
  // delete msg.dynamodb.Keys;

  t.throws(() => resolveMessageIdsAndSeqNos(msg, rec, context), /Failed to resolve any keys/, `no keys must throw error`);

  t.end();
});

test(`resolveMessageIdsAndSeqNos for dynamodb with all property names NOT configured, missing Keys & keys and message sequencing required, but sequencingPerKey false`, t => {
  const context = createContext('dynamodb', [], [], []);
  context.streamProcessing.sequencingPerKey = false;

  const eventID = 'E001';
  const eventSeqNo = '10000000000000000';

  const [msg, rec] = sampleDynamoDBMessageAndRecord(eventID, eventSeqNo, eventSourceARN, '123', '456', 'ABC', 10, 1, 2, 3);

  // Remove any trace of dynamodb.Keys information
  delete msg.dynamodb.keys;
  delete rec.dynamodb.Keys;
  // delete msg.dynamodb.Keys;

  const messageIdsAndSeqNos = resolveMessageIdsAndSeqNos(msg, rec, context);

  const expectedIds = [];
  const expectedKeys = [];
  const expectedSeqNos = [['eventSeqNo', eventSeqNo]];
  checkMsgIdsKeysAndSeqNos(t, messageIdsAndSeqNos, expectedIds, expectedKeys, expectedSeqNos, eventID, eventSeqNo);

  t.end();
});

test(`resolveMessageIdsAndSeqNos for dynamodb with all property names NOT configured, missing Keys & keys and message sequencing NOT required`, t => {
  const context = createContext('dynamodb', [], [], []);
  context.streamProcessing.sequencingRequired = false;
  context.streamProcessing.sequencingPerKey = true;

  const eventID = 'E001';
  const eventSeqNo = '10000000000000000';
  const [msg, rec] = sampleDynamoDBMessageAndRecord(eventID, eventSeqNo, eventSourceARN, '123', '456', 'ABC', 10, 1, 2, 3);

  // Remove any trace of dynamodb.Keys information
  delete rec.dynamodb.Keys;
  delete msg.dynamodb.keys;

  const messageIdsAndSeqNos = resolveMessageIdsAndSeqNos(msg, rec, context);

  const expectedIds = [];
  const expectedKeys = [];
  const expectedSeqNos = [['eventSeqNo', eventSeqNo]];
  checkMsgIdsKeysAndSeqNos(t, messageIdsAndSeqNos, expectedIds, expectedKeys, expectedSeqNos, eventID, eventSeqNo);

  t.end();
});

test(`resolveMessageIdsAndSeqNos for dynamodb with all property names configured and with a key property undefined AND sequencingPerKey true`, t => {
  const context = createContext('dynamodb', ['id1', 'id2'], ['k1', 'k2'], ['n1', 'n2', 'n3']);
  context.streamProcessing.sequencingPerKey = true;

  const eventID = 'E001';
  const eventSeqNo = '10000000000000000';

  const [msg, rec] = sampleDynamoDBMessageAndRecord(eventID, eventSeqNo, eventSourceARN, '456', '789', 'ABC', undefined, '1', '2', '3');

  t.throws(() => resolveMessageIdsAndSeqNos(msg, rec, context), /Missing property \[k2] for keys for message/, `any undefined key must throw error`);

  t.end();
});

test(`resolveMessageIdsAndSeqNos for dynamodb with all property names configured and with BOTH key properties undefined AND sequencingPerKey true`, t => {
  const context = createContext('dynamodb', ['id1', 'id2'], ['k1', 'k2'], ['n1', 'n2', 'n3']);
  context.streamProcessing.sequencingPerKey = true;

  const eventID = 'E001';
  const eventSeqNo = '10000000000000000';

  const [msg, rec] = sampleDynamoDBMessageAndRecord(eventID, eventSeqNo, eventSourceARN, '456', '789', undefined, undefined, '1', '2', '3');

  t.throws(() => resolveMessageIdsAndSeqNos(msg, rec, context), /Missing properties \[k1, k2] for keys for message/, `any undefined key must throw error`);

  t.end();
});

test(`resolveMessageIdsAndSeqNos for dynamodb with all property names configured and with a key property undefined AND sequencingPerKey false`, t => {
  const context = createContext('dynamodb', ['id1', 'id2'], ['k1', 'k2'], ['n1', 'n2', 'n3']);
  context.streamProcessing.sequencingPerKey = false;

  const eventID = 'E001';
  const eventSeqNo = '10000000000000000';

  const [msg, rec] = sampleDynamoDBMessageAndRecord(eventID, eventSeqNo, eventSourceARN, '456', '789', 'ABC', undefined, '1', '2', '3');

  const messageIdsAndSeqNos = resolveMessageIdsAndSeqNos(msg, rec, context);

  const expectedIds = [['id1', '456'], ['id2', '789']];
  const expectedKeys = [['k1', 'ABC'], ['k2', undefined]];
  const expectedSeqNos = [['n1', 1], ['n2', 2], ['n3', 3]];
  checkMsgIdsKeysAndSeqNos(t, messageIdsAndSeqNos, expectedIds, expectedKeys, expectedSeqNos, eventID, eventSeqNo);

  t.end();
});

test(`resolveMessageIdsAndSeqNos for dynamodb with all property names configured and with an id property undefined`, t => {
  const context = createContext('dynamodb', ['id1', 'id2'], ['k1', 'k2'], ['n1', 'n2', 'n3']);
  context.streamProcessing.sequencingPerKey = true;

  const eventID = 'E001';
  const eventSeqNo = '10000000000000000';

  const [msg, rec] = sampleDynamoDBMessageAndRecord(eventID, eventSeqNo, eventSourceARN, '123', undefined, 'ABC', 10, 1, 2, 3);

  const messageIdsAndSeqNos = resolveMessageIdsAndSeqNos(msg, rec, context);

  const expectedIds = [['id1', '123'], ['id2', undefined]];
  const expectedKeys = [['k1', 'ABC'], ['k2', 10]];
  const expectedSeqNos = [['n1', 1], ['n2', 2], ['n3', 3]];
  checkMsgIdsKeysAndSeqNos(t, messageIdsAndSeqNos, expectedIds, expectedKeys, expectedSeqNos, eventID, eventSeqNo);

  t.end();
});

test(`resolveMessageIdsAndSeqNos for dynamodb with all property names configured and with a sequence number property undefined`, t => {
  const context = createContext('dynamodb', ['id1', 'id2'], ['k1', 'k2'], ['n1', 'n2', 'n3', 'n4']);
  const eventID = 'E001';
  const eventSeqNo = '10000000000000000';

  const [msg, rec] = sampleDynamoDBMessageAndRecord(eventID, eventSeqNo, eventSourceARN, '123', '456', 'ABC', '10', 10, 1, 2, undefined);

  t.throws(() => resolveMessageIdsAndSeqNos(msg, rec, context), /Missing property \[n4] for seqNos for message/, `any undefined sequence number must throw error`);

  t.end();
});