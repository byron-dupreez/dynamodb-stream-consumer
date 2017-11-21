'use strict';

/**
 * Unit tests for aws-stream-consumer/sequencing.js
 * @author Byron du Preez
 */

const test = require('tape');

// The test subject
const sequencing = require('aws-stream-consumer/sequencing');
const sorting = require('core-functions/sorting');
const SortType = sorting.SortType;

const samples = require('./samples');
const sampleDynamoDBMessageAndRecord = samples.sampleDynamoDBMessageAndRecord;

const Batch = require('aws-stream-consumer/batch');

// Setting-related utilities
const Settings = require('aws-stream-consumer/settings');
const StreamType = Settings.StreamType;

const strings = require('core-functions/strings');
const stringify = strings.stringify;

const identify = require('../dynamo-identify');

const logging = require('logging-utils');
const LogLevel = logging.LogLevel;

const eventSourceARN = samples.sampleDynamoDBEventSourceArn('us-west-2', 'TestTable_DEV', '2017-03-13T21:33:45');

// Dummy discard unusable record function
const discardUnusableRecord = (unusableRecord, batch, context) => {
  const i = batch.unusableRecords.indexOf(unusableRecord);
  context.info(`Running discardUnusableRecord with unusable record[${i}]`);
  return Promise.resolve({index: i});
};

// Dummy discard rejected message function
const discardRejectedMessage = (rejectedMessage, batch, context) => {
  const i = batch.messages.indexOf(rejectedMessage);
  context.info(`Running discardRejectedMessage with message[${i}]`);
  return Promise.resolve({index: i});
};

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
      generateMD5s: identify.generateDynamoMD5s,
      resolveEventIdAndSeqNos: identify.resolveDynamoEventIdAndSeqNos,
      resolveMessageIdsAndSeqNos: identify.resolveDynamoMessageIdsAndSeqNos,
      discardUnusableRecord: discardUnusableRecord,
      discardRejectedMessage: discardRejectedMessage
    }
  };
  logging.configureLogging(context, {logLevel: LogLevel.TRACE});
  return context;
}

function checkMsgSeqNoPart(t, seqNos, m, p, expectedValue, expectedOldValue, expectedSortType) {
  const seqNoPart = seqNos[p];
  const key = seqNoPart[0];

  const actualValue = seqNoPart[1];
  const opts = {quoteStrings: true};
  if (actualValue instanceof Date) {
    t.equal(actualValue.toISOString(), expectedValue, `msg${m} part[${p}] key(${key}): new value Date(${stringify(actualValue, opts)}).toISOString() must be ${stringify(expectedValue, opts)}`);
  } else {
    t.equal(actualValue, expectedValue, `msg${m} part[${p}] key(${key}): new value (${stringify(actualValue, opts)}) must be ${stringify(expectedValue, opts)}`);
  }
  t.equal(seqNoPart.oldValue, expectedOldValue, `msg${m} part[${p}] key(${key}): old value (${stringify(seqNoPart.oldValue, opts)}) must be ${stringify(expectedOldValue, opts)}`);
  t.equal(seqNoPart.sortable.sortType, expectedSortType, `msg${m} part[${p}] key(${key}): sortable.sortType (${stringify(seqNoPart.sortable.sortType, opts)}) must be ${stringify(expectedSortType, opts)}`);
}

// =====================================================================================================================
// prepareMessagesForSequencing - dynamodb
// =====================================================================================================================

test(`prepareMessagesForSequencing for dynamodb`, t => {
  const context = createContext(StreamType.dynamodb, [], ['k1', 'k2'], ['n1', 'n2', 'n3', 'n4']);

  const eventID1 = 'E001';
  const seqNo1 = '10000000000000001';
  const [msg1, rec1] = sampleDynamoDBMessageAndRecord(eventID1, seqNo1, eventSourceARN, undefined, undefined, 'ABC', 10, 1.1, 100, 3000, '2017-01-17T23:59:59.001Z');

  const eventID2 = 'E002';
  const seqNo2 = '10000000000000002';
  const [msg2, rec2] = sampleDynamoDBMessageAndRecord(eventID2, seqNo2, eventSourceARN, undefined, undefined, 'ABC', 10, 2.5, 200, '20000000000000000000002', '2017-01-17T23:59:59.002Z');

  const eventID3 = 'E003';
  const seqNo3 = 10003;
  const [msg3, rec3] = sampleDynamoDBMessageAndRecord(eventID3, seqNo3, eventSourceARN, undefined, undefined, 'ABC', 10, 3.3, 300, '30000000000000000000003', '2017-01-17T23:59:59.003Z');

  const batch = new Batch([rec1, rec2, rec3], [], [], context);
  const states = batch.states;
  batch.addMessage(msg1, rec1, undefined, context);
  batch.addMessage(msg2, rec2, undefined, context);
  batch.addMessage(msg3, rec3, undefined, context);

  const msgs = batch.messages;
  // const msgs = [msg1, msg2, msg3];

  sequencing.prepareMessagesForSequencing(msgs, states, context);

  checkMsgSeqNoPart(t, states.get(msg1).seqNos, 1, 0, 1.1, 1.1, SortType.NUMBER);
  checkMsgSeqNoPart(t, states.get(msg2).seqNos, 2, 0, 2.5, 2.5, SortType.NUMBER);
  checkMsgSeqNoPart(t, states.get(msg3).seqNos, 3, 0, 3.3, 3.3, SortType.NUMBER);

  checkMsgSeqNoPart(t, states.get(msg1).seqNos, 1, 1, 100, 100, SortType.NUMBER);
  checkMsgSeqNoPart(t, states.get(msg2).seqNos, 2, 1, 200, 200, SortType.NUMBER);
  checkMsgSeqNoPart(t, states.get(msg3).seqNos, 3, 1, 300, 300, SortType.NUMBER);

  checkMsgSeqNoPart(t, states.get(msg1).seqNos, 1, 2, '3000', 3000, SortType.INTEGER_LIKE);
  checkMsgSeqNoPart(t, states.get(msg2).seqNos, 2, 2, '20000000000000000000002', '20000000000000000000002', SortType.INTEGER_LIKE);
  checkMsgSeqNoPart(t, states.get(msg3).seqNos, 3, 2, '30000000000000000000003', '30000000000000000000003', SortType.INTEGER_LIKE);

  checkMsgSeqNoPart(t, states.get(msg1).seqNos, 1, 3, '2017-01-17T23:59:59.001Z', '2017-01-17T23:59:59.001Z', SortType.DATE_TIME);
  checkMsgSeqNoPart(t, states.get(msg2).seqNos, 2, 3, '2017-01-17T23:59:59.002Z', '2017-01-17T23:59:59.002Z', SortType.DATE_TIME);
  checkMsgSeqNoPart(t, states.get(msg3).seqNos, 3, 3, '2017-01-17T23:59:59.003Z', '2017-01-17T23:59:59.003Z', SortType.DATE_TIME);

  t.end();
});

test(`prepareMessagesForSequencing for dynamodb with sequence numbers with different keys & sequencing required`, t => {
  const context1 = createContext('dynamodb', [], ['k2', 'k1'], ['n3', 'n2', 'n1']);
  const context3 = createContext('dynamodb', [], ['k2', 'k1'], ['n1', 'n2', 'n3']);

  const eventID1 = 'E001';
  const seqNo1 = '10000000000000001';
  const [msg1, rec1] = sampleDynamoDBMessageAndRecord(eventID1, seqNo1, eventSourceARN, undefined, undefined, 'ABC', 10, 1.1, 100, 3000);

  const eventID2 = 'E002';
  const seqNo2 = '10000000000000002';
  const [msg2, rec2] = sampleDynamoDBMessageAndRecord(eventID2, seqNo2, eventSourceARN, undefined, undefined, 'ABC', 10, '2.5', '200', '20000000000000000000002');

  const eventID3 = 'E003';
  const seqNo3 = 10003;
  const [msg3, rec3] = sampleDynamoDBMessageAndRecord(eventID3, seqNo3, eventSourceARN, undefined, undefined, 'ABC', 10, 3.3, 300, '30000000000000000000003');

  const batch = new Batch([rec1, rec2, rec3], [], [], context1);
  const states = batch.states;
  batch.addMessage(msg2, rec2, undefined, context1);
  batch.addMessage(msg1, rec1, undefined, context1);
  batch.addMessage(msg3, rec3, undefined, context3); // different context!

  const msgs = batch.messages;
  // const msgs = [msg2, msg1, msg3];

  t.throws(() => sequencing.prepareMessagesForSequencing(msgs, states, context3), /NOT all of the messages have the same key at sequence number part\[0\]/, `different seq number keys must throw an error`);

  t.end();
});

test(`prepareMessagesForSequencing for dynamodb with sequence numbers with different keys & sequencing NOT required`, t => {
  const context1 = createContext('dynamodb', [], ['k2', 'k1'], ['n3', 'n2', 'n1', 'n4', 'n5']);
  context1.streamProcessing.sequencingRequired = false;

  const context3 = createContext('dynamodb', [], ['k2', 'k1'], ['n1', 'n2', 'n3', 'n4']); // msg3 has NO 'n5'
  context3.streamProcessing.sequencingRequired = false;

  const eventID1 = 'E001';
  const seqNo1 = '10000000000000001';
  const [msg1, rec1] = sampleDynamoDBMessageAndRecord(eventID1, seqNo1, eventSourceARN, undefined, undefined, 'ABC', 10, 1.1, 100, 3000, '2016-12-30', 'zZz');

  const eventID2 = 'E002';
  const seqNo2 = '10000000000000002';
  const [msg2, rec2] = sampleDynamoDBMessageAndRecord(eventID2, seqNo2, eventSourceARN, undefined, undefined, 'ABC', 10, 2.5, 200, '20000000000000000000002', '2016-12-31', 'Abc');

  const eventID3 = 'E003';
  const seqNo3 = 10003;
  const [msg3, rec3] = sampleDynamoDBMessageAndRecord(eventID3, seqNo3, eventSourceARN, undefined, undefined, 'ABC', 10, 3.3, 300, '30000000000000000000003', '2017-01-01');

  const batch = new Batch([rec1, rec2, rec3], [], [], context3);
  const states = batch.states;
  batch.addMessage(msg1, rec1, undefined, context1);
  batch.addMessage(msg2, rec2, undefined, context1);
  batch.addMessage(msg3, rec3, undefined, context3); // different context!

  const msgs = batch.messages; // [msg1, msg2, msg3];

  sequencing.prepareMessagesForSequencing(msgs, states, context3);

  checkMsgSeqNoPart(t, states.get(msg1).seqNos, 1, 0, '3000', 3000, SortType.INTEGER_LIKE);
  checkMsgSeqNoPart(t, states.get(msg2).seqNos, 2, 0, '20000000000000000000002', '20000000000000000000002', SortType.INTEGER_LIKE);
  checkMsgSeqNoPart(t, states.get(msg3).seqNos, 3, 0, 3.3, 3.3, SortType.NUMBER);

  checkMsgSeqNoPart(t, states.get(msg1).seqNos, 1, 1, 100, 100, SortType.NUMBER);
  checkMsgSeqNoPart(t, states.get(msg2).seqNos, 2, 1, 200, 200, SortType.NUMBER);
  checkMsgSeqNoPart(t, states.get(msg3).seqNos, 3, 1, 300, 300, SortType.NUMBER);

  checkMsgSeqNoPart(t, states.get(msg1).seqNos, 1, 2, 1.1, 1.1, SortType.NUMBER);
  checkMsgSeqNoPart(t, states.get(msg2).seqNos, 2, 2, 2.5, 2.5, SortType.NUMBER);
  checkMsgSeqNoPart(t, states.get(msg3).seqNos, 3, 2, '30000000000000000000003', '30000000000000000000003', SortType.INTEGER_LIKE);

  checkMsgSeqNoPart(t, states.get(msg1).seqNos, 1, 3, '2016-12-30T00:00:00.000Z', '2016-12-30', SortType.DATE);
  checkMsgSeqNoPart(t, states.get(msg2).seqNos, 2, 3, '2016-12-31T00:00:00.000Z', '2016-12-31', SortType.DATE);
  checkMsgSeqNoPart(t, states.get(msg3).seqNos, 3, 3, '2017-01-01T00:00:00.000Z', '2017-01-01', SortType.DATE);

  checkMsgSeqNoPart(t, states.get(msg1).seqNos, 1, 4, 'zZz', 'zZz', SortType.STRING);
  checkMsgSeqNoPart(t, states.get(msg2).seqNos, 2, 4, 'Abc', 'Abc', SortType.STRING);

  t.end();
});

// =====================================================================================================================
// compareAnyKeyMessages - dynamodb
// =====================================================================================================================

test(`compareAnyKeyMessages for dynamodb with NO key & sequence property names`, t => {
  const context = createContext('dynamodb', [], [], []);

  const eventID1 = 'E001';
  const seqNo1 = '10000000000000001';
  const [msg1, rec1] = sampleDynamoDBMessageAndRecord(eventID1, seqNo1, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.001Z');

  const eventID2 = 'E002';
  const seqNo2 = '10000000000000002';
  const [msg2, rec2] = sampleDynamoDBMessageAndRecord(eventID2, seqNo2, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.002Z');

  const eventID3 = 'E003';
  const seqNo3 = 10003;
  const [msg3, rec3] = sampleDynamoDBMessageAndRecord(eventID3, seqNo3, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.003Z');

  const batch = new Batch([rec1, rec2, rec3], [], [], context);
  const states = batch.states;
  batch.addMessage(msg1, rec1, undefined, context);
  batch.addMessage(msg2, rec2, undefined, context);
  batch.addMessage(msg3, rec3, undefined, context);

  const msgs = batch.messages;
  // const msgs = [msg1, msg2, msg3];

  // Normalize all of the messages' sequence numbers
  sequencing.prepareMessagesForSequencing(msgs, states, context);

  t.ok(sequencing.compareAnyKeyMessages(msg1, msg1, states, context) === 0, `msg 1 = msg 1`);
  t.ok(sequencing.compareAnyKeyMessages(msg2, msg2, states, context) === 0, `msg 2 = msg 2`);
  t.ok(sequencing.compareAnyKeyMessages(msg3, msg3, states, context) === 0, `msg 3 = msg 3`);

  t.ok(sequencing.compareAnyKeyMessages(msg1, msg2, states, context) < 0, `msg 1 < msg 2`);
  t.ok(sequencing.compareAnyKeyMessages(msg2, msg1, states, context) > 0, `msg 2 > msg 1`);

  t.ok(sequencing.compareAnyKeyMessages(msg2, msg3, states, context) > 0, `msg 2 > msg 3`);
  t.ok(sequencing.compareAnyKeyMessages(msg3, msg2, states, context) < 0, `msg 3 < msg 2`);

  t.ok(sequencing.compareAnyKeyMessages(msg1, msg3, states, context) > 0, `msg 1 > msg 3`);
  t.ok(sequencing.compareAnyKeyMessages(msg3, msg1, states, context) < 0, `msg 3 < msg 1`);

  t.end();
});

test(`compareAnyKeyMessages for dynamodb with key & sequence property names with difference in 4th seq # part`, t => {
  const context = createContext('dynamodb', [], ['k1', 'k2'], ['n1', 'n2', 'n3', 'n4']);

  const eventID1 = 'E001';
  const seqNo1 = '10000000000000001';
  const [msg1, rec1] = sampleDynamoDBMessageAndRecord(eventID1, seqNo1, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.001Z');

  const eventID2 = 'E002';
  const seqNo2 = '10000000000000002';
  const [msg2, rec2] = sampleDynamoDBMessageAndRecord(eventID2, seqNo2, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.002Z');

  const eventID3 = 'E003';
  const seqNo3 = 10003;
  const [msg3, rec3] = sampleDynamoDBMessageAndRecord(eventID3, seqNo3, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.003Z');

  const batch = new Batch([rec1, rec2, rec3], [], [], context);
  const states = batch.states;
  batch.addMessage(msg1, rec1, undefined, context);
  batch.addMessage(msg2, rec2, undefined, context);
  batch.addMessage(msg3, rec3, undefined, context);

  const msgs = batch.messages;
  // const msgs = [msg1, msg2, msg3];

  // Normalize all of the messages' sequence numbers
  sequencing.prepareMessagesForSequencing(msgs, states, context);

  t.ok(sequencing.compareAnyKeyMessages(msg1, msg1, states, context) === 0, `msg 1 = msg 1`);
  t.ok(sequencing.compareAnyKeyMessages(msg2, msg2, states, context) === 0, `msg 2 = msg 2`);
  t.ok(sequencing.compareAnyKeyMessages(msg3, msg3, states, context) === 0, `msg 3 = msg 3`);

  t.ok(sequencing.compareAnyKeyMessages(msg1, msg2, states, context) < 0, `msg 1 < msg 2`);
  t.ok(sequencing.compareAnyKeyMessages(msg2, msg1, states, context) > 0, `msg 2 > msg 1`);

  t.ok(sequencing.compareAnyKeyMessages(msg2, msg3, states, context) < 0, `msg 2 < msg 3`);
  t.ok(sequencing.compareAnyKeyMessages(msg3, msg2, states, context) > 0, `msg 3 > msg 2`);

  t.ok(sequencing.compareAnyKeyMessages(msg1, msg3, states, context) < 0, `msg 1 < msg 3`);
  t.ok(sequencing.compareAnyKeyMessages(msg3, msg1, states, context) > 0, `msg 3 > msg 1`);

  t.end();
});

// =====================================================================================================================
// sequenceMessages - dynamodb
// =====================================================================================================================

test(`sequenceMessages for dynamodb with NO key and sequence property names`, t => {
  const context = createContext('dynamodb', [], [], []);

  // 3 messages with the same key differing ONLY in sequence number)
  const eventID1 = 'E001';
  const seqNo1 = '10000000000000003';
  const [msg1, rec1] = sampleDynamoDBMessageAndRecord(eventID1, seqNo1, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.001Z');

  const eventID2 = 'E002';
  const seqNo2 = '10000000000000005';
  const [msg2, rec2] = sampleDynamoDBMessageAndRecord(eventID2, seqNo2, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.002Z');

  const eventID3 = 'E003';
  const seqNo3 = '10000000000000000';
  const [msg3, rec3] = sampleDynamoDBMessageAndRecord(eventID3, seqNo3, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.003Z');

  // 2 messages with a different key (differing ONLY in sequence number from each other)
  const eventID4 = 'E004';
  const seqNo4 = '10000000000000004';
  const [msg4, rec4] = sampleDynamoDBMessageAndRecord(eventID4, seqNo4, eventSourceARN, undefined, undefined, 'ABC', 11, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.001Z');

  const eventID5 = 'E005';
  const seqNo5 = '10000000000000002';
  const [msg5, rec5] = sampleDynamoDBMessageAndRecord(eventID5, seqNo5, eventSourceARN, undefined, undefined, 'ABC', 11, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.002Z');

  // 1 message with a different key
  const eventID6 = 'E006';
  const seqNo6 = '10000000000000001';
  const [msg6, rec6] = sampleDynamoDBMessageAndRecord(eventID6, seqNo6, eventSourceARN, undefined, undefined, 'XYZ', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.003Z');

  const batch = new Batch([rec1, rec2, rec3, rec4, rec5, rec6], [], [], context);
  const states = batch.states;
  batch.addMessage(msg2, rec2, undefined, context);
  batch.addMessage(msg5, rec5, undefined, context);
  batch.addMessage(msg6, rec6, undefined, context);
  batch.addMessage(msg1, rec1, undefined, context);
  batch.addMessage(msg4, rec4, undefined, context);
  batch.addMessage(msg3, rec3, undefined, context);

  // Sequence the messages
  const firstMessagesToProcess = sequencing.sequenceMessages(batch, context);

  const msgState1 = states.get(msg1);
  const msgState2 = states.get(msg2);
  const msgState3 = states.get(msg3);
  const msgState4 = states.get(msg4);
  const msgState5 = states.get(msg5);
  const msgState6 = states.get(msg6);

  // expected sequence: [msg3, msg6, msg5, msg1, msg4, msg2]

  t.equal(msgState3.prevMessage, undefined, `msg3.prevMessage must be undefined`);

  t.equal(msgState3.nextMessage, msg6, `msg3.nextMessage must be msg6`);
  t.equal(msgState6.prevMessage, msg3, `msg6.prevMessage must be msg3`);

  t.equal(msgState6.nextMessage, msg5, `msg6.nextMessage must be msg5`);
  t.equal(msgState5.prevMessage, msg6, `msg5.prevMessage must be msg6`);

  t.equal(msgState5.nextMessage, msg1, `msg5.nextMessage must be msg1`);
  t.equal(msgState1.prevMessage, msg5, `msg1.prevMessage must be msg5`);

  t.equal(msgState1.nextMessage, msg4, `msg1.nextMessage must be msg4`);
  t.equal(msgState4.prevMessage, msg1, `msg4.prevMessage must be msg1`);

  t.equal(msgState4.nextMessage, msg2, `msg4.nextMessage must be msg2`);
  t.equal(msgState2.prevMessage, msg4, `msg2.prevMessage must be msg4`);

  t.equal(msgState2.nextMessage, undefined, `msg2.nextMessage must be undefined`);

  t.equal(firstMessagesToProcess.length, 1, `firstMessagesToProcess must have 1 message`);
  t.notEqual(firstMessagesToProcess.indexOf(msg3), -1, `firstMessagesToProcess must contain msg3`);

  t.end();
});

test(`sequenceMessages for dynamodb with key and sequence property names`, t => {
  const context = createContext('dynamodb', [], ['k1', 'k2'], ['n1', 'n2', 'n3', 'n4']);

  // 3 messages with the same key differing ONLY in sequence number)
  const eventID1 = 'E001';
  const seqNo1 = '10000000000000001';
  const [msg1, rec1] = sampleDynamoDBMessageAndRecord(eventID1, seqNo1, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.004Z');

  const eventID2 = 'E002';
  const seqNo2 = '10000000000000002';
  const [msg2, rec2] = sampleDynamoDBMessageAndRecord(eventID2, seqNo2, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.005Z');

  const eventID3 = 'E003';
  const seqNo3 = '10000000000000000';
  const [msg3, rec3] = sampleDynamoDBMessageAndRecord(eventID3, seqNo3, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.002Z');

  // 2 messages with a different key (differing ONLY in sequence number from each other)
  const eventID4 = 'E004';
  const seqNo4 = '10000000000000001';
  const [msg4, rec4] = sampleDynamoDBMessageAndRecord(eventID4, seqNo4, eventSourceARN, undefined, undefined, 'ABC', 11, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.003Z');

  const eventID5 = 'E005';
  const seqNo5 = '10000000000000002';
  const [msg5, rec5] = sampleDynamoDBMessageAndRecord(eventID5, seqNo5, eventSourceARN, undefined, undefined, 'ABC', 11, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.000Z');

  // 1 message with a different key
  const eventID6 = 'E006';
  const seqNo6 = '10000000000000000';
  const [msg6, rec6] = sampleDynamoDBMessageAndRecord(eventID6, seqNo6, eventSourceARN, undefined, undefined, 'XYZ', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.001Z');

  const batch = new Batch([rec1, rec2, rec3, rec4, rec5, rec6], [], [], context);
  const states = batch.states;
  batch.addMessage(msg2, rec2, undefined, context);
  batch.addMessage(msg5, rec5, undefined, context);
  batch.addMessage(msg6, rec6, undefined, context);
  batch.addMessage(msg1, rec1, undefined, context);
  batch.addMessage(msg4, rec4, undefined, context);
  batch.addMessage(msg3, rec3, undefined, context);

  // Sequence the messages
  const firstMessagesToProcess = sequencing.sequenceMessages(batch, context);

  const msgState1 = states.get(msg1);
  const msgState2 = states.get(msg2);
  const msgState3 = states.get(msg3);
  const msgState4 = states.get(msg4);
  const msgState5 = states.get(msg5);
  const msgState6 = states.get(msg6);

  // expected sequence: [msg5, msg6, msg3, msg4, msg1, msg2]

  t.equal(msgState5.prevMessage, undefined, `msg5.prevMessage must be undefined`);

  t.equal(msgState5.nextMessage, msg6, `msg5.nextMessage must be msg6`);
  t.equal(msgState6.prevMessage, msg5, `msg6.prevMessage must be msg5`);

  t.equal(msgState6.nextMessage, msg3, `msg6.nextMessage must be msg3`);
  t.equal(msgState3.prevMessage, msg6, `msg3.prevMessage must be msg6`);

  t.equal(msgState3.nextMessage, msg4, `msg3.nextMessage must be msg4`);
  t.equal(msgState4.prevMessage, msg3, `msg4.prevMessage must be msg3`);

  t.equal(msgState4.nextMessage, msg1, `msg4.nextMessage must be msg1`);
  t.equal(msgState1.prevMessage, msg4, `msg1.prevMessage must be msg4`);

  t.equal(msgState1.nextMessage, msg2, `msg1.nextMessage must be msg2`);
  t.equal(msgState2.prevMessage, msg1, `msg2.prevMessage must be msg1`);

  t.equal(msgState2.nextMessage, undefined, `msg2.nextMessage must be undefined`);

  t.equal(firstMessagesToProcess.length, 1, `firstMessagesToProcess must have 1 message`);
  t.notEqual(firstMessagesToProcess.indexOf(msg5), -1, `firstMessagesToProcess must contain msg5`);

  t.end();
});