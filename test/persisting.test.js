'use strict';

/**
 * Unit tests for aws-stream-consumer/persisting.js to test loading & saving of tracked state for a batch sourced from a
 * DynamoDB stream.
 * @author Byron du Preez
 */

const test = require('tape');

// The test subject
const persisting = require('aws-stream-consumer/persisting');
const toBatchStateItem = persisting.toBatchStateItem;
const saveBatchStateToDynamoDB = persisting.saveBatchStateToDynamoDB;
const loadBatchStateFromDynamoDB = persisting.loadBatchStateFromDynamoDB;

const dynamoProcessing = require('../dynamo-processing');

const Batch = require('aws-stream-consumer/batch');

const Settings = require('aws-stream-consumer/settings');
const StreamType = Settings.StreamType;

const sequencing = require('aws-stream-consumer/sequencing');

const taskUtils = require('task-utils');
const TaskDef = require('task-utils/task-defs');
const Task = require('task-utils/tasks');
const taskStates = require('task-utils/task-states');
const TaskState = taskStates.TaskState;
const core = require('task-utils/core');
const StateType = core.StateType;
const cleanCompletedState = JSON.parse(JSON.stringify(taskStates.instances.Completed));

const logging = require('logging-utils');
const LogLevel = logging.LogLevel;

const Promises = require('core-functions/promises');

const strings = require('core-functions/strings');
const stringify = strings.stringify;

const tries = require('core-functions/tries');
//const Try = tries.Try;
const Success = tries.Success;
// const Failure = tries.Failure;

const noRecordsMsgRegex = /No Batch .* state to save, since 0 records/;
const noMsgsOrUnusableRecordsMsgRegex = /LOGIC FLAWED - No Batch .* state to save, since 0 messages and 0 unusable records/;

const awsRegion = "us-west-2";
const accountNumber = "XXXXXXXXXXXX";

const samples = require('./samples');
const sampleDynamoDBMessageAndRecord = samples.sampleDynamoDBMessageAndRecord;

const eventSourceARN = `arn:aws:dynamodb:${awsRegion}:${accountNumber}:table/MyTable_DEV/stream/2016-10-24T18:14:40.531`;

function createContext(idPropertyNames, keyPropertyNames, seqNoPropertyNames) {
  idPropertyNames = idPropertyNames ? idPropertyNames : ['id1', 'id2'];
  keyPropertyNames = keyPropertyNames ? keyPropertyNames : ['k1', 'k2'];
  seqNoPropertyNames = seqNoPropertyNames ? seqNoPropertyNames : ['n1', 'n2', 'n3'];
  const context = {
    region: awsRegion,
    stage: 'dev'
  };
  const options = {
    streamProcessingOptions: {
      streamType: StreamType.dynamodb,
      sequencingRequired: true,
      sequencingPerKey: false,
      consumerIdSuffix: undefined,
      maxNumberOfAttempts: 2,
      idPropertyNames: idPropertyNames,
      keyPropertyNames: keyPropertyNames,
      seqNoPropertyNames: seqNoPropertyNames,
      batchStateTableName: 'TEST_StreamConsumerBatchState'
    }
  };
  logging.configureLogging(context, {logLevel: LogLevel.TRACE});

  dynamoProcessing.configureDefaultDynamoDBStreamProcessing(context, options.streamProcessingOptions, undefined,
    require('../default-dynamo-options.json'), undefined, undefined, true);

  context.streamProcessing.consumerId = `my-function:${context.stage}`;

  // Set the task factory to use on the context (after configuring logging, so that the context can be used as a logger too)
  taskUtils.configureTaskFactory(context, {logger: context}, undefined);
  return context;
}

function dummyDynamoDBDocClient(t, prefix, error, data, delayMs) {
  const ms = delayMs ? delayMs : 1;
  return {
    put(request) {
      return {
        promise() {
          return Promises.delay(ms).then(() => {
            return new Promise((resolve, reject) => {
              t.pass(`${prefix} simulated put to DynamoDB.DocumentClient with request (${stringify(request)})`);
              if (error)
                reject(error);
              else
                resolve({});
            });
          });
        }
      };
    },

    get(request) {
      return {
        promise() {
          return Promises.delay(ms).then(() => {
            return new Promise((resolve, reject) => {
              t.pass(`${prefix} simulated get from DynamoDB.DocumentClient with request (${stringify(request)})`);
              if (error)
                reject(error);
              else
                resolve(data);
            });
          });
        }
      };
    }

  };
}

function processOne(message, batch, context) {
  const msgState = batch.states.get(message);
  context.trace(`Executing processOne on ${msgState.desc}`);
  return Promise.resolve(msgState.eventID);
}

function processAll(batch, context) {
  const messages = batch.messages;
  context.trace(`Executing processAll on batch (${batch.shardOrEventID})`);
  return Promise.resolve(messages.map(m => batch.states.get(m).eventID));
}

function discardUnusableRecord(unusableRecord, batch, context) {
  context.trace(`Simulating execution of discardUnusableRecord on batch (${batch.shardOrEventID})`);
  return Promise.resolve(unusableRecord);
}

function discardRejectedMessage(rejectedMessage, batch, context) {
  context.trace(`Simulating execution of discardRejectedMessage on batch (${batch.shardOrEventID})`);
  return Promise.resolve(rejectedMessage);
}

function dynamoDBFixture() {
  const context = createContext([], [], []);

  const processOneTaskDef = TaskDef.defineTask(processOne.name, processOne);
  const processAllTaskDef = TaskDef.defineTask(processAll.name, processAll);

  context.streamProcessing.discardUnusableRecord = discardUnusableRecord;
  context.streamProcessing.discardRejectedMessage = discardRejectedMessage;

  // Message 1 - usable
  const eventID1 = 'E001';
  const seqNo1 = '10000000000000001';
  const [msg1, record1] = sampleDynamoDBMessageAndRecord(eventID1, seqNo1, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 100, '10000000000000000000001', '2017-01-17T23:59:59.001Z');

  // Message 2 - "usable", but with rejected task
  const eventID2 = 'E002';
  const seqNo2 = '10000000000000002';
  const [msg2, record2] = sampleDynamoDBMessageAndRecord(eventID2, seqNo2, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 100, '10000000000000000000002', '2017-01-17T23:59:59.002Z');

  // Message 3 - "unusable"
  const eventID3 = 'E003';
  const seqNo3 = '10000000000000003';
  const [msg3, record3] = sampleDynamoDBMessageAndRecord(eventID3, seqNo3, eventSourceARN, undefined, undefined, 'ABC', 10, 1, 100, '10000000000000000000003', '2017-01-17T23:59:59.003Z');

  // Unusable record 1
  const badRec1 = {badApple: true};
  const reason1 = 'Bad apple';

  // Unusable record 2
  // noinspection UnnecessaryLocalVariableJS
  const badRec2 = record3;
  const reason2 = 'Unparseable';

  return {
    context: context,
    cancellable: {},
    processOneTaskDef: processOneTaskDef,
    processAllTaskDef: processAllTaskDef,
    eventID1: eventID1,
    seqNo1: seqNo1,
    msg1: msg1,
    record1: record1,
    eventID2: eventID2,
    seqNo2: seqNo2,
    msg2: msg2,
    record2: record2,
    badRec1: badRec1,
    badRec2: badRec2,
    reason1: reason1,
    reason2: reason2,
    eventID3: eventID3,
    seqNo3: seqNo3,
    msg3: msg3,
    record3: record3
  };
}

function executeTasks(f, batch, t) {
  // Message 1 task 1 (process one task)
  const msg1Task1 = batch.getProcessOneTask(f.msg1, processOne.name);
  const m1p1 = msg1Task1 ? msg1Task1.execute(f.msg1, f.context) : Promise.resolve(undefined);

  if (batch.messages.indexOf(f.msg1) !== -1 && batch.taskDefs.processOneTaskDefs.length > 0) {
    t.ok(msg1Task1, `msg1Task1 must exist`);
  }

  // Message 2 task 1 (process one task) - execute & then mark as rejected
  const msg2Task1 = batch.getProcessOneTask(f.msg2, processOne.name);
  const m2p1 = msg2Task1 ? msg2Task1.execute(f.msg2, f.context) : Promise.resolve(undefined);

  if (batch.messages.indexOf(f.msg2) !== -1 && batch.taskDefs.processOneTaskDefs.length > 0) {
    t.ok(msg2Task1, `msg2Task1 must exist`);
  }

  const m2p1b = m2p1.then(res => {
    msg2Task1.reject('We\'re NOT gonna take it!', new Error('Twisted'));
    return res;
  });

  // Batch task 1 (process all task)
  const batchTask1 = batch.getProcessAllTask(batch, processAll.name);
  const b1p1 = batchTask1 ? batchTask1.execute(batch, f.context) : Promise.resolve(undefined);

  if (batch.messages.length > 0 && batch.taskDefs.processAllTaskDefs.length > 0) {
    t.ok(batchTask1, `batchTask1 must exist`);

    t.notOk(batchTask1.isMasterTask(), `batchTask1 must NOT be a master task`);

    // Message 1 task 2 (process all task)
    const msg1Task2 = batch.getProcessAllTask(f.msg1, processAll.name);
    t.notOk(msg1Task2, `msg1Task2 must NOT exist`);

    // Message 2 task 2 (process all task)
    const msg2Task2 = batch.getProcessAllTask(f.msg2, processAll.name);
    t.notOk(msg2Task2, `msg2Task2 must NOT exist`);
  }

  const d1 = batch.discardUnusableRecords(f.cancellable, f.context);

  const d2 = m2p1b.then(() => batch.discardRejectedMessages(f.cancellable, f.context));

  return Promises.every([m1p1, m2p1, m2p1b, b1p1, d1, d2], f.cancellable, f.context);
}

// =====================================================================================================================
// toBatchStateItem for dynamodb
// =====================================================================================================================

test('toBatchStateItem for dynamodb - 1 message', t => {
  const f = dynamoDBFixture();

  const batch = new Batch([f.record1], [f.processOneTaskDef], [f.processAllTaskDef], f.context);
  batch.addMessage(f.msg1, f.record1, undefined, f.context);

  batch.reviveTasks(f.context);

  // Simulate execution of tasks
  const promise = executeTasks(f, batch, t);

  promise.then(
    () => {
      const item = toBatchStateItem(batch, f.context);
      // console.log(`item = ${JSON.stringify(item)}`);

      t.ok(item, `item must be defined`);
      const consumerId = f.context.streamProcessing.consumerId;
      t.equal(item.streamConsumerId, `D|MyTable_DEV/2016-10-24T18:14:40.531|${consumerId}`, `item streamConsumerId must be 'D|MyTable_DEV/2016-10-24T18:14:40.531|${consumerId}'`);
      t.equal(item.shardOrEventID, `E|${f.eventID1}`, `item shardOrEventID must be 'E|${f.eventID1}'`);

      t.equal(item.messageStates.length, 1, `item messageStates.length must be 1`);
      t.equal(item.unusableRecordStates, undefined, `item unusableRecordStates must be undefined`);
      t.ok(item.batchState, `item batchState must be defined`);
      t.ok(item.batchState.alls.processAll, `item batchState.alls.processAll must be defined`);
      t.deepEqual(item.batchState.alls.processAll.state, cleanCompletedState, `item batchState.alls.processAll.state must be Completed`);

      const msgState1 = item.messageStates[0];
      t.equal(msgState1.eventID, f.eventID1, `msgState1 eventID must be ${f.eventID1}`);
      t.ok(msgState1.ones.processOne, `msgState1 ones.processOne must be defined`);
      t.deepEqual(msgState1.ones.processOne.state, cleanCompletedState, `msgState1 ones.processOne.state must be Completed`);
      t.notOk(msgState1.alls, `msgState1 ones must NOT be defined`);

      t.end();
    },
    err => {
      t.end(err);
    }
  );
});

test('toBatchStateItem for dynamodb - 2 messages', t => {
  const f = dynamoDBFixture();

  // Wait for the messages processOne & processAll tasks to finish executing
  // 2 messages
  const batch = new Batch([f.record2, f.record1], [f.processOneTaskDef], [f.processAllTaskDef], f.context);
  batch.addMessage(f.msg2, f.record2, undefined, f.context);
  batch.addMessage(f.msg1, f.record1, undefined, f.context);

  batch.reviveTasks(f.context);

  // Simulate execution of tasks
  const promise = executeTasks(f, batch, t);

  promise.then(
    () => {
      const item = toBatchStateItem(batch, f.context);
      // console.log(`item = ${JSON.stringify(item)}`);

      t.ok(item, `item must be defined`);
      const consumerId = f.context.streamProcessing.consumerId;
      t.equal(item.streamConsumerId, `D|MyTable_DEV/2016-10-24T18:14:40.531|${consumerId}`, `item streamConsumerId must be 'D|MyTable_DEV/2016-10-24T18:14:40.531|${consumerId}'`);
      t.equal(item.shardOrEventID, `E|${f.eventID2}`, `item shardOrEventID must be 'E|${f.eventID2}'`);

      t.equal(item.messageStates.length, 2, `item messageStates.length must be 2`);
      t.equal(item.unusableRecordStates, undefined, `item unusableRecordStates must be undefined`);
      t.ok(item.batchState, `item batchState must be defined`);
      t.ok(item.batchState.alls.processAll, `item batchState.alls.processAll must be defined`);
      t.deepEqual(item.batchState.alls.processAll.state, cleanCompletedState, `item batchState.alls.processAll.state must be Completed`);

      const msgState1 = item.messageStates[0];
      t.equal(msgState1.eventID, f.eventID2, `msgState1 eventID must be ${f.eventID2}`);
      t.ok(msgState1.ones.processOne, `msgState1 ones.processOne must be defined`);
      t.equal(msgState1.ones.processOne.state.type, StateType.Rejected, `msgState1 ones.processOne.state.type must be rejected`);
      t.notOk(msgState1.alls, `msgState1 ones must NOT be defined`);
      t.ok(msgState1.discards.discardRejectedMessage, `msgState1 discards.discardRejectedMessage must be defined`);
      t.deepEqual(msgState1.discards.discardRejectedMessage.state, cleanCompletedState, `msgState2 discards.discardRejectedMessage.state must be Completed`);

      const msgState2 = item.messageStates[1];
      t.equal(msgState2.eventID, f.eventID1, `msgState2 eventID must be ${f.eventID1}`);
      t.ok(msgState2.ones.processOne, `msgState2 ones.processOne must be defined`);
      t.deepEqual(msgState2.ones.processOne.state, cleanCompletedState, `msgState2 ones.processOne.state must be Completed`);
      t.notOk(msgState2.alls, `msgState2 ones must NOT be defined`);

      t.end();
    },
    err => {
      t.end(err);
    }
  );
});

test('toBatchStateItem for dynamodb - 1 completely unusable record', t => {
  const f = dynamoDBFixture();

  // 1 unusable record
  const batch = new Batch([f.badRec1], [f.processOneTaskDef], [f.processAllTaskDef], f.context);
  f.uRec1 = batch.addUnusableRecord(f.badRec1, undefined, f.reason1, f.context);

  batch.reviveTasks(f.context);

  // Simulate execution of tasks
  const promise = executeTasks(f, batch, t);

  promise.then(
    () => {
      const item = toBatchStateItem(batch, f.context);
      // console.log(`item = ${JSON.stringify(item)}`);

      t.notOk(item, `item must be undefined`);

      t.end();
    },
    err => {
      t.end(err);
    }
  );
});

test('toBatchStateItem for dynamodb - 2 unusable records', t => {
  const f = dynamoDBFixture();

  // 2 unusable records
  const batch = new Batch([f.badRec1, f.badRec2], [f.processOneTaskDef], [f.processAllTaskDef], f.context);
  f.uRec1 = batch.addUnusableRecord(f.badRec1, undefined, f.reason1, f.context);
  f.uRec2 = batch.addUnusableRecord(f.badRec2, undefined, f.reason2, f.context);

  batch.reviveTasks(f.context);

  // Simulate execution of tasks
  const promise = executeTasks(f, batch, t);

  promise.then(
    () => {
      const item = toBatchStateItem(batch, f.context);
      // console.log(`item = ${JSON.stringify(item)}`);

      t.ok(item, `item must be defined`);
      const consumerId = f.context.streamProcessing.consumerId;
      t.equal(item.streamConsumerId, `D|MyTable_DEV/2016-10-24T18:14:40.531|${consumerId}`, `item streamConsumerId must be 'D|MyTable_DEV/2016-10-24T18:14:40.531|${consumerId}'`);
      t.equal(item.shardOrEventID, `E|${f.eventID3}`, `item shardOrEventID must be 'E|${f.eventID3}'`);

      t.equal(item.messageStates, undefined, `item messageStates must be undefined`);
      t.equal(item.unusableRecordStates.length, 2, `item unusableRecordStates.length must be 2`);
      t.ok(item.batchState, `item batchState must be defined`);
      t.deepEqual(item.batchState, {}, `item batchState must be {}`);
      t.notOk(item.batchState.alls, `item batchState.alls must be undefined`);
      t.notOk(item.batchState.ones, `item batchState.ones must be undefined`);
      t.notOk(item.batchState.discards, `item batchState.discards must be undefined`);

      const uRecState1 = item.unusableRecordStates[0];
      t.equal(uRecState1.eventID, undefined, `uRecState1 eventID must be undefined`);
      t.ok(uRecState1.record, `uRecState1 record must NOT be undefined`);
      t.ok(uRecState1.discards.discardUnusableRecord, `uRecState1 discards.discardUnusableRecord must be defined`);
      t.deepEqual(uRecState1.discards.discardUnusableRecord.state, cleanCompletedState, `uRecState1 discards.discardUnusableRecord.state must be Completed`);

      const uRecState2 = item.unusableRecordStates[1];
      t.equal(uRecState2.eventID, f.eventID3, `uRecState2 eventID must be ${f.eventID3}`);
      t.notOk(uRecState2.record, `uRecState2 record must be undefined`);

      t.end();
    },
    err => {
      t.end(err);
    }
  );
});

test('toBatchStateItem for dynamodb - 2 unusable records & 1 ok message', t => {
  const f = dynamoDBFixture();

  // 2 unusable records + 1 ok record
  const batch = new Batch([f.badRec1, f.record1, f.badRec2], [f.processOneTaskDef], [f.processAllTaskDef], f.context);

  f.uRec1 = batch.addUnusableRecord(f.badRec1, undefined, f.reason1, f.context);
  batch.addMessage(f.msg1, f.record1, undefined, f.context);
  f.uRec2 = batch.addUnusableRecord(f.badRec2, undefined, f.reason2, f.context);

  batch.reviveTasks(f.context);

  // Simulate execution of tasks
  const promise = executeTasks(f, batch, t);

  promise.then(
    () => {
      const item = toBatchStateItem(batch, f.context);
      // console.log(`item = ${JSON.stringify(item)}`);

      t.ok(item, `item must be defined`);
      const consumerId = f.context.streamProcessing.consumerId;
      t.equal(item.streamConsumerId, `D|MyTable_DEV/2016-10-24T18:14:40.531|${consumerId}`, `item streamConsumerId must be 'D|MyTable_DEV/2016-10-24T18:14:40.531|${consumerId}'`);
      t.equal(item.shardOrEventID, `E|${f.eventID1}`, `item shardOrEventID must be 'E|${f.eventID1}'`);

      t.equal(item.messageStates.length, 1, `item messageStates.length must be 1`);
      t.equal(item.unusableRecordStates.length, 2, `item unusableRecordStates.length must be 2`);
      t.ok(item.batchState, `item batchState must be defined`);
      t.ok(item.batchState.alls, `item batchState.alls must be defined`);
      t.notOk(item.batchState.ones, `item batchState.ones must be undefined`);
      t.notOk(item.batchState.discards, `item batchState.discards must be undefined`);

      const msgState1 = item.messageStates[0];
      t.equal(msgState1.eventID, f.eventID1, `msgState1 eventID must be ${f.eventID1}`);

      const uRecState1 = item.unusableRecordStates[0];
      t.equal(uRecState1.eventID, undefined, `uRecState1 eventID must be undefined`);
      t.ok(uRecState1.record, `uRecState1 record must NOT be undefined`);

      const uRecState2 = item.unusableRecordStates[1];
      t.equal(uRecState2.eventID, f.eventID3, `uRecState2 eventID must be ${f.eventID3}`);
      t.notOk(uRecState2.record, `uRecState2 record must be undefined`);

      t.end();
    },
    err => {
      t.end(err);
    }
  );
});

// =====================================================================================================================
// saveBatchStateToDynamoDB - failure cases
// =====================================================================================================================

test('saveBatchStateToDynamoDB - 0 messages, 0 unusable records, 0 records', t => {
  const f = dynamoDBFixture();
  const taskFactory = f.context.taskFactory;

  const dynamoDBError = undefined;
  f.context.dynamoDBDocClient = dummyDynamoDBDocClient(t, 'persisting-dynamodb.test.js', dynamoDBError, undefined, 5);

  const task = taskFactory.createTask(TaskDef.defineTask(saveBatchStateToDynamoDB.name, saveBatchStateToDynamoDB));

  const batch = new Batch([], [f.processOneTaskDef], [f.processAllTaskDef], f.context);

  batch.reviveTasks(f.context);

  // Simulate execution of tasks
  const promise = executeTasks(f, batch, t);

  promise.then(
    () => {
      const executeResult = task.execute(batch, f.context);
      executeResult.then(
        result => {
          t.pass(`saveBatchStateToDynamoDB must succeed - result ${stringify(result)}`);
          t.equal(result, undefined, `result must be undefined`);

          t.ok(task.rejected, `Task (${task.name}) must be rejected`);
          t.equal(task.result, undefined, `Task (${task.name}) result must be undefined`);
          t.equal(task.state.name, TaskState.names.Rejected, `Task (${task.name}) state.name must be ${TaskState.names.Rejected}`);
          t.ok(noRecordsMsgRegex.test(task.state.reason), `Task (${task.name}) state.reason must match '${noRecordsMsgRegex}'`);

          t.deepEqual(task.outcome, new Success(executeResult), `Task (${task.name}) outcome must be Success(executeResult)`);
          t.equal(task.outcome.value, executeResult, `Task (${task.name}) outcome.value must be executeResult`);

          t.ok(task.donePromise, `Task (${task.name}) donePromise must not be undefined`);
          t.ok(task.donePromise instanceof Promise, `Task (${task.name}) donePromise must be a Promise`);
          task.donePromise.then(
            doneResult => {
              t.equal(doneResult, undefined, `Task (${task.name}) donePromise result must be undefined`);
              t.equal(doneResult, result, `Task (${task.name}) donePromise result must be result`);

              t.ok(task.rejected, `Task (${task.name}) must still be rejected`);
              t.equal(task.result, result, `Task (${task.name}) result must still be result`);
              t.end();
            },
            err => {
              t.fail(`Task (${task.name}) donePromise must NOT reject`, err);
              t.end();
            }
          );
        },
        err => {
          t.fail(`saveBatchStateToDynamoDB must NOT fail`, err);
          t.end();
        }
      );
    },
    err => {
      t.end(err);
    }
  );
});

test('saveBatchStateToDynamoDB - 0 messages, 0 unusable records, but 1 record', t => {
  const f = dynamoDBFixture();
  const taskFactory = f.context.taskFactory;

  const dynamoDBError = undefined;
  f.context.dynamoDBDocClient = dummyDynamoDBDocClient(t, 'persisting-dynamodb.test.js', dynamoDBError, undefined, 5);

  const task = taskFactory.createTask(TaskDef.defineTask(saveBatchStateToDynamoDB.name, saveBatchStateToDynamoDB));
  const initialAttempts = task.attempts;

  const batch = new Batch([f.record1], [f.processOneTaskDef], [f.processAllTaskDef], f.context);

  batch.reviveTasks(f.context);

  // Simulate execution of tasks
  const promise = executeTasks(f, batch, t);

  promise.then(
    () => {
      const executeResult = task.execute(batch, f.context);
      executeResult.then(
        result => {
          t.fail(`saveBatchStateToDynamoDB must NOT succeed - result ${stringify(result)}`);
          t.end();
        },
        err => {
          t.pass(`saveBatchStateToDynamoDB must fail with an error`);
          t.ok(noMsgsOrUnusableRecordsMsgRegex.test(err.message), `err.message must match '${noMsgsOrUnusableRecordsMsgRegex}'`);

          t.ok(task.failed, `Task (${task.name}) must be failed`);
          t.equal(task.error, err, `Task (${task.name}) error must be rejected err'`);
          t.equal(task.state.name, TaskState.names.LogicFlawed, `Task (${task.name}) state.name must be ${TaskState.names.LogicFlawed}`);
          t.equal(task.attempts, initialAttempts, `Task (${task.name}) attempts must be reset to initial attempts`);
          t.equal(task.result, undefined, `Task (${task.name}) result must be undefined`);

          t.deepEqual(task.outcome, new Success(executeResult), `Task (${task.name}) outcome must be Success(${stringify(executeResult)}`);
          t.equal(task.outcome.value, executeResult, `Task (${task.name}) outcome.value must be executeResult`);

          t.ok(task.donePromise, `Task (${task.name}) donePromise must exist`);
          t.ok(task.donePromise instanceof Promise, `Task (${task.name}) donePromise must be a Promise`);
          task.donePromise.then(
            doneResult => {
              t.fail(`Task (${task.name}) donePromise must NOT resolve with:`, doneResult);
              t.end();
            },
            doneErr => {
              t.deepEqual(doneErr, err, `Task (${task.name}) donePromise error must be ${stringify(err)}`);
              t.equal(doneErr, err, `Task (${task.name}) donePromise error must be rejected err`);

              t.ok(task.failed, `Task (${task.name}) must still be failed`);
              t.equal(task.error, err, `Task (${task.name}) error must still be rejected err`);

              t.end();
            }
          );
        }
      );
    },
    err => {
      t.end(err);
    }
  );
});

test('saveBatchStateToDynamoDB for dynamodb - 0 messages, 1 totally unusable record, 1 record (with max attempts 2)', t => {
  const f = dynamoDBFixture();
  const taskFactory = f.context.taskFactory;

  const dynamoDBError = undefined;
  f.context.dynamoDBDocClient = dummyDynamoDBDocClient(t, 'persisting-dynamodb.test.js', dynamoDBError, undefined, 5);
  f.context.streamProcessing.maxNumberOfAttempts = 2;

  const task = taskFactory.createTask(TaskDef.defineTask(saveBatchStateToDynamoDB.name, saveBatchStateToDynamoDB));

  const batch = new Batch([f.badRec1], [f.processOneTaskDef], [f.processAllTaskDef], f.context);

  f.uRec1 = batch.addUnusableRecord(f.badRec1, undefined, f.reason1, f.context);

  batch.reviveTasks(f.context);

  // Simulate execution of tasks
  const promise = executeTasks(f, batch, t);

  promise.then(
    () => {
      const executeResult = task.execute(batch, f.context);
      executeResult.then(
        result => {
          t.fail(`saveBatchStateToDynamoDB must NOT succeed - result ${stringify(result)}`);
          t.end();
        },
        err => {
          t.pass(`saveBatchStateToDynamoDB must fail with an error`);
          t.ok(err.message.startsWith('Cannot save Batch'), `err.message must start with 'Cannot save Batch'`);

          t.ok(task.failed, `Task (${task.name}) must be failed`);
          t.equal(task.result, undefined, `Task (${task.name}) result must be undefined`);
          t.ok(task.error, `Task (${task.name}) error must be defined`);
          t.equal(task.error, err, `Task (${task.name}) error.message must be err`);

          t.deepEqual(task.outcome, new Success(executeResult), `Task (${task.name}) outcome must be Success(executeResult)`);
          t.equal(task.outcome.value, executeResult, `Task (${task.name}) outcome.value must be executeResult`);

          t.ok(task.donePromise, `Task (${task.name}) donePromise must be defined`);
          t.ok(task.donePromise instanceof Promise, `Task (${task.name}) donePromise must be a Promise`);
          task.donePromise.then(
            doneResult => {
              t.fail(`Task (${task.name}) donePromise must NOT resolve with:`, doneResult);
              t.end();
            },
            doneErr => {
              t.deepEqual(doneErr, err, `Task (${task.name}) donePromise error must be ${stringify(err)}`);
              t.equal(doneErr, err, `Task (${task.name}) donePromise error must be rejected err`);

              t.ok(task.failed, `Task (${task.name}) must still be failed`);
              t.equal(task.error, err, `Task (${task.name}) error must still be err`);

              t.end();
            }
          );
        }
      );
    },
    err => {
      t.end(err);
    }
  );
});

test('saveBatchStateToDynamoDB for dynamodb - 0 messages, 1 totally unusable record, 1 record (with max attempts 1)', t => {
  const f = dynamoDBFixture();
  const taskFactory = f.context.taskFactory;

  const dynamoDBError = undefined;
  f.context.dynamoDBDocClient = dummyDynamoDBDocClient(t, 'persisting-dynamodb.test.js', dynamoDBError, undefined, 5);
  f.context.streamProcessing.maxNumberOfAttempts = 1;

  const task = taskFactory.createTask(TaskDef.defineTask(saveBatchStateToDynamoDB.name, saveBatchStateToDynamoDB));

  const batch = new Batch([f.badRec1], [f.processOneTaskDef], [f.processAllTaskDef], f.context);

  f.uRec1 = batch.addUnusableRecord(f.badRec1, undefined, f.reason1, f.context);

  batch.reviveTasks(f.context);

  // Simulate execution of tasks
  const promise = executeTasks(f, batch, t);

  promise.then(
    () => {
      const executeResult = task.execute(batch, f.context);
      executeResult.then(
        result => {
          t.pass(`saveBatchStateToDynamoDB must succeed - result ${stringify(result)}`);
          t.equal(result, undefined, `result must be undefined`);

          t.ok(task.rejected, `Task (${task.name}) must be rejected`);
          t.equal(task.result, undefined, `Task (${task.name}) result must be undefined`);
          t.ok(task.state.reason, `Task (${task.name}) state.reason must be defined`);
          t.ok(task.state.reason.startsWith('Cannot save Batch'), `Task (${task.name}) state.reason must start with 'Cannot save Batch'`);
          t.equal(task.error, undefined, `Task (${task.name}) error must be undefined`);

          t.deepEqual(task.outcome, new Success(executeResult), `Task (${task.name}) outcome must be Success(executeResult)`);
          t.equal(task.outcome.value, executeResult, `Task (${task.name}) outcome.value must be executeResult`);

          t.ok(task.donePromise, `Task (${task.name}) donePromise must be defined`);
          t.ok(task.donePromise instanceof Promise, `Task (${task.name}) donePromise must be a Promise`);
          task.donePromise.then(
            doneResult => {
              t.equal(doneResult, undefined, `Task (${task.name}) donePromise result must be undefined`);
              t.equal(doneResult, result, `Task (${task.name}) donePromise result must be result`);

              t.ok(task.rejected, `Task (${task.name}) must still be rejected`);
              t.ok(task.state.reason.startsWith('Cannot save Batch'), `Task (${task.name}) state.reason must still start with 'Cannot save Batch'`);
              t.equal(task.error, undefined, `Task (${task.name}) error must still be undefined`);

              t.end();
            },
            err => {
              t.fail(`Task (${task.name}) donePromise must NOT reject`, err);
              t.end();
            }
          );
        }
      );
    },
    err => {
      t.end(err);
    }
  );
});

// =====================================================================================================================
// saveBatchStateToDynamoDB - dynamodb
// =====================================================================================================================

test('saveBatchStateToDynamoDB for dynamodb - 1 message & 1 totally unusable record', t => {
  const f = dynamoDBFixture();
  const taskFactory = f.context.taskFactory;

  const dynamoDBError = undefined;
  f.context.dynamoDBDocClient = dummyDynamoDBDocClient(t, 'persisting-dynamodb.test.js', dynamoDBError, undefined, 5);

  const task = taskFactory.createTask(TaskDef.defineTask(saveBatchStateToDynamoDB.name, saveBatchStateToDynamoDB));

  const batch = new Batch([f.badRec1, f.record1], [f.processOneTaskDef], [f.processAllTaskDef], f.context);

  f.uRec1 = batch.addUnusableRecord(f.badRec1, undefined, f.reason1, f.context);
  batch.addMessage(f.msg1, f.record1, undefined, f.context);

  batch.reviveTasks(f.context);

  // Simulate execution of tasks
  const promise = executeTasks(f, batch, t);

  promise.then(
    () => {
      const executeResult = task.execute(batch, f.context);
      executeResult.then(
        result => {
          t.pass(`saveBatchStateToDynamoDB must succeed - result ${stringify(result)}`);
          t.ok(task.completed, `Task (${task.name}) must be completed`);
          t.equal(task.result, result, `Task (${task.name}) result must be result`);
          t.equal(task.error, undefined, `Task (${task.name}) error must be undefined`);

          t.deepEqual(task.outcome, new Success(executeResult), `Task (${task.name}) outcome must be Success(executeResult)`);
          t.equal(task.outcome.value, executeResult, `Task (${task.name}) outcome.value must be executeResult`);

          t.ok(task.donePromise, `Task (${task.name}) donePromise must not be undefined`);
          t.ok(task.donePromise instanceof Promise, `Task (${task.name}) donePromise must be a Promise`);
          task.donePromise.then(
            doneResult => {
              t.equal(doneResult, result, `Task (${task.name}) donePromise result must be result`);

              t.ok(task.completed, `Task (${task.name}) must still be completed`);
              t.equal(task.result, result, `Task (${task.name}) result must still be result`);
              t.end();
            },
            err => {
              t.fail(`Task (${task.name}) donePromise must NOT reject`, err);
              t.end();
            }
          );
        },
        err => {
          t.fail(`saveBatchStateToDynamoDB must NOT fail`, err);
          t.end();
        }
      );
    },
    err => {
      t.end(err);
    }
  );
});

test('saveBatchStateToDynamoDB for dynamodb - 1 message with DynamoDB error', t => {
  const f = dynamoDBFixture();
  const taskFactory = f.context.taskFactory;

  const dynamoDBError = new Error('Planned DynamoDB error');
  f.context.dynamoDBDocClient = dummyDynamoDBDocClient(t, 'persisting-dynamodb.test.js', dynamoDBError, undefined, 5);

  const task = taskFactory.createTask(TaskDef.defineTask(saveBatchStateToDynamoDB.name, saveBatchStateToDynamoDB));

  const batch = new Batch([f.record1], [f.processOneTaskDef], [f.processAllTaskDef], f.context);

  batch.addMessage(f.msg1, f.record1, undefined, f.context);

  batch.reviveTasks(f.context);

  // Simulate execution of tasks
  const promise = executeTasks(f, batch, t);

  promise.then(
    () => {
      const executeResult = task.execute(batch, f.context);
      executeResult.then(
        result => {
          t.fail(`saveBatchStateToDynamoDB must NOT succeed - result ${stringify(result)}`);
          t.end();
        },
        err => {
          t.pass(`saveBatchStateToDynamoDB must fail`);
          t.equal(err, dynamoDBError, `Error ${err} must be dynamoDBError`);
          t.ok(task.failed, `Task (${task.name}) must be failed`);
          t.equal(task.result, undefined, `Task (${task.name}) result must be undefined`);
          t.equal(task.error, dynamoDBError, `Task (${task.name}) error must be dynamoDBError`);

          t.deepEqual(task.outcome, new Success(executeResult), `Task (${task.name}) outcome must be Success(executeResult)`);
          t.equal(task.outcome.value, executeResult, `Task (${task.name}) outcome.value must be executeResult`);

          t.ok(task.donePromise, `Task (${task.name}) donePromise must not be undefined`);
          t.ok(task.donePromise instanceof Promise, `Task (${task.name}) donePromise must be a Promise`);
          task.donePromise.then(
            doneResult => {
              t.fail(`Task (${task.name}) donePromise must NOT resolve with:`, doneResult);
              t.end();
            },
            doneErr => {
              t.equal(doneErr, dynamoDBError, `Task (${task.name}) donePromise error must be dynamoDBError`);

              t.ok(task.failed, `Task (${task.name}) must still be failed`);
              t.equal(task.result, undefined, `Task (${task.name}) result must still be undefined`);
              t.equal(task.error, dynamoDBError, `Task (${task.name}) error must still be dynamoDBError`);
              t.end();
            }
          );
        }
      );
    },
    err => {
      t.end(err);
    }
  );
});

// =====================================================================================================================
// loadBatchStateFromDynamoDB - dynamodb
// =====================================================================================================================

test('loadBatchStateFromDynamoDB for dynamodb - 2 messages & 2 unusable records', t => {
  const f = dynamoDBFixture();
  const taskFactory = f.context.taskFactory;

  const data = require('./persisting-dynamodb.test.json');
  f.context.dynamoDBDocClient = dummyDynamoDBDocClient(t, 'persisting-dynamodb.test.js', undefined, data, 5);

  const task = taskFactory.createTask(TaskDef.defineTask(loadBatchStateFromDynamoDB.name, loadBatchStateFromDynamoDB));

  const batch = new Batch([f.badRec1, f.badRec2, f.record1, f.record2], [f.processOneTaskDef], [f.processAllTaskDef], f.context);

  f.uRec1 = batch.addUnusableRecord(f.badRec1, undefined, f.reason1, f.context);
  f.uRec2 = batch.addUnusableRecord(f.badRec2, undefined, f.reason2, f.context);
  batch.addMessage(f.msg1, f.record1, undefined, f.context);
  batch.addMessage(f.msg2, f.record2, undefined, f.context);

  batch.reviveTasks(f.context);

  // Simulate execution of tasks
  const promise = Promise.resolve(undefined); // executeTasks(f, batch, t);

  promise.then(
    () => {
      const executeResult = task.execute(batch, f.context);
      executeResult.then(
        result => {
          t.pass(`loadBatchStateFromDynamoDB must succeed - result ${stringify(result)}`);
          t.ok(task.completed, `Task (${task.name}) must be completed`);
          t.equal(task.result, result, `Task (${task.name}) result must be result`);
          t.equal(task.error, undefined, `Task (${task.name}) error must be undefined`);

          t.deepEqual(task.outcome, new Success(executeResult), `Task (${task.name}) outcome must be Success(executeResult)`);
          t.equal(task.outcome.value, executeResult, `Task (${task.name}) outcome.value must be executeResult`);

          t.ok(task.donePromise, `Task (${task.name}) donePromise must not be undefined`);
          t.ok(task.donePromise instanceof Promise, `Task (${task.name}) donePromise must be a Promise`);
          task.donePromise.then(
            doneResult => {
              t.equal(doneResult, data, `Task (${task.name}) donePromise result must be data`);
              t.equal(doneResult, result, `Task (${task.name}) donePromise result must be executeResult result`);

              t.ok(task.completed, `Task (${task.name}) must still be completed`);
              t.equal(task.result, result, `Task (${task.name}) result must still be result`);
              t.end();
            },
            err => {
              t.fail(`Task (${task.name}) donePromise must NOT reject`, err);
              t.end();
            }
          );
        },
        err => {
          t.fail(`loadBatchStateFromDynamoDB must NOT fail`, err);
          t.end();
        }
      );
    },
    err => {
      t.end(err);
    }
  );
});

// =====================================================================================================================
// loadBatchStateFromDynamoDB - dynamodb - with simulated DynamoDB error
// =====================================================================================================================

test('loadBatchStateFromDynamoDB for dynamodb - 2 messages & 2 unusable records with DynamoDB error', t => {
  const f = dynamoDBFixture();
  const taskFactory = f.context.taskFactory;

  const dynamoDBError = new Error('Planned DynamoDB error');
  f.context.dynamoDBDocClient = dummyDynamoDBDocClient(t, 'persisting-dynamodb.test.js', dynamoDBError, undefined, 5);

  const task = taskFactory.createTask(TaskDef.defineTask(loadBatchStateFromDynamoDB.name, loadBatchStateFromDynamoDB));

  const batch = new Batch([f.badRec1, f.badRec2, f.record1, f.record2], [f.processOneTaskDef], [f.processAllTaskDef], f.context);

  f.uRec1 = batch.addUnusableRecord(f.badRec1, undefined, f.reason1, f.context);
  f.uRec2 = batch.addUnusableRecord(f.badRec2, undefined, f.reason2, f.context);
  batch.addMessage(f.msg1, f.record1, undefined, f.context);
  batch.addMessage(f.msg2, f.record2, undefined, f.context);

  batch.reviveTasks(f.context);

  // Simulate execution of tasks
  const promise = Promise.resolve(undefined); // executeTasks(f, batch, t);

  promise.then(
    () => {
      const executeResult = task.execute(batch, f.context);
      executeResult.then(
        result => {
          t.fail(`loadBatchStateFromDynamoDB must NOT succeed - result ${stringify(result)}`);
          t.end();
        },
        err => {
          t.pass(`loadBatchStateFromDynamoDB must fail`);
          t.equal(err, dynamoDBError, `Error ${err} must be dynamoDBError`);
          t.ok(task.failed, `Task (${task.name}) must be failed`);
          t.equal(task.result, undefined, `Task (${task.name}) result must be undefined`);
          t.equal(task.error, dynamoDBError, `Task (${task.name}) error must be dynamoDBError`);

          t.deepEqual(task.outcome, new Success(executeResult), `Task (${task.name}) outcome must be Success(executeResult)`);
          t.equal(task.outcome.value, executeResult, `Task (${task.name}) outcome.value must be executeResult`);

          t.ok(task.donePromise, `Task (${task.name}) donePromise must not be undefined`);
          t.ok(task.donePromise instanceof Promise, `Task (${task.name}) donePromise must be a Promise`);
          task.donePromise.then(
            doneResult => {
              t.fail(`Task (${task.name}) donePromise must NOT resolve with:`, doneResult);
              t.end();
            },
            doneErr => {
              t.equal(doneErr, dynamoDBError, `Task (${task.name}) donePromise error must be dynamoDBError`);

              t.ok(task.failed, `Task (${task.name}) must still be failed`);
              t.equal(task.result, undefined, `Task (${task.name}) result must still be undefined`);
              t.equal(task.error, dynamoDBError, `Task (${task.name}) error must still be dynamoDBError`);

              t.end();
            }
          );
        }
      );
    },
    err => {
      t.end(err);
    }
  );
});
