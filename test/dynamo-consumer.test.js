'use strict';

/**
 * Unit tests for aws-stream-consumer/stream-consumer.js
 * @author Byron du Preez
 */

const test = require('tape');

// Leave integrationTestingMode committed as false - ONLY TEMPORARILY set it to true to run in integration testing mode
// instead of normal unit testing mode
const integrationTestingMode = false;
console.log(`***** RUNNING IN ${integrationTestingMode ? 'INTEGRATION' : 'UNIT'} TESTING MODE *****`);

// The test subject
const consumer = require('../dynamo-consumer');
const prefix = 'dynamo-consumer';
// const streamConsumer = require('aws-stream-consumer/stream-consumer');

const dynamoProcessing = require('../dynamo-processing');

const tracking = require('aws-stream-consumer/tracking');
const toCountString = tracking.toCountString;

const groupBy = require('lodash.groupby');

// const Batch = require('aws-stream-consumer/batch');

// const Settings = require('aws-stream-consumer/settings');
// const StreamType = Settings.StreamType;

const taskUtils = require('task-utils');
const TaskDef = require('task-utils/task-defs');
const Task = require('task-utils/tasks');
// const TaskFactory = require('task-utils/task-factory');
const taskStates = require('task-utils/task-states');
const TaskState = taskStates.TaskState;
const core = require('task-utils/core');
const StateType = core.StateType;

const stages = require('aws-core-utils/stages');
const kinesisCache = require('aws-core-utils/kinesis-cache');
const dynamoDBDocClientCache = require('aws-core-utils/dynamodb-doc-client-cache');

const Promises = require('core-functions/promises');

const strings = require('core-functions/strings');
const stringify = strings.stringify;

const Numbers = require('core-functions/numbers');

const copying = require('core-functions/copying');
const copy = copying.copy;
const merging = require('core-functions/merging');
const merge = merging.merge;

const Arrays = require('core-functions/arrays');

const sorting = require('core-functions/sorting');

const logging = require('logging-utils');

const samples = require('./samples');
const sampleDynamoDBEventSourceArn = samples.sampleDynamoDBEventSourceArn;
const sampleDynamoDBRecord = samples.sampleDynamoDBRecord;
const sampleDynamoDBEventWithRecords = samples.sampleDynamoDBEventWithRecords;

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
  const region = process.env.AWS_REGION;
  kinesisCache.deleteKinesis(region);
  dynamoDBDocClientCache.deleteDynamoDBDocClient(region);
}

function sampleAwsContext(functionVersion, functionAlias, maxTimeInMillis) {
  const region = process.env.AWS_REGION;
  const functionName = 'sampleFunctionName';
  const invokedFunctionArn = samples.sampleInvokedFunctionArn(region, functionName, functionAlias);
  return samples.sampleAwsContext(functionName, functionVersion, invokedFunctionArn, maxTimeInMillis);
}

// Simulate ideal conditions - everything meant to be configured beforehand has been configured
function createContext(idPropertyNames, keyPropertyNames, seqNoPropertyNames, event, awsContext) {
  const defaultOptions = dynamoProcessing.loadDynamoDBDefaultOptions();
  const options = {
    loggingOptions: {
      logLevel: logging.LogLevel.TRACE
    },
    streamProcessingOptions: {
      consumerIdSuffix: undefined,
      maxNumberOfAttempts: 1,
      idPropertyNames: idPropertyNames ? idPropertyNames : ['id1', 'id2'],
      keyPropertyNames: keyPropertyNames ? keyPropertyNames : [],
      seqNoPropertyNames: seqNoPropertyNames ? seqNoPropertyNames : [],
      batchStateTableName: 'TEST_StreamConsumerBatchState',
      deadRecordQueueName: 'TEST_DRQ',
      deadMessageQueueName: 'TEST_DMQ'
    }
  };
  if (integrationTestingMode) {
    options.kinesisOptions = {maxRetries: 0};
    options.dynamoDBDocClientOptions = {maxRetries: 0};
  }

  merge(defaultOptions, options, {deep: true, replace: false});

  if (!integrationTestingMode) {
    options.kinesisOptions = undefined;
    options.dynamoDBDocClientOptions = undefined;
  }

  return consumer.configureStreamConsumer({}, undefined, options, event, awsContext);
}

function configureKinesisAndDynamoDBDocClient(t, context, prefix, kinesisError, loadError, loadData, saveError, dynamoDelay) {
  if (!integrationTestingMode) {
    context.kinesis = dummyKinesis(t, prefix, kinesisError);
    context.dynamoDBDocClient = dummyDynamoDBDocClient(t, prefix, loadError, loadData, saveError, dynamoDelay);
  }
}

function dummyKinesis(t, prefix, error) {
  return {
    putRecord(request) {
      return {
        promise() {
          return new Promise((resolve, reject) => {
            t.pass(`${prefix} simulated putRecord to Kinesis with request (${stringify(request)})`);
            if (error)
              reject(error);
            else
              resolve({});
          })
        }
      }
    }
  };
}

function dummyDynamoDBDocClient(t, prefix, getError, getData, putError, delayMs) {
  const ms = delayMs ? delayMs : 1;
  return {
    put(request) {
      return {
        promise() {
          return Promises.delay(ms).then(() => {
            return new Promise((resolve, reject) => {
              t.pass(`${prefix} simulated put to DynamoDB.DocumentClient with request (${stringify(request)})`);
              if (putError)
                reject(putError);
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
              if (getError)
                reject(getError);
              else
                resolve(getData);
            });
          });
        }
      };
    }

  };
}

function execute1() {
  console.log(`*** Executing execute1 on task (${this.name})`);
}
function execute2() {
  console.log(`*** Executing execute2 on task (${this.name})`);
}
function execute3() {
  console.log(`*** Executing execute3 on task (${this.name})`);
}
function execute4() {
  console.log(`*** Executing execute4 on task (${this.name})`);
}

function sampleExecuteOneAsync(ms, mustRejectWithError, callback) {
  function executeOneAsync(message, context) {
    const state = context.batch.states.get(message);
    const messageId = state.id ? state.id : state.eventID;
    console.log(`*** ${executeOneAsync.name} started processing message (${messageId})`);
    return Promises.delay(ms)
      .then(
        () => {
          if (typeof callback === 'function') {
            callback(message, context);
          }
          if (!mustRejectWithError) {
            console.log(`*** ${executeOneAsync.name} completed message (${messageId})`);
            return message;
          } else {
            console.error(`*** ${executeOneAsync.name} failed intentionally on message (${messageId}) with error (${mustRejectWithError})`,
              mustRejectWithError);
            throw mustRejectWithError;
          }
        },
        err => {
          console.error(`*** ${executeOneAsync.name} hit UNEXPECTED error on message (${messageId})`, err);
          if (typeof callback === 'function') {
            callback(message, context);
          }
          if (!mustRejectWithError) {
            console.log(`*** ${executeOneAsync.name} "completed" message (${messageId})`);
            return message;
          } else {
            console.error(`*** ${executeOneAsync.name} "failed" intentionally on message (${messageId}) with error (${mustRejectWithError})`,
              mustRejectWithError);
            throw mustRejectWithError;
          }
        }
      );
  }

  return executeOneAsync;
}

function sampleExecuteAllAsync(ms, mustRejectWithError, callback) {

  function executeAllAsync(batch, incompleteMessages, context) {
    const m = batch.messages.length;
    const ms = toCountString(m, 'message');
    const i = incompleteMessages ? incompleteMessages.length : 0;
    const is = toCountString(m, 'incomplete message');
    const isOfMs = `${is} of ${ms}`;

    console.log(`*** ${executeAllAsync.name} started processing ${isOfMs}`);
    return Promises.delay(ms)
      .then(
        () => {
          if (typeof callback === 'function') {
            callback(incompleteMessages, context);
          }
          if (!mustRejectWithError) {
            console.log(`*** ${executeAllAsync.name} completed ${isOfMs}`);
            return incompleteMessages;
          } else {
            console.error(`*** ${executeAllAsync.name} failed intentionally on ${isOfMs} with error (${mustRejectWithError})`,
              mustRejectWithError);
            throw mustRejectWithError;
          }
        },
        err => {
          console.error(`*** ${executeAllAsync.name} hit UNEXPECTED error`, err);
          if (typeof callback === 'function') {
            callback(message, context);
          }
          if (!mustRejectWithError) {
            console.log(`*** ${executeAllAsync.name} "completed" ${isOfMs} messages`);
            return incompleteMessages;
          } else {
            console.error(`*** ${executeAllAsync.name} "failed" intentionally on ${isOfMs} messages with error (${mustRejectWithError})`,
              mustRejectWithError);
            throw mustRejectWithError;
          }
        }
      );
  }

  return executeAllAsync;
}

// =====================================================================================================================
// processStreamEvent
// =====================================================================================================================

function checkMessagesTasksStates(t, batch, oneStateType, allStateType, context) {
  const messages = batch.messages;
  for (let i = 0; i < messages.length; ++i) {
    checkMessageTasksStates(t, messages[i], batch, oneStateType, allStateType)
  }
  if (allStateType) {
    const processAllTasksByName = batch.getProcessAllTasks();
    const processAllTasks = taskUtils.getTasksAndSubTasks(processAllTasksByName);
    t.ok(processAllTasks.every(t => t.state instanceof allStateType), `batch (${batch.shardOrEventID}) every process all task state must be instance of ${allStateType.name}`);
  }
}

function checkMessageTasksStates(t, message, batch, oneStateType, allStateType) {
  const states = batch.states;
  const state = states.get(message);
  const messageId = state.id;
  if (oneStateType) {
    const processOneTasksByName = batch.getProcessOneTasks(message);
    const processOneTasks = taskUtils.getTasksAndSubTasks(processOneTasksByName);
    t.ok(processOneTasks.every(t => t.state instanceof oneStateType), `message (${messageId}) every process one task state must be instance of ${oneStateType.name}`);
  }
  if (allStateType) {
    const processAllTasksByName = batch.getProcessAllTasks(message);
    const processAllTasks = taskUtils.getTasksAndSubTasks(processAllTasksByName);
    t.ok(processAllTasks.every(t => t.state instanceof allStateType), `message (${messageId}) every process all task state must be instance of ${allStateType.name}`);
  }
}

function checkUnusableRecordsTasksStates(t, batch, discardStateType) {
  const records = batch.unusableRecords;
  for (let i = 0; i < records.length; ++i) {
    checkUnusableRecordTasksStates(t, records[i], batch, discardStateType)
  }
}

function checkUnusableRecordTasksStates(t, record, batch, discardStateType) {
  if (discardStateType) {
    const discardOneTasksByName = batch.getDiscardOneTasks(record);
    const discardOneTasks = taskUtils.getTasksAndSubTasks(discardOneTasksByName);
    t.ok(discardOneTasks.every(t => t.state instanceof discardStateType), `unusable record (${record.eventID}) every discard one task state must be instance of ${discardStateType.name}`);
  }
}

function pad(number, digits) {
  return Numbers.zeroPadLeft(`${number}`, digits);
}

function generateEvent(s, n, region, opts) {
  const b = opts && opts.badApples && !Number.isNaN(Number(opts.badApples)) ? Number(opts.badApples) : 0;
  const u = opts && opts.unusables && !Number.isNaN(Number(opts.unusables)) ? Number(opts.unusables) : 0;
  const k = opts && opts.uniqueKeys && !Number.isNaN(Number(opts.uniqueKeys)) ? Math.max(Math.min(Number(opts.uniqueKeys), n), 1) : Math.max(n, 1);
  const records = new Array(n + u + b);

  const eventSourceARN = sampleDynamoDBEventSourceArn(region, 'TEST_Table_DEV', '2017-03-21T13:54:59');
  const ss = pad(s, 2);
  for (let i = 0; i < n; ++i) {
    const ii = pad(i, 2);
    const eventID = `E0${ss}-${ii}`;
    const eventSeqNo = `100000000000000${ii}`;
    records[i] = sampleDynamoDBRecord(eventID, eventSeqNo, eventSourceARN, `ID-${ss}`, `70${ii}`, 'ABC', 10 + (i % k),
      1, 100, '10000000000000000000001', `2017-01-17T23:59:59.0${ii}Z`);
  }

  for (let i = n; i < n + u; ++i) {
    const ii = pad(i, 2);
    const eventID = `U0${ss}-${ii}`;
    const eventSeqNo = `100000000000000${ii}`;
    records[i] = sampleDynamoDBRecord(eventID, eventSeqNo, eventSourceARN, `ID-${ss}`, `70${ii}`, 'ABC', 10 + (i % k),
      1, 100, '10000000000000000000001', `2017-01-17T23:59:59.0${ii}Z`);
    delete records[i].dynamodb.Keys; // make the record unusable
  }

  for (let i = n + u; i < n + u + b; ++i) {
    const ii = pad(i, 2);
    records[i] = {bad: `apple-${ss}-${ii}`};
  }

  records.reverse(); // Start them in the worst sequence possible

  return sampleDynamoDBEventWithRecords(records);
}

// =====================================================================================================================
// processStreamEvent with successful message(s)
// =====================================================================================================================

test('processStreamEvent with 1 message that succeeds all tasks', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const n = 1;

    // Generate a sample AWS event
    const event = generateEvent(1, n, region, {unusables: 0, uniqueKeys: n});

    // Generate a sample AWS context
    const maxTimeInMillis = 120000;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Setup the task definitions
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, undefined));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      const context = createContext(undefined, undefined, undefined, event, awsContext);
      configureKinesisAndDynamoDBDocClient(t, context, prefix, undefined, undefined, {}, undefined, 5);

      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(batch => {
          t.pass(`processStreamEvent must resolve`);
          const states = batch.states;
          const messages = batch.messages;
          t.equal(messages.length, n, `processStreamEvent batch must have ${n} messages`);
          checkMessagesTasksStates(t, batch, taskStates.CompletedState, taskStates.CompletedState, context);
          messages.forEach((msg, i) => {
            const msgState = states.get(msg);
            t.equal(msgState.ones.Task1.attempts, 1, `message [${i}] (${msgState.id}) Task1 attempts must be 1`);
            t.equal(msgState.alls.Task2.attempts, 1, `message [${i}] (${msgState.id}) Task2 attempts must be 1`);
          });

          const batchState = states.get(batch);
          t.equal(batchState.alls.Task2.attempts, 1, `Task2 attempts must be 1`);

          t.ok(batchState.initiating.initiateBatch.isFullyFinalised(), `processStreamEvent initiating must be fully finalised`);
          t.ok(batchState.processing.processBatch.isFullyFinalised(), `processStreamEvent processing must be fully finalised`);
          t.ok(batchState.finalising.finaliseBatch.isFullyFinalised(), `processStreamEvent finalising must be fully finalised`);

          t.equal(batch.unusableRecords.length, 0, `processStreamEvent batch must have ${0} unusable records`);
          t.equal(batch.rejectedMessages.length, 0, `processStreamEvent batch must have ${0} rejected messages`);
          t.equal(batch.undiscardedRejectedMessages.length, 0, `processStreamEvent batch must have ${0} undiscarded rejected messages`);

          t.end();
        })
        .catch(err => {
          t.fail(`processStreamEvent should NOT have failed (${JSON.stringify(err)})`, err);
          t.end(err);
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${JSON.stringify(err)})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('processStreamEvent with 1 message that succeeds all tasks (despite broken Kinesis, i.e. no unusable/rejected/incomplete)', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const n = 1;

    // Generate a sample AWS event
    const event = generateEvent(2, n, region, {unusables: 0, uniqueKeys: n});

    // Generate a sample AWS context
    const maxTimeInMillis = 60000;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Setup the task definitions
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, undefined));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      const context = createContext(undefined, undefined, undefined, event, awsContext);
      configureKinesisAndDynamoDBDocClient(t, context, prefix, new Error('Disabling Kinesis'), undefined, {}, undefined, 5);

      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(batch => {
          t.pass(`processStreamEvent must resolve`);
          const states = batch.states;
          const messages = batch.messages;
          t.equal(messages.length, n, `processStreamEvent batch must have ${n} messages`);
          checkMessagesTasksStates(t, batch, taskStates.CompletedState, taskStates.CompletedState, context);
          t.equal(states.get(messages[0]).ones.Task1.attempts, 1, `Task1 attempts must be 1`);

          messages.forEach((msg, i) => {
            const msgState = states.get(msg);
            t.equal(msgState.ones.Task1.attempts, 1, `message [${i}] (${msgState.id}) Task1 attempts must be 1`);
            t.equal(msgState.alls.Task2.attempts, 1, `message [${i}] (${msgState.id}) Task2 attempts must be 1`);
          });

          const batchState = states.get(batch);
          t.equal(batchState.alls.Task2.attempts, 1, `Task2 attempts must be 1`);

          t.ok(batchState.initiating.initiateBatch.isFullyFinalised(), `processStreamEvent initiating must be fully finalised`);
          t.ok(batchState.processing.processBatch.isFullyFinalised(), `processStreamEvent processing must be fully finalised`);
          t.ok(batchState.finalising.finaliseBatch.isFullyFinalised(), `processStreamEvent finalising must be fully finalised`);

          t.equal(batch.unusableRecords.length, 0, `processStreamEvent batch must have ${0} unusable records`);
          t.equal(batch.rejectedMessages.length, 0, `processStreamEvent batch must have ${0} rejected messages`);
          t.equal(batch.undiscardedRejectedMessages.length, 0, `processStreamEvent batch must have ${0} undiscarded rejected messages`);

          t.end();
        })
        .catch(err => {
          t.fail(`processStreamEvent should NOT have failed (${JSON.stringify(err)})`, err);
          t.end(err);
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${JSON.stringify(err)})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('processStreamEvent with 10 messages with 10 unique keys that succeed all tasks (despite broken Kinesis, i.e. no unusable/rejected/incomplete)', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const n = 10;

    // Generate a sample AWS event
    const event = generateEvent(3, n, region, {unusables: 0, uniqueKeys: n});

    // Generate a sample AWS context
    const maxTimeInMillis = 120000;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Setup the task definitions
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, undefined));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      const context = createContext(undefined, undefined, undefined, event, awsContext);
      configureKinesisAndDynamoDBDocClient(t, context, prefix, new Error('Disabling Kinesis'), undefined, {}, undefined, 5);

      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(batch => {
          t.pass(`processStreamEvent must resolve`);
          const states = batch.states;
          const messages = batch.messages;
          t.equal(messages.length, n, `processStreamEvent batch must have ${n} messages`);
          checkMessagesTasksStates(t, batch, taskStates.CompletedState, taskStates.CompletedState, context);

          messages.forEach((msg, i) => {
            const msgState = states.get(msg);
            t.equal(msgState.ones.Task1.attempts, 1, `message [${i}] (${msgState.id}) Task1 attempts must be 1`);
            t.equal(msgState.alls.Task2.attempts, 1, `message [${i}] (${msgState.id}) Task2 attempts must be 1`);
          });

          const batchState = states.get(batch);
          t.equal(batchState.alls.Task2.attempts, 1, `Task2 attempts must be 1`);

          t.ok(batchState.initiating.initiateBatch.isFullyFinalised(), `processStreamEvent initiating must be fully finalised`);
          t.ok(batchState.processing.processBatch.isFullyFinalised(), `processStreamEvent processing must be fully finalised`);
          t.ok(batchState.finalising.finaliseBatch.isFullyFinalised(), `processStreamEvent finalising must be fully finalised`);

          t.equal(batch.unusableRecords.length, 0, `processStreamEvent batch must have ${0} unusable records`);
          t.equal(batch.rejectedMessages.length, 0, `processStreamEvent batch must have ${0} rejected messages`);
          t.equal(batch.undiscardedRejectedMessages.length, 0, `processStreamEvent batch must have ${0} undiscarded rejected messages`);

          t.end();
        })
        .catch(err => {
          t.fail(`processStreamEvent should NOT have failed (${stringify(err)})`, err);
          t.end(err);
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});


test('processStreamEvent with 11 messages with ONLY 3 unique keys that succeed all tasks (despite broken Kinesis, i.e. no unusable/rejected/incomplete)', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const n = 11;

    // Generate a sample AWS event
    const event = generateEvent(4, n, region, {unusables: 0, uniqueKeys: 3});

    // Generate a sample AWS context
    const maxTimeInMillis = 300000;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Setup the task definitions
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, undefined));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      const context = createContext(undefined, undefined, undefined, event, awsContext);
      configureKinesisAndDynamoDBDocClient(t, context, prefix, new Error('Disabling Kinesis'), undefined, {}, undefined, 5);

      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(batch => {
          t.pass(`processStreamEvent must resolve`);
          const states = batch.states;
          const messages = batch.messages;
          t.equal(messages.length, n, `processStreamEvent batch must have ${n} messages`);
          t.equal(batch.firstMessagesToProcess.length, 3, `processStreamEvent batch must have ${3} firstMessagesToProcess`);
          checkMessagesTasksStates(t, batch, taskStates.CompletedState, taskStates.CompletedState, context);

          messages.forEach((msg, i) => {
            const msgState = states.get(msg);
            t.equal(msgState.ones.Task1.attempts, 1, `message [${i}] (${msgState.id}) Task1 attempts must be 1`);
            t.equal(msgState.alls.Task2.attempts, 1, `message [${i}] (${msgState.id}) Task2 attempts must be 1`);
          });

          const batchState = states.get(batch);
          t.equal(batchState.alls.Task2.attempts, 1, `Task2 attempts must be 1`);

          t.ok(batchState.initiating.initiateBatch.isFullyFinalised(), `processStreamEvent initiating must be fully finalised`);
          t.ok(batchState.processing.processBatch.isFullyFinalised(), `processStreamEvent processing must be fully finalised`);
          t.ok(batchState.finalising.finaliseBatch.isFullyFinalised(), `processStreamEvent finalising must be fully finalised`);

          t.equal(batch.unusableRecords.length, 0, `processStreamEvent batch must have ${0} unusable records`);
          t.equal(batch.rejectedMessages.length, 0, `processStreamEvent batch must have ${0} rejected messages`);
          t.equal(batch.undiscardedRejectedMessages.length, 0, `processStreamEvent batch must have ${0} undiscarded rejected messages`);

          // Check that messages were processed in the right sequence
          // Sort a copy of the messages array into message id sequence, which is the right sequence for processing
          const msgsByKey = groupBy(messages, m => states.get(m).key);
          const keys = Object.getOwnPropertyNames(msgsByKey);

          const idOrderByKey = {
            "k1:ABC|k2:10": ["id1:ID-04|id2:7000", "id1:ID-04|id2:7003", "id1:ID-04|id2:7006", "id1:ID-04|id2:7009"],
            "k1:ABC|k2:11": ["id1:ID-04|id2:7001", "id1:ID-04|id2:7004", "id1:ID-04|id2:7007", "id1:ID-04|id2:7010"],
            "k1:ABC|k2:12": ["id1:ID-04|id2:7002", "id1:ID-04|id2:7005", "id1:ID-04|id2:7008"]
          };

          for (let i = 0; i < keys.length; ++i) {
            const key = keys[i];
            const msgs = msgsByKey[key];
            const idOrder = idOrderByKey[key];
            msgs.sort((a, b) => idOrder.indexOf(states.get(a).id) - idOrder.indexOf(states.get(b).id));
            // console.log(`############################ SORTED msgs = ${stringify(msgs.map(m => states.get(m).id))}`);
            // ... and then check that the began & ended dates are correct
            msgs.reduce((acc, msg) => {
              if (acc) {
                const accState = states.get(acc);
                const msgState = states.get(msg);
                t.ok(accState.id <= msgState.id, `Message (${accState.id}) must be < message (${msgState.id})`);
                const accTask1 = accState.ones.Task1;
                const msgTask1 = msgState.ones.Task1;
                t.ok(accTask1.began <= msgTask1.began, `Message (${accState.id}) began (${accTask1.began}) must be <= message (${msgState.id}) began (${msgTask1.began})`);
                t.ok(accTask1.ended <= msgTask1.ended, `Message (${accState.id}) ended (${accTask1.ended}) must be <= message (${msgState.id}) ended (${msgTask1.ended})`);
                t.ok(accTask1.ended <= msgTask1.began, `Message (${accState.id}) ended (${accTask1.ended}) must be <= message (${msgState.id}) began (${msgTask1.began})`);
              }
              return msg;
            }, undefined);
          }

          t.end();
        })
        .catch(err => {
          t.fail(`processStreamEvent should NOT have failed (${stringify(err)})`, err);
          t.end(err);
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('processStreamEvent with 7 messages with ONLY 1 unique key that succeed all tasks (despite broken Kinesis, i.e. no unusable/rejected/incomplete)', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const n = 7;

    // Generate a sample AWS event
    const event = generateEvent(5, n, region, {unusables: 0, uniqueKeys: 1});

    // Generate a sample AWS context
    const maxTimeInMillis = 300000;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Setup the task definitions
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, undefined));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      const context = createContext(undefined, undefined, undefined, event, awsContext);
      configureKinesisAndDynamoDBDocClient(t, context, prefix, new Error('Disabling Kinesis'), undefined, {}, undefined, 5);

      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(batch => {
          t.pass(`processStreamEvent must resolve`);
          const states = batch.states;
          const messages = batch.messages;
          t.equal(messages.length, n, `processStreamEvent batch must have ${n} messages`);
          t.equal(batch.firstMessagesToProcess.length, 1, `processStreamEvent batch must have ${1} firstMessagesToProcess`);
          const lastMsgState = states.get(messages[n - 1]);
          t.equal(states.get(batch.firstMessagesToProcess[0]).eventID, lastMsgState.eventID, `processStreamEvent first message to process must be message with eventID (${lastMsgState.eventID})`);
          checkMessagesTasksStates(t, batch, taskStates.CompletedState, taskStates.CompletedState, context);

          messages.forEach((msg, i) => {
            const msgState = states.get(msg);
            t.equal(msgState.ones.Task1.attempts, 1, `message [${i}] (${msgState.id}) Task1 attempts must be 1`);
            t.equal(msgState.alls.Task2.attempts, 1, `message [${i}] (${msgState.id}) Task2 attempts must be 1`);
          });

          const batchState = states.get(batch);
          t.equal(batchState.alls.Task2.attempts, 1, `Task2 attempts must be 1`);

          t.ok(batchState.initiating.initiateBatch.isFullyFinalised(), `processStreamEvent initiating must be fully finalised`);
          t.ok(batchState.processing.processBatch.isFullyFinalised(), `processStreamEvent processing must be fully finalised`);
          t.ok(batchState.finalising.finaliseBatch.isFullyFinalised(), `processStreamEvent finalising must be fully finalised`);

          t.equal(batch.unusableRecords.length, 0, `processStreamEvent batch must have ${0} unusable records`);
          t.equal(batch.rejectedMessages.length, 0, `processStreamEvent batch must have ${0} rejected messages`);
          t.equal(batch.undiscardedRejectedMessages.length, 0, `processStreamEvent batch must have ${0} undiscarded rejected messages`);

          // Check that messages were processed in the right sequence
          // Sort a copy of the messages array into message id sequence, which is the right sequence for processing
          const msgs = copy(messages);
          const compareOpts = {ignoreCase: true};
          msgs.sort((a, b) => sorting.compareStrings(states.get(a).id, states.get(b).id, compareOpts));
          // ... and then check that the began & ended dates are correct
          msgs.reduce((acc, msg) => {
            if (acc) {
              const accState = states.get(acc);
              const msgState = states.get(msg);
              t.ok(accState.id <= msgState.id, `Message (${accState.id}) must be < message (${msgState.id})`);
              const accTask1 = accState.ones.Task1;
              const msgTask1 = msgState.ones.Task1;
              t.ok(accTask1.began <= msgTask1.began, `Message (${accState.id}) began (${accTask1.began}) must be < message (${msgState.id}) began (${msgTask1.began})`);
              t.ok(accTask1.ended <= msgTask1.ended, `Message (${accState.id}) ended (${accTask1.ended}) must be < message (${msgState.id}) ended (${msgTask1.ended})`);
              t.ok(accTask1.ended <= msgTask1.began, `Message (${accState.id}) ended (${accTask1.ended}) must be < message (${msgState.id}) began (${msgTask1.began})`);
            }
            return msg;
          }, undefined);

          t.end();
        })
        .catch(err => {
          t.fail(`processStreamEvent should NOT have failed (${stringify(err)})`, err);
          t.end(err);
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

// =====================================================================================================================
// processStreamEvent with unusable record(s)
// =====================================================================================================================

test('processStreamEvent with 1 unusable record', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const n = 0;
    const u = 1;

    // Generate a sample AWS event
    const event = generateEvent(5, n, region, {unusables: u});

    // Generate a sample AWS context
    const maxTimeInMillis = 60000;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Setup the task definitions
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, undefined));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      const context = createContext(undefined, undefined, undefined, event, awsContext);
      configureKinesisAndDynamoDBDocClient(t, context, prefix, undefined, undefined, {}, undefined, 5);

      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(batch => {
          t.pass(`processStreamEvent must resolve`);
          const states = batch.states;
          const messages = batch.messages;
          t.equal(messages.length, n, `processStreamEvent batch must have ${n} messages`);
          checkMessagesTasksStates(t, batch, taskStates.CompletedState, taskStates.CompletedState, context);

          messages.forEach((msg, i) => {
            const msgState = states.get(msg);
            t.equal(msgState.ones.Task1.attempts, 1, `message [${i}] (${msgState.id}) Task1 attempts must be 1`);
            t.equal(msgState.alls.Task2.attempts, 1, `message [${i}] (${msgState.id}) Task2 attempts must be 1`);
          });

          const unusableRecords = batch.unusableRecords;
          t.equal(unusableRecords.length, u, `processStreamEvent batch must have ${u} unusable records`);
          checkUnusableRecordsTasksStates(t, batch, taskStates.CompletedState, context);
          for (let j = 0; j < unusableRecords.length; ++j) {
            const unusableRecord = unusableRecords[j];
            const recordState = states.get(unusableRecord);
            t.equal(recordState.discards.discardUnusableRecord.attempts, 1, `unusable record [${j}] (${recordState.eventID}) discardUnusableRecord attempts must be 1`);
          }

          const batchState = states.get(batch);
          if (messages.length > 0) {
            t.equal(batchState.alls.Task2.attempts, 1, `Task2 attempts must be 1`);
          }

          t.ok(batchState.initiating.initiateBatch.isFullyFinalised(), `processStreamEvent initiating must be fully finalised`);
          t.ok(batchState.processing.processBatch.isFullyFinalised(), `processStreamEvent processing must be fully finalised`);
          t.ok(batchState.finalising.finaliseBatch.isFullyFinalised(), `processStreamEvent finalising must be fully finalised`);

          t.equal(batch.rejectedMessages.length, 0, `processStreamEvent batch must have ${0} rejected messages`);
          t.equal(batch.undiscardedRejectedMessages.length, 0, `processStreamEvent batch must have ${0} undiscarded rejected messages`);

          t.end();
        })
        .catch(err => {
          t.fail(`processStreamEvent should NOT have failed (${JSON.stringify(err)})`, err);
          t.end(err);
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${JSON.stringify(err)})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('processStreamEvent with 1 unusable record, but if cannot discard must fail', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const event = sampleKinesisEvent(streamName, undefined, undefined, false);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    const fatalError = new Error('Disabling Kinesis');
    configureDefaults(t, context, fatalError);

    // Setup the task definitions
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, undefined));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      consumer.configureStreamConsumer(context, undefined, undefined, event, awsContext);
      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(messages => {
          const n = messages.length;
          t.fail(`processStreamEvent must NOT resolve with ${n} message(s)`);
          t.end();
        })
        .catch(err => {
            t.pass(`processStreamEvent must reject with error (${stringify(err)})`);
            t.equal(err, fatalError, `processStreamEvent error must be ${fatalError}`);

            consumer.awaitStreamConsumerResults(err.streamConsumerResults).then(batch => {
              const messages = batch.messages;
              t.equal(messages.length, 0, `processStreamEvent batch must have ${0} messages`);
              t.equal(batch.unusableRecords.length, 1, `processStreamEvent batch must have ${1} unusable records`);

              t.ok(batch.processing.completed, `processStreamEvent processing must be completed`);
              t.notOk(batch.processing.failed, `processStreamEvent processing must not be failed`);
              t.notOk(batch.processing.timedOut, `processStreamEvent processing must not be timed-out`);

              if (batch.discardedUnusableRecords || !batch.discardUnusableRecordsError) {
                t.fail(`discardUnusableRecord must fail`);
              }
              t.equal(batch.discardUnusableRecordsError, fatalError, `discardUnusableRecord must fail with ${fatalError}`);

              if (!batch.handledIncompleteMessages || batch.handleIncompleteMessagesError) {
                t.fail(`handleIncompleteMessages must not fail with ${batch.handleIncompleteMessagesError}`);
              }
              if (batch.handledIncompleteMessages) {
                t.equal(batch.handledIncompleteMessages.length, 0, `handleIncompleteMessages must have ${0} handled incomplete records`);
              }

              if (!batch.discardedRejectedMessages || batch.discardRejectedMessagesError) {
                t.fail(`discardRejectedMessage must not fail with ${batch.discardRejectedMessagesError}`);
              }
              if (batch.discardedRejectedMessages) {
                t.equal(batch.discardedRejectedMessages.length, 0, `discardedRejectedMessages must have ${0} discarded rejected messages`);
              }

              t.end();
            });
          }
        );
    } catch
      (err) {
      t.fail(`processStreamEvent should NOT have failed on try-catch (${err})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

// TODO HERE


// =====================================================================================================================
// processStreamEvent with failing processOne message(s)
// =====================================================================================================================

test('processStreamEvent with 1 message that fails its processOne task, resubmits', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const event = sampleKinesisEvent(streamName, undefined, sampleMessage(1), false);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    configureDefaults(t, context, undefined);

    // Setup the task definitions
    const processOneError = new Error('Failing process one task');
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, processOneError));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      consumer.configureStreamConsumer(context, undefined, undefined, event, awsContext);
      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(batch => {
          t.pass(`processStreamEvent must resolve`);
          const n = 1;
          const messages = batch.messages;
          t.equal(messages.length, n, `processStreamEvent batch must have ${n} messages`);
          checkMessagesTasksStates(t, batch, taskStates.Failed, taskStates.CompletedState, context);

          const msg1State = batch.states.get(messages[0]);
          t.equal(msg1State.ones.Task1.attempts, 1, `Task1 attempts must be 1`);
          t.equal(msg1State.alls.Task2.attempts, 1, `Task2 attempts must be 1`);

          t.ok(batch.processing.completed, `processStreamEvent processing must be completed`);
          t.notOk(batch.processing.failed, `processStreamEvent processing must not be failed`);
          t.notOk(batch.processing.timedOut, `processStreamEvent processing must not be timed-out`);

          t.equal(batch.unusableRecords.length, 0, `processStreamEvent batch must have ${0} unusable records`);
          t.equal(batch.discardedUnusableRecords.length, 0, `processStreamEvent batch must have ${0} discarded unusable records`);
          t.equal(batch.handledIncompleteMessages.length, 1, `processStreamEvent batch must have ${1} handled incomplete records`);
          t.equal(batch.discardedRejectedMessages.length, 0, `processStreamEvent batch must have ${0} discarded rejected messages`);

          t.end();
        })
        .catch(err => {
          t.fail(`processStreamEvent should NOT have failed (${stringify(err)})`, err);
          t.end(err);
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('processStreamEvent with 1 message that fails its processOne task, but cannot resubmit must fail', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const event = sampleKinesisEvent(streamName, undefined, sampleMessage(1), false);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    const fatalError = new Error('Disabling Kinesis');
    configureDefaults(t, context, fatalError);

    // Setup the task definitions
    const processOneError = new Error('Failing process one task');
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, processOneError));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      consumer.configureStreamConsumer(context, undefined, undefined, event, awsContext);
      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(messages => {
          const n = messages.length;
          t.fail(`processStreamEvent must NOT resolve with ${n} message(s)`);
          t.end();
        })
        .catch(err => {
          t.pass(`processStreamEvent must reject with error (${stringify(err)})`);
          t.equal(err, fatalError, `processStreamEvent error must be ${fatalError}`);

          consumer.awaitStreamConsumerResults(err.streamConsumerResults).then(batch => {
            const messages = batch.messages;
            t.equal(messages.length, 1, `processStreamEvent batch must have ${1} messages`);
            t.equal(batch.unusableRecords.length, 0, `processStreamEvent batch must have ${0} unusable records`);

            t.ok(batch.processing.completed, `processStreamEvent processing must be completed`);
            t.notOk(batch.processing.failed, `processStreamEvent processing must not be failed`);
            t.notOk(batch.processing.timedOut, `processStreamEvent processing must not be timed-out`);

            if (!batch.discardedUnusableRecords || batch.discardUnusableRecordsError) {
              t.fail(`discardUnusableRecord must not fail with ${batch.discardUnusableRecordsError}`);
            }
            if (batch.discardedUnusableRecords) {
              t.equal(batch.discardedUnusableRecords.length, 0, `discardedUnusableRecords must have ${0} discarded unusable records`);
            }

            if (batch.handledIncompleteMessages || !batch.handleIncompleteMessagesError) {
              t.fail(`handleIncompleteMessages must fail with ${batch.handleIncompleteMessagesError}`);
            }
            t.equal(batch.handleIncompleteMessagesError, fatalError, `handleIncompleteMessages must fail with ${fatalError}`);

            if (!batch.discardedRejectedMessages || batch.discardRejectedMessagesError) {
              t.fail(`discardRejectedMessage must not fail with ${batch.discardRejectedMessagesError}`);
            }
            if (batch.discardedRejectedMessages) {
              t.equal(batch.discardedRejectedMessages.length, 0, `discardedRejectedMessages must have ${0} discarded rejected messages`);
            }

            t.end();
          });
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

// =====================================================================================================================
// processStreamEvent with failing processAll message(s)
// =====================================================================================================================

test('processStreamEvent with 1 message that fails its processAll task, resubmits', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const event = sampleKinesisEvent(streamName, undefined, sampleMessage(1), false);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    configureDefaults(t, context, undefined);

    // Setup the task definitions
    const processAllError = new Error('Failing process all task');
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, undefined));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, processAllError));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      consumer.configureStreamConsumer(context, undefined, undefined, event, awsContext);
      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(batch => {
          t.pass(`processStreamEvent must resolve`);
          const n = 1;
          const messages = batch.messages;
          t.equal(messages.length, n, `processStreamEvent batch must have ${n} messages`);
          checkMessagesTasksStates(t, batch, taskStates.CompletedState, taskStates.FailedState, context);

          const msg1State = batch.states.get(messages[0]);
          t.equal(msg1State.ones.Task1.attempts, 1, `Task1 attempts must be 1`);
          t.equal(msg1State.alls.Task2.attempts, 1, `Task2 attempts must be 1`);

          t.ok(batch.processing.completed, `processStreamEvent processing must be completed`);
          t.notOk(batch.processing.failed, `processStreamEvent processing must not be failed`);
          t.notOk(batch.processing.timedOut, `processStreamEvent processing must not be timed-out`);

          t.equal(batch.unusableRecords.length, 0, `processStreamEvent batch must have ${0} unusable records`);
          t.equal(batch.discardedUnusableRecords.length, 0, `processStreamEvent batch must have ${0} discarded unusable records`);
          t.equal(batch.handledIncompleteMessages.length, 1, `processStreamEvent batch must have ${1} handled incomplete records`);
          t.equal(batch.discardedRejectedMessages.length, 0, `processStreamEvent batch must have ${0} discarded rejected messages`);

          t.end();
        })
        .catch(err => {
          t.fail(`processStreamEvent should NOT have failed (${stringify(err)})`, err);
          t.end(err);
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('processStreamEvent with 1 message that fails its processAll task, but cannot resubmit must fail', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const event = sampleKinesisEvent(streamName, undefined, sampleMessage(1), false);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    const fatalError = new Error('Disabling Kinesis');
    configureDefaults(t, context, fatalError);

    // Setup the task definitions
    const processAllError = new Error('Failing process all task');
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, undefined));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, processAllError));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      consumer.configureStreamConsumer(context, undefined, undefined, event, awsContext);
      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(messages => {
          const n = messages.length;
          t.fail(`processStreamEvent must NOT resolve with ${n} message(s)`);
          t.end();
        })
        .catch(err => {
          t.pass(`processStreamEvent must reject with error (${stringify(err)})`);
          t.equal(err, fatalError, `processStreamEvent error must be ${fatalError}`);

          consumer.awaitStreamConsumerResults(err.streamConsumerResults).then(batch => {
            const messages = batch.messages;
            t.equal(messages.length, 1, `processStreamEvent batch must have ${1} messages`);
            t.equal(batch.unusableRecords.length, 0, `processStreamEvent batch must have ${0} unusable records`);

            t.ok(batch.processing.completed, `processStreamEvent processing must be completed`);
            t.notOk(batch.processing.failed, `processStreamEvent processing must not be failed`);
            t.notOk(batch.processing.timedOut, `processStreamEvent processing must not be timed-out`);

            if (!batch.discardedUnusableRecords || batch.discardUnusableRecordsError) {
              t.fail(`discardUnusableRecord must not fail with ${batch.discardUnusableRecordsError}`);
            }
            if (batch.discardedUnusableRecords) {
              t.equal(batch.discardedUnusableRecords.length, 0, `discardedUnusableRecords must have ${0} discarded unusable records`);
            }

            if (batch.handledIncompleteMessages || !batch.handleIncompleteMessagesError) {
              t.fail(`handleIncompleteMessages must fail with ${batch.handleIncompleteMessagesError}`);
            }
            t.equal(batch.handleIncompleteMessagesError, fatalError, `handleIncompleteMessages must fail with ${fatalError}`);

            if (!batch.discardedRejectedMessages || batch.discardRejectedMessagesError) {
              t.fail(`discardRejectedMessage must not fail with ${batch.discardRejectedMessagesError}`);
            }
            if (batch.discardedRejectedMessages) {
              t.equal(batch.discardedRejectedMessages.length, 0, `discardedRejectedMessages must have ${0} discarded rejected messages`);
            }

            t.end();
          });
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

// =====================================================================================================================
// processStreamEvent with successful message(s) with inactive tasks, must discard abandoned message(s)
// =====================================================================================================================

test('processStreamEvent with 1 message that succeeds, but has 1 abandoned task - must discard rejected message', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const msg = sampleMessage(1);

    // Add some "history" to this message to give it a no-longer active task that will trigger abandonment of this task
    const taskX = Task.createTask(TaskDef.defineTask('TaskX', execute1));
    taskX.fail(new Error('Previously failed'));
    for (let i = 0; i < 100; ++i) {
      taskX.incrementAttempts();
    }
    const taskXLike = JSON.parse(JSON.stringify(taskX));

    const msgState0 = {ones: {'TaskX': taskXLike}};

    const msgState = JSON.parse(JSON.stringify(msgState0));
    t.ok(msgState, 'Message state with tasks is parsable');
    const taskXRevived = msgState.ones.TaskX;
    t.ok(Task.isTaskLike(taskXRevived), `TaskX must be task-like (${stringify(taskXRevived)})`);
    t.deepEqual(taskXRevived, taskXLike, `TaskX revived must be original TaskX task-like (${stringify(taskXRevived)})`);
    // console.log(`##### TASK X  ${JSON.stringify(taskX)}`);
    // console.log(`##### REVIVED ${JSON.stringify(taskXRevived)}`);

    const event = sampleKinesisEvent(streamName, undefined, msg, false);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    configureDefaults(t, context, undefined);

    // Setup the task definitions
    //const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, undefined)); //.then(msg => taskUtils.getTask(batch.states.get(msg).ones, 'Task1')
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    const processOneTaskDefs = []; //[taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      consumer.configureStreamConsumer(context, undefined, undefined, event, awsContext);
      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(batch => {
          t.pass(`processStreamEvent must resolve`);
          const n = 1;
          const messages = batch.messages;
          t.equal(messages.length, n, `processStreamEvent batch must have ${n} messages`);
          checkMessagesTasksStates(t, batch, taskStates.Abandoned, taskStates.CompletedState, context);
          const msg1State = batch.states.get(messages[0]);
          t.equal(msg1State.ones.TaskX.attempts, 100, `TaskX attempts must be 100`);
          t.equal(msg1State.alls.Task2.attempts, 1, `Task2 attempts must be 1`);

          t.ok(batch.processing.completed, `processStreamEvent processing must be completed`);
          t.notOk(batch.processing.failed, `processStreamEvent processing must not be failed`);
          t.notOk(batch.processing.timedOut, `processStreamEvent processing must not be timed-out`);

          t.equal(batch.unusableRecords.length, 0, `processStreamEvent batch must have ${0} unusable records`);
          t.equal(batch.discardedUnusableRecords.length, 0, `processStreamEvent batch must have ${0} discarded unusable records`);
          t.equal(batch.handledIncompleteMessages.length, 0, `processStreamEvent batch must have ${0} handled incomplete records`);
          t.equal(batch.discardedRejectedMessages.length, 1, `processStreamEvent batch must have ${1} discarded rejected messages`);

          t.end();
        })
        .catch(err => {
          t.fail(`processStreamEvent should NOT have failed (${stringify(err)})`, err);
          t.end(err);
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('processStreamEvent with 1 message that succeeds, but has 1 abandoned task - must fail if cannot discard rejected message', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const msg = sampleMessage(1);

    // Add some "history" to this message to give it a no-longer active task that will trigger abandonment of this task
    const taskX = Task.createTask(TaskDef.defineTask('TaskX', execute1));
    taskX.fail(new Error('Previously failed'));
    const msgState0 = {ones: {'TaskX': JSON.parse(JSON.stringify(taskX))}};

    const event = sampleKinesisEvent(streamName, undefined, msg, false);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    const fatalError = new Error('Disabling Kinesis');
    configureDefaults(t, context, fatalError);

    // Setup the task definitions
    //const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, undefined)); //.then(msg => taskUtils.getTask(batch.states.get(msg).ones, 'Task1')
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    const processOneTaskDefs = []; //[taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      consumer.configureStreamConsumer(context, undefined, undefined, event, awsContext);
      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(messages => {
          const n = messages.length;
          t.fail(`processStreamEvent must NOT resolve with ${n} message(s)`);
          t.end();
        })
        .catch(err => {
          t.pass(`processStreamEvent must reject with error (${stringify(err)})`);
          t.equal(err, fatalError, `processStreamEvent error must be ${fatalError}`);

          consumer.awaitStreamConsumerResults(err.streamConsumerResults).then(batch => {
            const messages = batch.messages;
            t.equal(messages.length, 1, `processStreamEvent batch must have ${1} messages`);
            t.equal(batch.unusableRecords.length, 0, `processStreamEvent batch must have ${0} unusable records`);

            t.ok(batch.processing.completed, `processStreamEvent processing must be completed`);
            t.notOk(batch.processing.failed, `processStreamEvent processing must not be failed`);
            t.notOk(batch.processing.timedOut, `processStreamEvent processing must not be timed-out`);

            if (!batch.discardedUnusableRecords || batch.discardUnusableRecordsError) {
              t.fail(`discardUnusableRecord must not fail with ${batch.discardUnusableRecordsError}`);
            }
            if (batch.discardedUnusableRecords) {
              t.equal(batch.discardedUnusableRecords.length, 0, `discardedUnusableRecords must have ${0} discarded unusable records`);
            }

            if (!batch.handledIncompleteMessages || batch.handleIncompleteMessagesError) {
              t.fail(`handleIncompleteMessages must not fail with ${batch.handleIncompleteMessagesError}`);
            }
            if (batch.handledIncompleteMessages) {
              t.equal(batch.handledIncompleteMessages.length, 0, `handleIncompleteMessages must have ${0} handled incomplete messages`);
            }

            if (batch.discardedRejectedMessages || !batch.discardRejectedMessagesError) {
              t.fail(`discardRejectedMessage must fail with ${batch.discardRejectedMessagesError}`);
            }
            t.equal(batch.discardRejectedMessagesError, fatalError, `discardedRejectedMessages must fail with ${fatalError}`);

            t.end();
          });
        });
    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

// =====================================================================================================================
// processStreamEvent with rejecting message(s), must discard rejected message(s)
// =====================================================================================================================

test('processStreamEvent with 1 message that rejects - must discard rejected message', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const msg = sampleMessage(1);

    const event = sampleKinesisEvent(streamName, undefined, msg, false);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    configureDefaults(t, context, undefined);

    // Setup the task definitions
    const rejectError = new Error('Rejecting message');
    const executeOneAsync = sampleExecuteOneAsync(5, undefined, (msg, context) => {
      // trigger a rejection from inside
      console.log(`*** Triggering an internal reject`);
      batch.states.get(msg).ones.Task1.reject('Forcing reject', rejectError, true);
    });
    const taskDef1 = TaskDef.defineTask('Task1', executeOneAsync);
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      consumer.configureStreamConsumer(context, undefined, undefined, event, awsContext);
      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(batch => {
          t.pass(`processStreamEvent must resolve`);
          const n = 1;
          const messages = batch.messages;
          t.equal(messages.length, n, `processStreamEvent batch must have ${n} messages`);
          checkMessagesTasksStates(t, batch, taskStates.Rejected, taskStates.CompletedState, context);
          const msg0State = batch.states.get(messages[0]);
          t.equal(msg0State.ones.Task1.attempts, 1, `Task1 attempts must be 1`);
          t.equal(msg0State.alls.Task2.attempts, 1, `Task2 attempts must be 1`);

          t.ok(batch.processing.completed, `processStreamEvent processing must be completed`);
          t.notOk(batch.processing.failed, `processStreamEvent processing must not be failed`);
          t.notOk(batch.processing.timedOut, `processStreamEvent processing must not be timed-out`);

          t.equal(batch.unusableRecords.length, 0, `processStreamEvent batch must have ${0} unusable records`);
          t.equal(batch.discardedUnusableRecords.length, 0, `processStreamEvent batch must have ${0} discarded unusable records`);
          t.equal(batch.handledIncompleteMessages.length, 0, `processStreamEvent batch must have ${0} handled incomplete records`);
          t.equal(batch.discardedRejectedMessages.length, 1, `processStreamEvent batch must have ${1} discarded rejected messages`);

          t.end();
        })
        .catch(err => {
          t.fail(`processStreamEvent should NOT have failed (${stringify(err)})`, err);
          t.end(err);
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('processStreamEvent with 1 message that rejects, but cannot discard rejected message must fail', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const msg = sampleMessage(1);

    const event = sampleKinesisEvent(streamName, undefined, msg, false);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    const fatalError = new Error('Disabling Kinesis');
    configureDefaults(t, context, fatalError);

    // Setup the task definitions
    const rejectError = new Error('Rejecting message');
    const executeOneAsync = sampleExecuteOneAsync(5, undefined, (msg, context) => {
      // trigger a rejection from inside
      console.log(`*** Triggering an internal reject`);
      batch.states.get(msg).ones.Task1.reject('Forcing reject', rejectError, true);
    });
    const taskDef1 = TaskDef.defineTask('Task1', executeOneAsync);
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      consumer.configureStreamConsumer(context, undefined, undefined, event, awsContext);
      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(messages => {
          const n = messages.length;
          t.fail(`processStreamEvent must NOT resolve with ${n} message(s)`);
          t.end();
        })
        .catch(err => {
          t.pass(`processStreamEvent must reject with error (${stringify(err)})`);
          t.equal(err, fatalError, `processStreamEvent error must be ${fatalError}`);

          consumer.awaitStreamConsumerResults(err.streamConsumerResults).then(batch => {
            const messages = batch.messages;
            t.equal(messages.length, 1, `processStreamEvent batch must have ${1} messages`);
            t.equal(batch.unusableRecords.length, 0, `processStreamEvent batch must have ${0} unusable records`);

            t.ok(batch.processing.completed, `processStreamEvent processing must be completed`);
            t.notOk(batch.processing.failed, `processStreamEvent processing must not be failed`);
            t.notOk(batch.processing.timedOut, `processStreamEvent processing must not be timed-out`);

            if (!batch.discardedUnusableRecords || batch.discardUnusableRecordsError) {
              t.fail(`discardUnusableRecord must not fail with ${batch.discardUnusableRecordsError}`);
            }
            if (batch.discardedUnusableRecords) {
              t.equal(batch.discardedUnusableRecords.length, 0, `discardedUnusableRecords must have ${0} discarded unusable records`);
            }

            if (!batch.handledIncompleteMessages || batch.handleIncompleteMessagesError) {
              t.fail(`handleIncompleteMessages must not fail with ${batch.handleIncompleteMessagesError}`);
            }
            if (batch.handledIncompleteMessages) {
              t.equal(batch.handledIncompleteMessages.length, 0, `handleIncompleteMessages must have ${0} handled incomplete messages`);
            }

            if (batch.discardedRejectedMessages || !batch.discardRejectedMessagesError) {
              t.fail(`discardRejectedMessage must fail with ${batch.discardRejectedMessagesError}`);
            }
            t.equal(batch.discardRejectedMessagesError, fatalError, `discardedRejectedMessages must fail with ${fatalError}`);

            t.end();
          });
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

// =====================================================================================================================
// processStreamEvent with message(s) exceeding max number of attempts on all tasks, must discard Discarded message(s)
// =====================================================================================================================

test('processStreamEvent with 1 message that exceeds max number of attempts on all its tasks - must discard Discarded message', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    configureDefaults(t, context, undefined);

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const msg = sampleMessage(1);

    const maxNumberOfAttempts = kinesisProcessing.getMaxNumberOfAttempts(context);

    // Add some "history" to this message to give it a no-longer active task that will trigger abandonment of this task
    const task1Before = Task.createTask(TaskDef.defineTask('Task1', execute1));
    task1Before.fail(new Error('Previously failed Task1'));

    const task2Before = Task.createTask(TaskDef.defineTask('Task2', execute1));
    task2Before.fail(new Error('Previously failed Task2'));

    // Push both tasks number of attempts to the brink
    for (let a = 0; a < maxNumberOfAttempts - 1; ++a) {
      task1Before.incrementAttempts();
      task2Before.incrementAttempts();
    }
    t.equal(task1Before.attempts, maxNumberOfAttempts - 1, `BEFORE Task1 attempts must be ${maxNumberOfAttempts - 1}`);
    t.equal(task2Before.attempts, maxNumberOfAttempts - 1, `BEFORE Task2 attempts must be ${maxNumberOfAttempts - 1}`);

    const task1Like = JSON.parse(JSON.stringify(task1Before));
    const task2Like = JSON.parse(JSON.stringify(task2Before));

    const msgState0 = {ones: {'Task1': task1Like}, alls: {'Task2': task2Like}};

    const msgState = JSON.parse(JSON.stringify(msgState0));
    t.ok(msgState, 'Message with tasks is parsable');

    const task1Revived = msgState.ones.Task1;
    t.ok(Task.isTaskLike(task1Revived), `Task1 must be task-like (${stringify(task1Revived)})`);
    t.deepEqual(task1Revived, task1Like, `Task1 revived must be original Task1 task-like (${stringify(task1Revived)})`);

    const task2Revived = msgState.alls.Task2;
    t.ok(Task.isTaskLike(task2Revived), `Task2 must be task-like (${stringify(task2Revived)})`);
    t.deepEqual(task2Revived, task2Like, `Task2 revived must be original Task2 task-like (${stringify(task2Revived)})`);

    t.equal(task1Revived.attempts, maxNumberOfAttempts - 1, `REVIVED Task1 attempts must be ${maxNumberOfAttempts - 1}`);
    t.equal(task2Revived.attempts, maxNumberOfAttempts - 1, `REVIVED Task2 attempts must be ${maxNumberOfAttempts - 1}`);


    const event = sampleKinesisEvent(streamName, undefined, msg, false);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Setup the task definitions
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, new Error('Final failure')));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, new Error('Final failure')));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      consumer.configureStreamConsumer(context, undefined, undefined, event, awsContext);
      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(batch => {
          t.pass(`processStreamEvent must resolve`);
          const n = 1;
          const messages = batch.messages;
          t.equal(messages.length, n, `processStreamEvent batch must have ${n} messages`);
          checkMessagesTasksStates(t, batch, taskStates.Discarded, taskStates.Discarded, context);
          const msgState = batch.states.get(messages[0]);
          t.equal(msgState.ones.Task1.attempts, maxNumberOfAttempts, `Task1 attempts must be ${maxNumberOfAttempts}`);
          t.equal(msgState.alls.Task2.attempts, maxNumberOfAttempts, `Task2 attempts must be ${maxNumberOfAttempts}`);

          t.ok(batch.processing.completed, `processStreamEvent processing must be completed`);
          t.notOk(batch.processing.failed, `processStreamEvent processing must not be failed`);
          t.notOk(batch.processing.timedOut, `processStreamEvent processing must not be timed-out`);

          t.equal(batch.unusableRecords.length, 0, `processStreamEvent batch must have ${0} unusable records`);
          t.equal(batch.discardedUnusableRecords.length, 0, `processStreamEvent batch must have ${0} discarded unusable records`);
          t.equal(batch.handledIncompleteMessages.length, 0, `processStreamEvent batch must have ${0} handled incomplete records`);
          t.equal(batch.discardedRejectedMessages.length, 1, `processStreamEvent batch must have ${1} discarded rejected messages`);

          t.end();
        })
        .catch(err => {
          t.fail(`processStreamEvent should NOT have failed (${stringify(err)})`, err);
          t.end(err);
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('processStreamEvent with 1 message that exceeds max number of attempts on all its tasks, but cannot discard message must fail', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    const fatalError = new Error('Disabling Kinesis');
    configureDefaults(t, context, fatalError);

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const msg = sampleMessage(1);

    const maxNumberOfAttempts = kinesisProcessing.getMaxNumberOfAttempts(context);

    // Add some "history" to this message to give it a no-longer active task that will trigger abandonment of this task
    const task1Before = Task.createTask(TaskDef.defineTask('Task1', execute1));
    task1Before.fail(new Error('Previously failed Task1'));

    const task2Before = Task.createTask(TaskDef.defineTask('Task2', execute1));
    task2Before.fail(new Error('Previously failed Task2'));

    // Push both tasks number of attempts to the brink
    for (let a = 0; a < maxNumberOfAttempts - 1; ++a) {
      task1Before.incrementAttempts();
      task2Before.incrementAttempts();
    }

    const task1Like = JSON.parse(JSON.stringify(task1Before));
    const task2Like = JSON.parse(JSON.stringify(task2Before));

    const msgState0 = {ones: {'Task1': task1Like}, alls: {'Task2': task2Like}};

    const msgState = JSON.parse(JSON.stringify(msgState0));
    t.ok(msgState, 'Message state with tasks is parsable');

    const task1Revived = msgState.ones.Task1;
    t.ok(Task.isTaskLike(task1Revived), `Task1 must be task-like (${stringify(task1Revived)})`);
    t.deepEqual(task1Revived, task1Like, `Task1 revived must be original Task1 task-like (${stringify(task1Revived)})`);

    const task2Revived = msgState.alls.Task2;
    t.ok(Task.isTaskLike(task2Revived), `Task2 must be task-like (${stringify(task2Revived)})`);
    t.deepEqual(task2Revived, task2Like, `Task2 revived must be original Task2 task-like (${stringify(task2Revived)})`);


    const event = sampleKinesisEvent(streamName, undefined, msg, false);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Setup the task definitions
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, new Error('Final failure')));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, new Error('Final failure')));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      consumer.configureStreamConsumer(context, undefined, undefined, event, awsContext);
      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(batch => {
          const n = batch.messages.length;
          t.fail(`processStreamEvent must NOT resolve with ${n} message(s)`);
          t.end();
        })
        .catch(err => {
          t.pass(`processStreamEvent must reject with error (${stringify(err)})`);
          t.equal(err, fatalError, `processStreamEvent error must be ${fatalError}`);

          consumer.awaitStreamConsumerResults(err.streamConsumerResults).then(batch => {
            const messages = batch.messages;
            t.equal(messages.length, 1, `processStreamEvent batch must have ${1} messages`);
            t.equal(batch.unusableRecords.length, 0, `processStreamEvent batch must have ${0} unusable records`);

            t.ok(batch.processing.completed, `processStreamEvent processing must be completed`);
            t.notOk(batch.processing.failed, `processStreamEvent processing must not be failed`);
            t.notOk(batch.processing.timedOut, `processStreamEvent processing must not be timed-out`);

            if (!batch.discardedUnusableRecords || batch.discardUnusableRecordsError) {
              t.fail(`discardUnusableRecord must not fail with ${batch.discardUnusableRecordsError}`);
            }
            if (batch.discardedUnusableRecords) {
              t.equal(batch.discardedUnusableRecords.length, 0, `discardedUnusableRecords must have ${0} discarded unusable records`);
            }

            if (!batch.handledIncompleteMessages || batch.handleIncompleteMessagesError) {
              t.fail(`handleIncompleteMessages must not fail with ${batch.handleIncompleteMessagesError}`);
            }
            if (batch.handledIncompleteMessages) {
              t.equal(batch.handledIncompleteMessages.length, 0, `handleIncompleteMessages must have ${0} handled incomplete messages`);
            }

            if (batch.discardedRejectedMessages || !batch.discardRejectedMessagesError) {
              t.fail(`discardRejectedMessage must fail with ${batch.discardRejectedMessagesError}`);
            }
            t.equal(batch.discardRejectedMessagesError, fatalError, `discardedRejectedMessages must fail with ${fatalError}`);

            t.end();
          });
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('processStreamEvent with 1 message that only exceeds max number of attempts on 1 of its 2 its tasks, must not discard message yet', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    configureDefaults(t, context, undefined);

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const msg = sampleMessage(1);

    const maxNumberOfAttempts = kinesisProcessing.getMaxNumberOfAttempts(context);

    // Add some "history" to this message to give it a no-longer active task that will trigger abandonment of this task
    const task1Before = Task.createTask(TaskDef.defineTask('Task1', execute1));
    task1Before.fail(new Error('Previously failed Task1'));

    const task2Before = Task.createTask(TaskDef.defineTask('Task2', execute1));
    task2Before.fail(new Error('Previously failed Task2'));

    // Push 1st task's number of attempts to max - 2 (i.e. won't exceed this round)
    for (let a = 0; a < maxNumberOfAttempts - 2; ++a) {
      task1Before.incrementAttempts();
    }
    // Push 2nd task's number of attempts to max - 1 (will exceed this round
    for (let a = 0; a < maxNumberOfAttempts - 1; ++a) {
      task2Before.incrementAttempts();
    }

    const task1Like = JSON.parse(JSON.stringify(task1Before));
    const task2Like = JSON.parse(JSON.stringify(task2Before));

    const msgState0 = {ones: {'Task1': task1Like}, alls: {'Task2': task2Like}};

    const msgState = JSON.parse(JSON.stringify(msgState0));
    t.ok(msgState, 'Message state with tasks is parsable');

    const task1Revived = msgState.ones.Task1;
    t.ok(Task.isTaskLike(task1Revived), `Task1 must be task-like (${stringify(task1Revived)})`);
    t.deepEqual(task1Revived, task1Like, `Task1 revived must be original Task1 task-like (${stringify(task1Revived)})`);

    const task2Revived = msgState.alls.Task2;
    t.ok(Task.isTaskLike(task2Revived), `Task2 must be task-like (${stringify(task2Revived)})`);
    t.deepEqual(task2Revived, task2Like, `Task2 revived must be original Task2 task-like (${stringify(task2Revived)})`);


    const event = sampleKinesisEvent(streamName, undefined, msg, false);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Setup the task definitions
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, new Error('Final failure')));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, new Error('Final failure')));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      consumer.configureStreamConsumer(context, undefined, undefined, event, awsContext);
      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(batch => {
          t.pass(`processStreamEvent must resolve`);
          const n = 1;
          const messages = batch.messages;
          t.equal(messages.length, n, `processStreamEvent batch must have ${n} messages`);
          checkMessagesTasksStates(t, batch, taskStates.Failed, taskStates.Failed, context);
          const msgState = batch.states.get(messages[0]);
          t.equal(msgState.ones.Task1.attempts, maxNumberOfAttempts - 1, `Task1 attempts must be ${maxNumberOfAttempts - 1}`);
          t.equal(msgState.alls.Task2.attempts, maxNumberOfAttempts, `Task1 attempts must be ${maxNumberOfAttempts}`);

          t.ok(batch.processing.completed, `processStreamEvent processing must be completed`);
          t.notOk(batch.processing.failed, `processStreamEvent processing must not be failed`);
          t.notOk(batch.processing.timedOut, `processStreamEvent processing must not be timed-out`);

          t.equal(batch.unusableRecords.length, 0, `processStreamEvent batch must have ${0} unusable records`);
          t.equal(batch.discardedUnusableRecords.length, 0, `processStreamEvent batch must have ${0} discarded unusable records`);
          t.equal(batch.handledIncompleteMessages.length, 1, `processStreamEvent batch must have ${1} handled incomplete records`);
          t.equal(batch.discardedRejectedMessages.length, 0, `processStreamEvent batch must have ${0} discarded rejected messages`);

          t.end();
        })
        .catch(err => {
          t.fail(`processStreamEvent should NOT have failed (${stringify(err)})`, err);
          t.end(err);
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

// =====================================================================================================================
// processStreamEvent with 1 message and triggered timeout promise, must resubmit incomplete message
// =====================================================================================================================

test('processStreamEvent with 1 message and triggered timeout promise, must resubmit incomplete message', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    configureDefaults(t, context, undefined);

    const n = 1;

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const event = sampleKinesisEvent(streamName, undefined, sampleMessage(1), false);

    // Generate a sample AWS context
    const maxTimeInMillis = 10;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Setup the task definitions
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(15, undefined));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(15, undefined));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      consumer.configureStreamConsumer(context, undefined, undefined, event, awsContext);
      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(batch => {
          t.pass(`processStreamEvent must resolve`);
          const n = 1;
          const messages = batch.messages;
          t.equal(messages.length, n, `processStreamEvent batch must have ${n} messages`);
          checkMessagesTasksStates(t, batch, taskStates.TimedOut, taskStates.TimedOut, context);
          const msgState = batch.states.get(messages[0]);
          t.equal(msgState.ones.Task1.attempts, 1, `Task1 attempts must be 1`);
          t.equal(msgState.alls.Task2.attempts, 1, `Task2 attempts must be 1`);

          t.notOk(batch.processing.completed, `processStreamEvent processing must not be completed`);
          t.notOk(batch.processing.failed, `processStreamEvent processing must not be failed`);
          t.ok(batch.processing.timedOut, `processStreamEvent processing must be timed-out`);

          t.equal(batch.unusableRecords.length, 0, `processStreamEvent batch must have ${0} unusable records`);
          t.equal(batch.discardedUnusableRecords.length, 0, `processStreamEvent batch must have ${0} discarded unusable records`);
          t.equal(batch.handledIncompleteMessages.length, 1, `processStreamEvent batch must have ${1} handled incomplete records`);
          t.equal(batch.discardedRejectedMessages.length, 0, `processStreamEvent batch must have ${0} discarded rejected messages`);

          t.end();
        })

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('processStreamEvent with 1 message and triggered timeout promise, must fail if it cannot resubmit incomplete messages', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    const fatalError = new Error('Disabling Kinesis');
    configureDefaults(t, context, fatalError);

    const n = 1;

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const event = sampleKinesisEvent(streamName, undefined, sampleMessage(1), false);

    // Generate a sample AWS context
    const maxTimeInMillis = 10;
    const awsContext = sampleAwsContext('1.0.1', 'dev', maxTimeInMillis);

    // Setup the task definitions
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(15, undefined));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(15, undefined));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      consumer.configureStreamConsumer(context, undefined, undefined, event, awsContext);
      const promise = consumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promises.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev', `context.stage must be dev`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(messages => {
          const n = messages.length;
          t.fail(`processStreamEvent must NOT resolve with ${n} message(s)`);
          t.end();
        })
        .catch(err => {
          t.pass(`processStreamEvent must reject with error (${stringify(err)})`);
          t.equal(err, fatalError, `processStreamEvent error must be ${fatalError}`);

          consumer.awaitStreamConsumerResults(err.streamConsumerResults).then(batch => {
            const messages = batch.messages;
            t.equal(messages.length, 1, `processStreamEvent batch must have ${1} messages`);
            t.equal(batch.unusableRecords.length, 0, `processStreamEvent batch must have ${0} unusable records`);

            t.notOk(batch.processing.completed, `processStreamEvent processing must not be completed`);
            t.notOk(batch.processing.failed, `processStreamEvent processing must not be failed`);
            t.ok(batch.processing.timedOut, `processStreamEvent processing must be timed-out`);

            if (!batch.discardedUnusableRecords || batch.discardUnusableRecordsError) {
              t.fail(`discardUnusableRecord must not fail with ${batch.discardUnusableRecordsError}`);
            }
            if (batch.discardedUnusableRecords) {
              t.equal(batch.discardedUnusableRecords.length, 0, `discardedUnusableRecords must have ${0} discarded unusable records`);
            }

            if (batch.handledIncompleteMessages || !batch.handleIncompleteMessagesError) {
              t.fail(`handleIncompleteMessages must fail with ${batch.handleIncompleteMessagesError}`);
            }
            t.equal(batch.handleIncompleteMessagesError, fatalError, `handleIncompleteMessages must fail with ${fatalError}`);

            if (!batch.discardedRejectedMessages || batch.discardRejectedMessagesError) {
              t.fail(`discardRejectedMessage must not fail with ${batch.discardRejectedMessagesError}`);
            }
            if (batch.discardedRejectedMessages) {
              t.equal(batch.discardedRejectedMessages.length, 0, `discardedRejectedMessages must have ${0} discarded rejected messages`);
            }

            t.end();
          });

        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});
