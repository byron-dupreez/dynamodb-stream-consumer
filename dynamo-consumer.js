'use strict';

// AWS core utilities
const stages = require('aws-core-utils/stages');

const copying = require('core-functions/copying');
const copy = copying.copy;

const Strings = require('core-functions/strings');
const isNotBlank = Strings.isNotBlank;

// Task utilities
const taskUtils = require('task-utils');

const logging = require('logging-utils');
const LogLevel = logging.LogLevel;
const log = logging.log;

const dynamoProcessing = require('./dynamo-processing');
const streamConsumer = require('aws-stream-consumer-core/stream-consumer');
const streamProcessing = require('aws-stream-consumer-core/stream-processing');

/**
 * Utilities and functions to be used to robustly consume messages from an AWS DynamoDB event stream.
 * @module dynamodb-stream-consumer/dynamo-consumer
 * @author Byron du Preez
 */
exports._$_ = '_$_'; //IDE workaround

// Configuration
exports.isStreamConsumerConfigured = isStreamConsumerConfigured;
exports.configureStreamConsumer = configureStreamConsumer;
exports.generateHandlerFunction = generateHandlerFunction;

// Processing
exports.processStreamEvent = processStreamEvent;


// =====================================================================================================================
// DynamoDB stream consumer configuration - configures the runtime settings for a DynamoDB stream consumer on a given
// context from a given AWS event and AWS context
// =====================================================================================================================

/**
 * Returns true if the DynamoDB stream consumer's dependencies and runtime settings have been configured on the given
 * context; otherwise returns false.
 * @param {StreamConsumerContext|StreamProcessing|Object} context - the context to check
 * @returns {boolean} true if configured; false otherwise
 */
function isStreamConsumerConfigured(context) {
  return !!context && logging.isLoggingConfigured(context) && stages.isStageHandlingConfigured(context) &&
    context.region && context.stage && context.awsContext && context.taskFactory &&
    dynamoProcessing.isStreamProcessingConfigured(context);
}

/**
 * Configures the dependencies and settings for the DynamoDB stream consumer on the given context from the given settings,
 * the given options, the given AWS event and the given AWS context in preparation for processing of a batch of DynamoDB
 * stream records. Any error thrown must subsequently trigger a replay of all the records in the current batch until the
 * Lambda can be fixed.
 *
 * Note that if either the given event or AWS context are undefined, then everything other than the event, AWS context &
 * stage will be configured. This missing configuration must be configured before invoking processStreamEvent by invoking
 * {@linkcode module:aws-core-utils/contexts#configureEventAwsContextAndStage}. This separation of configuration is
 * primarily useful for unit testing.
 *
 * @param {Object|StreamConsumerContext|StreamProcessing|StandardContext} context - the context onto which to configure stream consumer settings
 * @param {StreamConsumerSettings|undefined} [settings] - optional stream consumer settings to use
 * @param {StreamConsumerOptions|undefined} [options] - optional stream consumer options to use
 * @param {AWSEvent|undefined} [event] - the AWS event, which was passed to your lambda
 * @param {AWSContext|undefined} [awsContext] - the AWS context, which was passed to your lambda
 * @return {StreamConsumerContext|StreamProcessing} the given context object configured with full or partial stream consumer settings
 * @throws {Error} an error if event and awsContext are specified and the region and/or stage cannot be resolved or if
 * a task factory cannot be constructed from the given `settings.taskFactorySettings`
 */
function configureStreamConsumer(context, settings, options, event, awsContext) {
  // Configure stream processing and all of the standard settings (including logging, stage handling, etc)
  dynamoProcessing.configureStreamProcessing(context, settings ? settings.streamProcessingSettings : undefined,
    options ? options.streamProcessingOptions : undefined, settings, options, event, awsContext, false);

  // Configure the task factory to be used to create tasks (NB: must be configured after configuring logging above, so
  // that the context can be used as the task factory's logger)
  const taskFactorySettings = settings && typeof settings.taskFactorySettings === 'object' ?
    copy(settings.taskFactorySettings, {deep: false}) : {};

  if (!taskFactorySettings.logger || !logging.isMinimumViableLogger(taskFactorySettings.logger)) {
    taskFactorySettings.logger = context;
  }
  taskUtils.configureTaskFactory(context, taskFactorySettings, options ? options.taskFactoryOptions : undefined);

  return context;
}

/**
 * Generates a handler function for your DynamoDB stream consumer Lambda.
 *
 * @param {undefined|function(): (Object|StreamConsumerContext|StreamProcessing|StandardContext)} [createContext] - a optional function that will be used to generate the initial context to be configured & used
 * @param {undefined|StreamConsumerSettings|function(): StreamConsumerSettings} [createSettings] - an optional function that will be used to generate initial stream consumer settings to use; OR optional module-scoped stream consumer settings from which to copy initial stream consumer settings to use
 * @param {undefined|StreamConsumerOptions|function(): StreamConsumerOptions} [createOptions] - an optional function that will be used to generate initial stream consumer options to use; OR optional module-scoped stream consumer options from which to copy initial stream consumer options to use
 * @param {undefined|function(): ProcessOneTaskDef[]} [defineProcessOneTasks] - an "optional" function that must generate a new list of "processOne" task definitions, which will be subsequently used to generate the tasks to be executed on each message independently
 * @param {undefined|function(): ProcessAllTaskDef[]} [defineProcessAllTasks] - an "optional" function that must generate a new list of "processAll" task definitions, which will be subsequently used to generate the tasks to be executed on all of the event's messages collectively
 * @param {LogLevel|string|undefined} [logEventResultAtLogLevel] - an optional log level at which to log the AWS stream event
 * and result; if log level is undefined or invalid, then logs neither
 * @param {string|undefined} [failureMsg] - an optional message to log at error level on failure
 * @param {string|undefined} [successMsg] an optional message to log at info level on success
 * @returns {AwsLambdaHandlerFunction} a handler function for your stream consumer Lambda
 */
function generateHandlerFunction(createContext, createSettings, createOptions, defineProcessOneTasks,
  defineProcessAllTasks, logEventResultAtLogLevel, failureMsg, successMsg) {

  /**
   * A stream consumer Lambda handler function.
   * @param {AWSEvent} event - the AWS stream event passed to your handler
   * @param {AWSContext} awsContext - the AWS context passed to your handler
   * @param {Callback} callback - the AWS Lambda callback function passed to your handler
   */
  function handler(event, awsContext, callback) {
    let context = undefined;
    try {
      // Configure the context as a stream consumer context
      context = typeof createContext === 'function' ? createContext() : {};

      const deep = {deep: true};
      const settings = typeof createSettings === 'function' ? copy(createSettings(), deep) :
        createSettings && typeof createSettings === 'object' ? copy(createSettings, deep) : undefined;

      const options = typeof createOptions === 'function' ? copy(createOptions(), deep) :
        createOptions && typeof createOptions === 'object' ? copy(createOptions, deep) : undefined;

      context = configureStreamConsumer(context, settings, options, event, awsContext);

      // Optionally log the event at the given log level
      if (logEventResultAtLogLevel && logging.isValidLogLevel(logEventResultAtLogLevel)) {
        context.log(logEventResultAtLogLevel, 'Event:', JSON.stringify(event));
      }

      // Generate the "process one" and/or "process all" task definitions using the given functions
      const processOneTaskDefs = typeof defineProcessOneTasks === 'function' ? defineProcessOneTasks() : [];
      const processAllTaskDefs = typeof defineProcessAllTasks === 'function' ? defineProcessAllTasks() : [];

      // Process the stream event with the generated "process one" and/or "process all" task definitions
      processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context)
        .then(result => {
          // Optionally log the result at the given log level
          if (logEventResultAtLogLevel && logging.isValidLogLevel(logEventResultAtLogLevel)) {
            context.log(logEventResultAtLogLevel, 'Result:', JSON.stringify(result));
          }
          // Log the given success message (if any)
          if (isNotBlank(successMsg)) context.info(successMsg);

          // Succeed the Lambda callback
          callback(null, result);
        })
        .catch(err => {
          // Log the error encountered
          context.error(isNotBlank(failureMsg) ? failureMsg : 'Failed to process stream event', err);
          // Fail the Lambda callback
          callback(err, null);
        });

    } catch (err) {
      // Log the error encountered
      log(context, LogLevel.ERROR, isNotBlank(failureMsg) ? failureMsg : 'Failed to process stream event', err);
      // Fail the Lambda callback
      callback(err, null);
    }
  }

  return handler;
}

// =====================================================================================================================
// Process stream event
// =====================================================================================================================

/**
 * Processes the given DynamoDB stream event using the given AWS context and context by applying each of the tasks
 * defined by the task definitions in the given processOneTaskDefs and processAllTaskDefs to each message extracted from
 * the event.
 *
 * @param {DynamoDBEvent|*} event - the AWS DynamoDB stream event (or any other garbage passed as an event)
 * @param {ProcessOneTaskDef[]|undefined} [processOneTaskDefsOrNone] - an "optional" list of "processOne" task definitions that
 * will be used to generate the tasks to be executed on each message independently
 * @param {ProcessAllTaskDef[]|undefined} [processAllTaskDefsOrNone] - an "optional" list of "processAll" task definitions that
 * will be used to generate the tasks to be executed on all of the event's messages collectively
 * @param {StreamConsumerContext} context - the context to use with DynamoDB stream consumer configuration
 * @returns {Promise.<Batch|BatchError>} a promise that will resolve with the batch processed or reject with an error
 */
function processStreamEvent(event, processOneTaskDefsOrNone, processAllTaskDefsOrNone, context) {
  // Ensure that the stream consumer is fully configured before proceeding, and if not, trigger a replay of all the
  // records until it can be fixed
  if (!isStreamConsumerConfigured(context)) {
    const errMsg = `FATAL - Your DynamoDB stream consumer MUST be configured before invoking processStreamEvent (see dynamo-consumer#configureStreamConsumer & dynamo-processing). Fix your Lambda and redeploy ASAP, since this issue is blocking all of your stream's shards!`;
    log(context, LogLevel.ERROR, errMsg);
    return Promise.reject(new Error(errMsg));
  }
  return streamConsumer.processStreamEvent(event, processOneTaskDefsOrNone, processAllTaskDefsOrNone, context);
}