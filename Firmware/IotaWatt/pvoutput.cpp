#include "pvoutput.h"
#include <time.h>
#include <assert.h>

#define ENABLE_HTTP_DEBUG false
static const String PVOUTPUT_POST_DATA_PREFIX = "c1=0&n=0&data=";

// Extern vars
bool         pvoutputStarted = false;
bool         pvoutputStop = false;
bool         pvoutputRestart = true;
const char*  pvoutputApiKey = nullptr;
int          pvoutputSystemId = 0;
int          pvoutputMainsChannel = 0;
int          pvoutputSolarChannel = 0;
unsigned int pvoutputHTTPTimeout = 2000;

// @todo Looks like pvoutput quantizes to 5min intervals. We need 5 min quantization but we also want to configure
// if the client posts every 5 min, 15 mint or other multiple of 5. Right now assuming all the same
uint32_t     pvoutputReportInterval = 5*60; // Interval (sec) to invoke pvoutput
//ScriptSet*   pvoutputOutputs = nullptr;

// private vars
int32_t   pvoutputConfigRevision = -1;                      // Revision control for dynamic config

bool pvoutputConfig(const char* configObj) {
  trace(T_pvoutputConfig,0);
  DynamicJsonBuffer Json;
  JsonObject& config = Json.parseObject(configObj);
  trace(T_pvoutputConfig,1);
  if(!config.success()) {
    log("pvoutput: Json parse failed.");
    return false;
  }

  int revision = config["revision"];
  if(revision == pvoutputConfigRevision){
    return true;
  }

  trace(T_pvoutputConfig,2);
  pvoutputConfigRevision = revision;
  pvoutputStop = false;
  if(config["stop"].as<bool>()){
    trace(T_pvoutputConfig,3);
    pvoutputStop = true;
  }
  else if(pvoutputStarted){
    trace(T_pvoutputConfig,4);
    pvoutputRestart = true;
  }
  trace(T_pvoutputConfig,5);

  pvoutputSystemId = config["systemId"].as<int>();
  pvoutputMainsChannel = config["mainsChannel"].as<int>();
  pvoutputSolarChannel = config["solarChannel"].as<int>();
  pvoutputHTTPTimeout = config["httpTimeout"].as<unsigned int>();
  pvoutputReportInterval = config["reportInterval"].as<int>();

  delete[] pvoutputApiKey;
  pvoutputApiKey = charstar(config["apiKey"].as<const char*>());

  if(!pvoutputStarted) {
    trace(T_pvoutputConfig,6);
    NewService(pvoutputService);
    pvoutputStarted = true;
  }

  trace(T_pvoutputConfig,7);
  log("Loaded PVOutput config using: systemID:%d, mainChannel:%d, solarChannel:%d, HTTPTimeout:%d, interval:%d, ApiKey:<private>", pvoutputSystemId, pvoutputMainsChannel, pvoutputSolarChannel, pvoutputHTTPTimeout, pvoutputReportInterval);
  return true;
}


const char* ParseFixedInteger(char* tmp, const char* src, size_t size, int* value, int min, int max)
{
  if (src == nullptr)
    return src;

  strncpy(tmp, src, size);
  tmp[size] = 0;
  if (sscanf(tmp, "%u", value) != 1)
  {
    return nullptr;
  }

  if (*value < min || *value > max)
  {
    return nullptr;
  }

  return src + size;
}

const char* ParseExpectedCharacter(const char* src, char expected)
{
  if (src == nullptr)
    return nullptr;

  if (*src != expected)
    return nullptr;

  return src + 1;
}

// @todo These dont really belong here
static double energyConsumed = 0.0;
static double powerConsumed = 0.0;
static double energyGenerated = 0.0;
static double powerGenerated = 0.0;
static double voltage = 0.0;

static void SetNextPOSTTime(uint32_t* UnixLastPost, uint32_t* UnixNextPost, uint32_t pvoutputReportInterval,
  IotaLogRecord* dayStartLogRecord, IotaLogRecord* logRecord, IotaLogRecord* prevPostLogRecord);
static String DateTimeToString(DateTime dt);

static uint32_t pvoutputSendData(xbuf& reqData, bool isResend, uint32_t unixTime, double voltage, double energyConsumed, double powerConsumed,
  double energyGenerated, double powerGenerated)
  {
    //log("Send data");
  // This sends data to PVOutput using API: https://pvoutput.org/help.html#api-addbatchstatus
  trace(T_pvoutput, 32);

  // PVOutput expects reports as positive values, our internal calculations
  // expect negative values for generation so just convert them now
  energyGenerated *= -1;
  powerGenerated *= -1;

  // PVOutput requires data no older than 14 days, will only do 13 days
  //uint32_t thirteenDaysAgo = UNIXtime() - (13U * 24U * 60U * 60U);
  //if (unixTime < thirteenDaysAgo) {
  //  msgLog("pvoutput: Post for old data: " + String(unixTime) + " and PVOutput only accepts data upto 14 days old (we are limiting to 13 days): " + String(thirteenDaysAgo));
  //  return 0;
  //}

  // PVOutput requires localized time
  uint32_t localUnixTime = unixTime + (localTimeDiff * 3600);
  DateTime dt(localUnixTime);

  // Now sanity check the data so we dont get PVOutput infinite POST loop
  // due to known problems
  if(energyGenerated < 0.0) {
    trace(T_pvoutput, 33);
    //msgLog("pvoutput: energyGenerated: " + String(energyGenerated) + "Wh is invalid. Are the CT coils installed in the correct direction? PVOutput wont accept negative values force changing it to 0.0");
    energyGenerated = 0.0;
  }

  if(powerGenerated < 0.0) {
    trace(T_pvoutput, 34);
    //msgLog("pvoutput: powerGenerated: " + String(powerGenerated) + "W is invalid. Are the CT coils installed in the correct direction? PVOutput wont accept negative values force changing it to 0.0");
    powerGenerated = 0.0;
  }

  if(energyConsumed < 0.0) {
    trace(T_pvoutput, 35);
    //msgLog("pvoutput: energyConsumed: " + String(energyConsumed) + "Wh is invalid. Are the CT coils installed in the correct direction? PVOutput wont accept negative values force changing it to 0.0");
    energyConsumed = 0.0;
  }

  if(powerConsumed < 0.0) {
    trace(T_pvoutput, 36);
    //msgLog("pvoutput: powerConsumed: " + String(powerConsumed) + "W is invalid. Are the CT coils installed in the correct direction? PVOutput wont accept negative values force changing it to 0.0");
    powerConsumed = 0.0;
  }

  char dateStr[10];
  snprintf(dateStr, sizeof(dateStr), "%04u%02u%02u", dt.year(), dt.month(), dt.day());
  dateStr[sizeof(dateStr) - 1] = 0;

  char timeStr[10];
  snprintf(timeStr, sizeof(timeStr), "%02u:%02u", dt.hour(), dt.minute());
  timeStr[sizeof(timeStr) - 1] = 0;




//Date	Yes	yyyymmdd	date	20100830	r1	
//Time	Yes	hh:mm	time	14:12	r1	
//Energy Generation	Yes	number	watt hours	10000	r1	
//Power Generation	No	number	watts	2000	r1	
//Energy Consumption	No	number	watt hours	10000	r1	
//Power Consumption	No	number	watts	2000	r1	
//Temperature	No	decimal	celsius	23.4	r1	
//Voltage	No	decimal	volts	240.7	r1	

  //String path = String("/service/r2/addstatus.jsp") + "?d=" + String(dateStr)
  //  + "&t=" + String(timeStr) + "&v1=" + String(energyGenerated)
  //  + "&v2=" + String(powerGenerated) + "&v3=" + String(energyConsumed) + "&v4="
  //  + String(powerConsumed)
  //  //+ "&v5=" + String(temperature)
  //  + "&v6=" + String(voltage) + "&c1=0" // If 1 indicates cumuliative not reset of energy each day
  //  + "&n=0"                             // If 1 indicates net import/export not gross, currently calc gross in caller
  //  ;
  String entry = String(dateStr) 
    + "," + String(timeStr) 
    + "," + String(energyGenerated) 
    + "," + String(powerGenerated) 
    + "," + String(energyConsumed) 
    + "," + String(powerConsumed) 
    + "," // temperature
    + "," + String(voltage);

  if (reqData.available() > PVOUTPUT_POST_DATA_PREFIX.length())
  {
    reqData.write(';');
  }
  reqData.write(entry);
  log("Add entry: %s", entry.c_str());

  //msgLog("pvoutput: Posting for time: " + String(unixTime) + " path: " + "pvoutput.org:80" + path
  //  + " With key: " + String(pvoutputApiKey) + " and systemId: " + String(pvoutputSystemId));
  //http.begin("pvoutput.org", 80, path);
  //http.addHeader("Host", "pvoutput.org");
  //http.addHeader("Content-Type", "application/x-www-form-urlencoded");
  //http.addHeader("X-Pvoutput-Apikey", pvoutputApiKey);
  //http.addHeader("X-Pvoutput-SystemId", String(pvoutputSystemId));
  //http.setTimeout(pvoutputHTTPTimeout);

  //unsigned long start = micros();
  //int httpCode = http.GET();
  //unsigned long end = micros();
  //String response = http.getString();
  //http.end();
  //if(httpCode != HTTP_CODE_OK && httpCode != 204) {
  //  trace(T_pvoutput, 37);
  //  String code = String(httpCode);
  //  if(httpCode < 0) {
  //    code = http.errorToString(httpCode);
  //  }

  //  msgLog("pvoutput: POST FAILED code: " + String(httpCode) + " message: " + code + " response: " + response);
  //  if (httpCode == 403 && response.indexOf("Exceeded") >= 0 && response.indexOf("requests per hour") >= 0) {
  //    // Documentation says to wait for an hour before making further requests
  //    // https://pvoutput.org/help.html#api-errors
  //    msgLog("pvoutput: Waiting for an hour to try again in accordance if PVOutput API docs");
  //    return 60 * 60;
  //  }
  //  else if (isResend) {
  //    return 60;
  //  }
  //  else {
  //    return 10;
  //  }
  //}
  //else {
  //  trace(T_pvoutput, 38);
  //  msgLog("pvoutput: POST success code: " + String(httpCode) + " response: " + response);
  //}

  return 0;

  }


static bool PVOutputBuildPost(xbuf& reqData, IotaLogRecord* oldRecord, IotaLogRecord* logRecord, IotaLogRecord* dayStartLogRecord)
{
  //log("Build post");
  // Adjust the posting time to match the log entry time (at modulo we care about)
  // @todo this is wrong. Probably should be from service member
  static uint32_t reqUnixtime = 0;              // First measurement in current reqData buffer
      reqUnixtime = logRecord->UNIXtime - (logRecord->UNIXtime % pvoutputReportInterval);
      trace(T_pvoutput, 20);

      // The measurements should be such that:
      // chan 1 : mains +ve indicates net import -ve indicates net export
      // chan 2 : solar -ve indicates generation +ve should never really happen would indicate solar panels using power

      // Find the mean voltage since last post
      int voltageChannel = -1;
      if(pvoutputMainsChannel >= 0) {
        voltageChannel = inputChannel[pvoutputMainsChannel]->_vchannel;
      }
      else if(pvoutputSolarChannel >= 0) {
        voltageChannel = inputChannel[pvoutputSolarChannel]->_vchannel;
      }

      voltage = 0;
      if(voltageChannel >= 0 && logRecord->logHours != oldRecord->logHours) {
        trace(T_pvoutput, 21);
        voltage = (logRecord->accum1[voltageChannel] - oldRecord->accum1[voltageChannel]);
        voltage /= (logRecord->logHours - oldRecord->logHours);
      }

      // Energy is calculated since begining of the day
      //
      // Generated energy is always a negative value in our calculations.
      // I.e. I assume most power channels in existing iotawatt indicate
      // power usage of that channel and are positive. In fact many have
      // enabled ability to force them positive.
      //
      // By using a negative value to indicate export/generation we have
      // a more consitent view of things
      //
      // Because a solar channel always generates and never uses power, we
      // will enforce it has a negative value in the case the CT has been
      // installed in reverse. This will not be considered a failure in
      // the configuration we will just cope with it.
      energyGenerated = 0;
      if(pvoutputSolarChannel >= 0) {
        trace(T_pvoutput, 22);
        energyGenerated
          = logRecord->accum1[pvoutputSolarChannel] - dayStartLogRecord->accum1[pvoutputSolarChannel];
        if(energyGenerated > 0) energyGenerated *= -1;
      }

      // Find out how much energy we imported from the main line
      double energyImported = 0.0;
      if(pvoutputMainsChannel >= 0) {
        trace(T_pvoutput, 23);
        energyImported
          = logRecord->accum1[pvoutputMainsChannel] - dayStartLogRecord->accum1[pvoutputMainsChannel];
      }

      // Example:
      // generated = -5kWh
      // imported = 2kWh
      // thus we are consuming 7kWh as we are using all the 5kW and an additional 2kW from mains import
      energyConsumed = energyImported - energyGenerated;

      // the mean power used in W since the last post
      powerGenerated = 0;
      if(pvoutputSolarChannel >= 0 && logRecord->logHours != oldRecord->logHours) {
        trace(T_pvoutput, 24);
        powerGenerated
          = logRecord->accum1[pvoutputSolarChannel] - oldRecord->accum1[pvoutputSolarChannel];
        if(powerGenerated > 0) powerGenerated *= -1;
        powerGenerated = powerGenerated / (logRecord->logHours - oldRecord->logHours);
      }

      // Find out how much energy we imported from the main line
      double powerImported = 0.0;
      if(pvoutputMainsChannel >= 0 && logRecord->logHours != oldRecord->logHours) {
        trace(T_pvoutput, 25);
        powerImported
          = logRecord->accum1[pvoutputMainsChannel] - oldRecord->accum1[pvoutputMainsChannel];
        powerImported = powerImported / (logRecord->logHours - oldRecord->logHours);
      }

      // Example:
      // generated = -5kWh
      // imported = 2kWh
      // thus we are consuming 7kWh as we are using all the 5kW and an additional 2kW from mains import
      powerConsumed = powerImported - powerGenerated;

      // If we are exporting more than we are generating something is wrong
      if(powerImported < powerGenerated) {
        trace(T_pvoutput, 26);
        //log(
        //  "pvoutput: PVOutput configuration is incorrect. Appears we are exporting more power than we are generating. Between " + String(logRecord->UNIXtime) + " and " + String(oldRecord->UNIXtime) 
        //  + " we imported: " + String(powerImported) + "Wh and generated: " + String(powerGenerated) + "Wh");
        log("pvoutput: Config appears incorrect");
      }

      //uint32_t failureWaitDuration = 
      pvoutputSendData(reqData, false, reqUnixtime, voltage, energyConsumed, powerConsumed, energyGenerated, powerGenerated);
      //if(failureWaitDuration > 0) {
      //  trace(T_pvoutput, 30);
      //  msgLog("pvoutput: Pushing data to PVOutput failed, trying again in " + String(failureWaitDuration) + " sec");
      //  return UNIXtime() + failureWaitDuration;
      //}

  // @todo Writeme
  // Basically looking at diff between oldRecord and logRecord and figuring out the power generated/used etc
  // then writing the record to reqData
  return true;
}

enum class ErrorOperation
{
  RETRY_NOW,
  RETRY_PERIOD,
  SKIP,
  RESET
  // pvoutputStop Probably have a state for stpping the state machine completely
};

static ErrorOperation InterpretAddBatchStatusError(int responseCode, const String& responseText)
{
          // Most errors we will just never recover from. If they are bugs we will need to fix them
          // If they are config errors then user needs to fix something
          // 
          // Only some errors like old requests will be "skipped"
          if (responseCode == 400)
          {
            log("pvoutput: Got status 400");
            if (strstr(responseText.c_str(), "Date is older than") != nullptr)
            {
              log("pvoutput: Got msg date is older");
              // Skip this request
              // @todo Really want to skip until 14 days ago. Maybe RESET is better?
              return ErrorOperation::SKIP;
            }
            else if (strstr(responseText.c_str(), "Date is in the future") != nullptr)
            {
              log("pvoutput: Got msg date is future");
              // Wait for a while, probably local clock and pvoutput not in sync
              return ErrorOperation::RETRY_PERIOD;
            }
              log("pvoutput: Unknown 400 message: %s", responseText.c_str());
          }
          else if (responseCode == 403)
          {
            if (strstr(responseText.c_str(), "Exceeded 60 requests per hour") != nullptr)
            {
              // Wait for a while
              log("pvoutput: Got msg exceede reqs");
              return ErrorOperation::RETRY_PERIOD;
            }
              log("pvoutput: Unknown 403 message: %s", responseText.c_str());
          }
          else
          {
              log("pvoutput: Got unknown code: %d", responseCode);
          }

          // All other errors we will just reset and hope it fixes.
          // If in a infninite loop then there is most likely a bug we will get
          // an error report. This is better than silently failing and SKIP
          return ErrorOperation::RESET;


            // Errors we can recover from with time: Action: Wait and try again
            // * Forbidden 403: Exceeded number requests per hour	
            // * Bad request 400: Date is in the future [date]	

            // Errors with user config, can resolve but need updated config
            // * Unauthorized 401: Invalid System ID	
            // * Unauthorized 401: Invalid API Key	
            // * Unauthorized 401: Disabled API Key	
            // * Forbidden 403: Read only key	
            // * Unauthorized 401: Missing, invalid or inactive api key information (X-Pvoutput-Apikey)	
            
            // Errors that are bugs in this request : Action: Skip this post
            // * Bad request 400: No statuses added or updated	
            // * Bad request 400: Date is too far in the past [date]	
            // * Bad request 400: Energy value [energy_current] lower than previously recorded value: [energy_previous]	
            // * Bad request 400: Value number cannot exceed 250,000Wh [value]	
            // * Bad request 400: Energy value [energy] too high for [time]	
            // * Bad Request 400: Invalid net and cumulative	

            // Error either bad config or bug that we skip:
            // * Bad request 400: Energy value [energy] too high for system size [system_size]	
            // * Bad request 400: Power value [power] too high for system size [system_size]	
            // * Bad request 400: Moon Powered	

            // Errors that are bugs cant recover from : 
            // * Method Not Allowed 405: POST or GET only	

            // May be able to skip OR never recover from:
            // * Bad request 400: Could not read [field]	
            // * Bad Request 400: Missing net power value	

            // Other errors (this one is probably a bug, but could be resolved by donating)
            // * Forbidden 403: Donation Mode	

            // All restrictions and limitations of the addstatus service.


            // A maximum of 30 statuses can be sent in a single batch request.
            // An error is only returned where the entire batch fails to update any data
            // An addoutput is called once for the last successful status update in the batch.
            // The date parameter must be not be older than 14 days from the current date.
            // All net statuses in the batch must have the same date.
            // A net status must have export and import data, 0W should be sent if no export/import is to be recorded.

}

// Number of entries to send in a single request
// @todo the limit for this is : A maximum of 30 statuses can be sent in a single batch request.
// Unless paid account which permits 100 entries

// @todo Add limit check for config to MAX_BULK_SEND
const uint16_t MAX_BULK_SEND = 30;
uint16_t  pvoutputBulkSend = 1;

//uint16_t  pvoutputPort = 8086;
//int32_t   pvoutputRevision = -1;                      // Revision control for dynamic config
//uint32_t  pvoutputBeginPosting = 0;                   // Begin date specified in config
//char*     pvoutputUser = nullptr;
//char*     pvoutputPwd = nullptr; 
//char*     pvoutputRetention = nullptr;
//char*     pvoutputMeasurement = nullptr;
//char*     pvoutputFieldKey = nullptr; 
//pvoutputTag* pvoutputTagSet = nullptr;  
//ScriptSet* pvoutputOutputs;      
//String    pvoutputURL = "";
//String    pvoutputDataBase = "";

uint32_t pvoutputService(struct serviceBlock* _serviceBlock){

      // This is a standard IoTaWatt Service operating as a state machine.

  enum   states {initialize,        // Basic startup of the service - one time
                 queryLastPostTime, // Setup to query for last post time of each measurement
                 queryLastPostTimeWait,     // wait for [async] query to complete
                 //getLastRecord,     // Read the logRec and prep the context for logging
                 post,              // Add a measurement to the reqData xbuf
                 sendPost,          // Send the accumulated measurements
                 waitPost};         // Wait for the [async] post to complete

  static states state = initialize;
  static IotaLogRecord* logRecord = nullptr;
  static IotaLogRecord* oldRecord = nullptr;
  static uint32_t lastRequestTime = 0;          // Time of last measurement in last or current request
  static uint32_t lastBufferTime = 0;           // Time of last measurement reqData buffer
  uint32_t pvoutputLastPost = 0;                // Time of last measurement acknowledged by pvoutput
  static uint32_t UnixNextPost = UNIXtime();    // Next measurement to be posted
  static xbuf reqData;                          // Current request buffer
  static uint32_t reqUnixtime = 0;              // First measurement in current reqData buffer
  static int  reqEntries = 0;                   // Number of measurement intervals in current reqData
  static int16_t retryCount = 0;                // HTTP error count
  static asyncHTTPrequest* request = nullptr;   // -> instance of asyncHTTPrequest
  static uint32_t postFirstTime = UNIXtime();   // First measurement in outstanding post request
  static uint32_t postLastTime = UNIXtime();    // Last measurement in outstanding post request
  static size_t reqDataLimit = 4000;            // transaction yellow light size

  // @todo Allocate on init
static IotaLogRecord* dayStartLogRecord = new IotaLogRecord;


  trace(T_pvoutput,0);                            // Announce entry

          // If restart, set to reinitialize. 

  if(pvoutputRestart){
    trace(T_pvoutput,1);
    state = initialize;
    pvoutputRestart = false;
  }
      
          // Handle current state

  switch(state){

    case initialize: {
      trace(T_pvoutput,2);

          // We post from the log, so wait if not available.          

      if(!currLog.isOpen()){                  
        return UNIXtime() + 5;
      }
      log("pvoutput: started."); 
      state = queryLastPostTime;
      _serviceBlock->priority = priorityLow;
      return 1;
    }
 
    case queryLastPostTime:{
      trace(T_pvoutput,3);
      pvoutputLastPost = 0; // @todo influx makes this configurable pvoutputBeginPosting;

      trace(T_pvoutput,4);

          // Make sure wifi is connected and there is a resource available.

      if( ! WiFi.isConnected() || ! HTTPrequestFree){
        return UNIXtime() + 1;
      }
      HTTPrequestFree--;

          // Create a new request

      if(request){
        delete request;
      }
      request = new asyncHTTPrequest;
	  
	  // curl -H "X-Pvoutput-Apikey: 25d765379054a57fdb129269344ebfdb97436082" -H "X-Pvoutput-SystemId: 54871" "https://pvoutput.org/service/r2/getstatus.jsp"
	  // https://pvoutput.org/help.html#api-getstatus
	  // https://pvoutput.org/service/r2/getstatus.jsp
      request->setTimeout(pvoutputHTTPTimeout);
      request->setDebug(ENABLE_HTTP_DEBUG);
      // @todo Seems need uppercase HTTP in async req
      request->open("POST", "HTTP://pvoutput.org/service/r2/getstatus.jsp");
      request->setReqHeader("Host", "pvoutput.org"); 
      request->setReqHeader("Content-Type", "application/x-www-form-urlencoded"); 
      request->setReqHeader("X-Pvoutput-Apikey", pvoutputApiKey); 
      request->setReqHeader("X-Pvoutput-SystemId", String(pvoutputSystemId).c_str()); 
      trace(T_pvoutput,4);
      reqData.flush();

      // Send the request
      log("Sending POST request: curl -H \"X-Pvoutput-Apikey: %s\" -H \"X-Pvoutput-SystemId: %u\" \"http://pvoutput.org/service/r2/getstatus.jsp\"", pvoutputApiKey, pvoutputSystemId);
      if (!request->send(&reqData, reqData.available()))
      {
        log("Send failed");
      }
      trace(T_pvoutput,4);
      state = queryLastPostTimeWait;
      return 1;
    }

    case queryLastPostTimeWait: {

          // If not completed, return to wait.

      trace(T_pvoutput,5); 
      if(request->readyState() != 4){
        return 1; 
      }
      HTTPrequestFree++;
      String responseText = request->responseText();
      int responseCode = request->responseHTTPcode();
      delete request;
      request = nullptr;
      if(responseCode != 200){
        log("pvoutput: last entry query failed: %d : %s", responseCode, responseText.c_str());
        // @todo test case no data at all in pvoutput and handle specially. Dont know what message that has yet.


        // @todo Do we have a special interpretation for this one too? Maybe instead of op interpret, just returns an enum of all cases we know about that are important so easier to switch case on instead of string compares
          ErrorOperation op = InterpretAddBatchStatusError(responseCode, responseText);
          switch (op)
          {
          case ErrorOperation::RETRY_PERIOD: 
            state = queryLastPostTime;

            // @todo Add back in after testing
            //log("pvoutput: Retrying post again in %u seconds", pvoutputReportInterval);
            //return UNIXtime() + pvoutputReportInterval;
            log("pvoutput: Retrying post again in %u seconds", 5);
            return UNIXtime() + 5;

          case ErrorOperation::SKIP: 
            // Treats just like a success so fall through to that code
            // Will just assume now is the last time
            // @todo Is this something we ever want to do?
            log("pvoutput: Post is being skipped");
            break;

          case ErrorOperation::RESET: 
          case ErrorOperation::RETRY_NOW: 
          default:
            state = queryLastPostTime;
            log("pvoutput: Retrying post again in 1 second");
            return UNIXtime() + 1;
          }

		    // @todo Probably no data at all in pvoutput, start 14 days ago?
        //pvoutputStop = true;
        //state = post;

        // try again
        //state = queryLastPostTime;
        //return 1;
      } 
      
	  // Returns something like: 
	  // 20180607,03:30,223,125,334,322,0.022,24.5,242.0
	  // Following https://pvoutput.org/help.html#api-getstatus
	  // 0: Date, yyyymmdd, date
	  // 1: Time, hh:mm, time
	  // 2: Energy Generation, number, watt hours
	  // Power Generation, number, watt
	  // Energy Consumption, number, watt hours,
	  // Power Consumption, number, watt
	  // Normalised Output, number, kW/kW
	  // Temperature, decimal, celsius
	  // Voltage, decimal, volts
	  // 
	  // We only really care about the date and time
	  // So we will parse the first two items and ignore the rest
	  // Also the first two fields are a fixed length format. So we will 
	  // ensure the data is available and then truncate the string to be 
	  // parsed by strptime
    //
    // YYYYMMDD,HH:MM,
    char tmp[5];
    const char* src = responseText.c_str();

		int year = 0;
		int month = 0;
		int day = 0;
		int hour = 0;
		int minute = 0;
    //log("Parsing year");
    src = ParseFixedInteger(tmp, src, 4, &year, 0, 9999);
    //log("Parsing month");
    src = ParseFixedInteger(tmp, src, 2, &month, 1, 12);
    //log("Parsing day");
    src = ParseFixedInteger(tmp, src, 2, &day, 1, 31);
    //log("Parsing comma");
    src = ParseExpectedCharacter(src, ',');
    //log("Parsing hour");
    src = ParseFixedInteger(tmp, src, 2, &hour, 0, 23);
    //log("Parsing :");
    src = ParseExpectedCharacter(src, ':');
    //log("Parsing min");
    src = ParseFixedInteger(tmp, src, 2, &minute, 0, 59);
   // log("Parsing comma");
    src = ParseExpectedCharacter(src, ',');
    if (src == nullptr)
    {
      // @todo what to do?
        log("pvoutput: invalid date and time returned from PVOutput: %s", responseText.c_str());
        pvoutputStop = true;
        state = post;
        return 1;
    }
    log("Parsed last status date/time: %u %u %u %u %u", year, month, day, hour, minute);

    DateTime dt(year, month, day, hour, minute, 0);

    // PVOutput reports in local time, all our times here are UTC
      pvoutputLastPost = dt.unixtime() - (localTimeDiff * 3600);
      if(pvoutputLastPost == 0){
        log("pvoutputLastPost is 0, something is wrong assuming that the last post was about now");
        pvoutputLastPost = UNIXtime();
      }

      // Max permitted time in the past for a POST is 14 days
      // We will set max to 13 days for now
      // @todo Move to top of file consts and rename to something better
      static const uint32_t MAX_PAST_POST_TIME = 13 * 24 * 60 * 60;
      if (pvoutputLastPost + MAX_PAST_POST_TIME < UNIXtime()) {
        log("pvoutputLastPost is too old, we are setting it to a time that will be accepted by PVOutput");
        pvoutputLastPost = UNIXtime() - MAX_PAST_POST_TIME;
      }

      // Adjust to a report interval boundary
      pvoutputLastPost -= pvoutputLastPost % pvoutputReportInterval;

      log("pvoutput: Start posting from %s", dateString(pvoutputLastPost + pvoutputReportInterval).c_str());
      /*
      state = getLastRecord;
      return 1;
    }

    case getLastRecord: {
      */
      trace(T_pvoutput,6);   
      if( ! oldRecord){
        oldRecord = new IotaLogRecord;
      }
      oldRecord->UNIXtime = pvoutputLastPost;      
      if (logReadKey(oldRecord) != 0)
      {
        log("pvoutput failed to read last post key from iota log with time: %d", pvoutputLastPost);
      }
      trace(T_pvoutput,6);

          // Assume that record was posted (not important).
          // Plan to start posting one interval later
      
      // @todo Not sure we need this adjust interval as should be multiple anyway
      assert((oldRecord->UNIXtime % pvoutputReportInterval) == 0);
      UnixNextPost = oldRecord->UNIXtime + pvoutputReportInterval - (oldRecord->UNIXtime % pvoutputReportInterval);
      
          // Advance state.

      // Start new reqData
      //log("Start new rec data");
      reqData.flush();
      reqEntries = 0;
      reqData.write(PVOUTPUT_POST_DATA_PREFIX);
      state = post;
      return UnixNextPost;
    }

    case post: {
      //log("post");
      trace(T_pvoutput,7);

          // If stop requested, do it now.

      if(pvoutputStop) {
      log("stop requested");
        if(request && request->readyState() < 4) return 1;
        trace(T_pvoutput,71);
        log("pvoutput: Stopped. Last post %s", dateString(pvoutputLastPost).c_str());
        pvoutputStarted = false;
        trace(T_pvoutput,72);    
        state = initialize;
        delete oldRecord;
        oldRecord = nullptr;
        delete logRecord;
        logRecord = nullptr;
        delete request;
        request = nullptr;
        reqData.flush();
        reqEntries = 0; // @todo I added this
        pvoutputConfigRevision = -1;
        return 0;
      }

      if(request && request->readyState() < 4 && reqData.available() > reqDataLimit){
      log("Not ready yet try again soon");
        return 1; 
      }  


      // For pvoutput we log energy accumulated each day, so we need to
      // read the last record seen the day before and use that as the "reference"
      // When the day ticks over, then we will update the reference
            //log("calc pre day");

        trace(T_pvoutput,8);
      uint32_t localUnixNextPost = UnixNextPost + (localTimeDiff * 3600);
      DateTime localUnixNextPostDt(localUnixNextPost);

      DateTime localUnixPrevPostDt(oldRecord->UNIXtime + (localTimeDiff * 3600));

        trace(T_pvoutput,9);
      DateTime previousDay(
        localUnixNextPostDt.year(), localUnixNextPostDt.month(), localUnixNextPostDt.day(), 23, 59, 59);
      previousDay = previousDay - TimeSpan(1, 00, 0, 0);
      // @todo Is this supposed to be + localtimediff?
      uint32_t unixPreviousDay = previousDay.unixtime() - (localTimeDiff * 3600);

      uint32_t lastKey = currLog.lastKey();
      DateTime lastKeyDt(lastKey + (localTimeDiff * 3600));

      uint32_t now = UNIXtime() + (localTimeDiff * 3600);
      DateTime nowDt(now);
      log("Local UnixNextPost: %s prev post: %s, prev day: %s, now: %s, lastKey: %s", DateTimeToString(localUnixNextPostDt).c_str(), DateTimeToString(localUnixPrevPostDt).c_str(), DateTimeToString(previousDay).c_str(), DateTimeToString(nowDt).c_str(), DateTimeToString(lastKeyDt).c_str());
      // Now we search backwards in the currLog to find the nearest key matching unixPreviousDay
         trace(T_pvoutput,10);


      // @todo day start only changes rarely. Lets not query it each time
     dayStartLogRecord->UNIXtime = unixPreviousDay;
     if (currLog.firstKey() == currLog.lastKey()){
       log("IoTaLog empty!");
     }
     if (currLog.firstKey() > currLog.lastKey()){
       log("IoTaLog empty and weird numbers!");
     }
     
      if(dayStartLogRecord->UNIXtime < currLog.firstKey()) {
         trace(T_pvoutput,11);
        dayStartLogRecord->UNIXtime = currLog.firstKey();
      }
      if(dayStartLogRecord->UNIXtime > currLog.lastKey()) {
         trace(T_pvoutput,12);
        dayStartLogRecord->UNIXtime = currLog.lastKey();
      }

            //log("find prev day log record");
        trace(T_pvoutput,11);
      int rkResult = 1;
      do {
        // @todo This would be more efficient with currLog supporting readPrev()
        // @todo try using searchKey()
        rkResult = currLog.readKey(dayStartLogRecord);
        if(rkResult != 0) {
          trace(T_pvoutput, 9);
          //msgLog("pvoutput: WARNING: Failed to read log record " + String(dayStartLogRecord->UNIXtime)
          //  + " going backwards until a key matches, next: " + String(dayStartLogRecord->UNIXtime - 1));
          dayStartLogRecord->UNIXtime -= 1;
        }
      } while(rkResult != 0 && dayStartLogRecord->UNIXtime > currLog.firstKey());
        trace(T_pvoutput,12);

            //log("Fix NaN and bad values");
      if (currLog.firstKey() >= currLog.lastKey()){
        dayStartLogRecord->UNIXtime = currLog.firstKey();
        dayStartLogRecord->logHours = 0.0;
        dayStartLogRecord->serial = 0;
        memset(dayStartLogRecord->accum1, 0, sizeof(dayStartLogRecord->accum1));
        memset(dayStartLogRecord->accum2, 0, sizeof(dayStartLogRecord->accum2));
      }

      // make sure the dayStartLogRecord details are valid
      for(int i = 0; i < maxInputs; i++) {
        // Check for NaN
        if(dayStartLogRecord->accum1[i] != dayStartLogRecord->accum1[i]) {
          trace(T_pvoutput, 10);
          dayStartLogRecord->accum1[i] = 0;
        }
      }
        trace(T_pvoutput,13);

      // Check for NaN
      if(dayStartLogRecord->logHours != dayStartLogRecord->logHours) {
        trace(T_pvoutput, 11);
        dayStartLogRecord->logHours = 0;
      }

        trace(T_pvoutput,14);

          // If buffer isn't full,
          // add another measurement.
            //log("check if need to add another measurement");

      if(reqData.available() < reqDataLimit && UnixNextPost <= currLog.lastKey()){  

            //log("yes add another");
        trace(T_pvoutput,15);
            // Read the next log record.

        // @todo not necessary
        if( ! logRecord){
          logRecord = new IotaLogRecord;            
        }
        trace(T_pvoutput,16);
        logRecord->UNIXtime = UnixNextPost;
        // @todo Need to work out places using currLog. and places using logReadKey
        logReadKey(logRecord);
        trace(T_pvoutput,17);
        
            // Compute the time difference between log entries.
            // If zero, don't bother.
            
        double elapsedHours = logRecord->logHours - oldRecord->logHours;
        if(elapsedHours == 0){
        trace(T_pvoutput,18);

          log("No difference in recorded time (IoTa wasnt running during that period). Wont bother posting skipping forward log by: %u sec", pvoutputReportInterval);
          UnixNextPost += pvoutputReportInterval;
          return UnixNextPost;  
        }
        trace(T_pvoutput,19);
        
        // Build the request string.
        if (!PVOutputBuildPost(reqData, oldRecord, logRecord, dayStartLogRecord))
        {
        trace(T_pvoutput,20);
          // @todo
          log("ERROR failed to produce post data for period");
          UnixNextPost += pvoutputReportInterval;
          return UnixNextPost;  
        }

        trace(T_pvoutput,21);
        delete oldRecord;
        oldRecord = logRecord;
        logRecord = nullptr;
        //log("Done delete logRecord ready to create new");
        
        trace(T_pvoutput,22);  
        reqEntries++;
        lastBufferTime = UnixNextPost;
        UnixNextPost +=  pvoutputReportInterval - (UnixNextPost % pvoutputReportInterval);
      }

            // If there's no request pending and we have bulksend entries,
            // set to post.
         trace(T_pvoutput,23);

     // @todo We want to keep realtime data with best latency (no batch) but old data batched as much as possible
      // Does pvoutputBulkSend define the max to send in one request or max latency permitted?
      // Maybe used real max as abs max
      // and the pvoutputBulkSend as the permitted for realtime
      //
      //@todo Look into using pvoutputLastPost and removing oldRecord, only creating it temporarily
      
      // If we have any unposted entries and the next post requires a wait time in the future
      // then we will post these right away.
      bool realtimePost = false;
      // @todo Calling UnixTime is VERY slow. Instead use time from service?
      if (reqEntries >= pvoutputBulkSend && UnixNextPost >= UNIXtime())
      {
        realtimePost = true;
      }

     if((( ! request || request->readyState() == 4) && HTTPrequestFree) && 
          (reqEntries >= MAX_BULK_SEND || realtimePost || reqData.available() >= reqDataLimit)){
        trace(T_pvoutput,24);
        log("Got enough entries, posting to server now");
        state = sendPost;
        return 1;
      }

      // Nothing to do until UnixNextPost
      trace(T_pvoutput,25);
      return UnixNextPost;
    }

    case sendPost: {
      log("Sending post");
      trace(T_pvoutput,26);
      
      if( ! WiFi.isConnected()){
        return UNIXtime() + 1;
      }

              // Make sure there's enough memory

      if(ESP.getFreeHeap() < 15000){
        return UNIXtime() + 1;
      }
      if( ! HTTPrequestFree){
        return 1;
      }
      HTTPrequestFree--;
      if( ! request){
        request = new asyncHTTPrequest;
      }

      // curl -d "c1=0&n=0&data=20180607,02:00,222,222,333,333,23.4,239.5;20180607,02:10,223,223,334,334,24.5,240.5" -H "X-Pvoutput-Apikey: 25d765379054a57fdb129269344ebfdb97436082" -H "X-Pvoutput-SystemId: 54871" "https://pvoutput.org/service/r2/addbatchstatus.jsp"
  	  // https://pvoutput.org/help.html#api-addbatchstatus
      // @todo need uper ase HTTP
      // @todo removed :80 assume works though
      String URL = "HTTP://pvoutput.org/service/r2/addbatchstatus.jsp";
      request->setTimeout(pvoutputHTTPTimeout);
      request->setDebug(ENABLE_HTTP_DEBUG);
      request->open("POST", URL.c_str());
      request->setReqHeader("Host", "pvoutput.org"); 
      request->setReqHeader("Content-Type", "application/x-www-form-urlencoded"); 
      request->setReqHeader("X-Pvoutput-Apikey", pvoutputApiKey); 
      String sid = String(pvoutputSystemId);
      //log("Setting system id: %s", sid.c_str());
      request->setReqHeader("X-Pvoutput-SystemId", sid.c_str()); 
      trace(T_pvoutput,4);

      //-d "data=20110112,10:00,705,1029;20110112,10:05,775,1320;20110112,10:10,800,800"
      log("Sending POST request: curl -d \"%s\" -H \"X-Pvoutput-Apikey: %s\" -H \"X-Pvoutput-SystemId: %u\" \"http://pvoutput.org/service/r2/addbatchstatus.jsp\"", reqData.peekString().c_str(), pvoutputApiKey, pvoutputSystemId);

      // reqData already has all the data we want to POST in it
      // This data should also already be prefixed with: 
      // c1=0&n=0&data=
      if(request->debug()){
        Serial.println(ESP.getFreeHeap()); 
        DateTime now = DateTime(UNIXtime() + (localTimeDiff * 3600));
        String msg = timeString(now.hour()) + ':' + timeString(now.minute()) + ':' + timeString(now.second());
        Serial.println(msg);
        Serial.println(reqData.peekString(reqData.available()));
      }

      trace(T_pvoutput,8);
      //request->open("POST", URL.c_str());
      trace(T_pvoutput,8);
      request->send(&reqData, reqData.available());
      reqEntries = 0;
      lastRequestTime = lastBufferTime;
      state = waitPost;
      return 1;
    } 

    case waitPost: {
      trace(T_pvoutput,9);
      if(request && request->readyState() == 4){
        HTTPrequestFree++;
        trace(T_pvoutput,9);
          // @todo Bad API calling responseText() erases buffer so only able to request it once
        int responseCode = request->responseHTTPcode();
        String responseText = request->responseText();
        // @todo On retry with timeout dont delete request maybe to preserve? If not required probably simpler to delete and recreate as already doing
        delete request;
        request = nullptr; 

        if(responseCode != 200){
          log("pvoutput: Post Failed: %d : %s", responseCode, responseText.c_str());

          ErrorOperation op = InterpretAddBatchStatusError(responseCode, responseText);
          if ((op == ErrorOperation::RETRY_NOW || op == ErrorOperation::RETRY_PERIOD) && retryCount >= 10) {
            log("pvoutput: Retried post many times and failed, resetting state machine");
            op = ErrorOperation::RESET;
          }


          switch (op)
          {
          case ErrorOperation::RETRY_NOW: 
            state = sendPost;
            log("pvoutput: Retrying post again in 1 second");
            return UNIXtime() + 1;

          case ErrorOperation::RETRY_PERIOD: 
            state = sendPost;
            log("pvoutput: Retrying post again in %u seconds", pvoutputReportInterval);
            return UNIXtime() + pvoutputReportInterval;

          case ErrorOperation::SKIP: 
            // Treats just like a success so fall through to that code
            log("pvoutput: Post is being skipped");
            break;

          case ErrorOperation::RESET: 
          default:
            log("pvoutput: Resetting state machine from failed input");
            state = queryLastPostTime;
            return 1;
          }
        }
        log("pvoutput: Batch POST was successful");

        // POST was successful, go back into loop reading new post data
        trace(T_pvoutput,9);
        retryCount = 0;
        pvoutputLastPost = lastRequestTime; 
        state = post;

        // @todo Maybe create a class for the pvoutput and also add state change. When entering the post state again we always want to do this I think
        // @todo I added this block
        reqData.flush();
        reqEntries = 0;
        reqData.write(PVOUTPUT_POST_DATA_PREFIX);

        trace(T_pvoutput,9);
        return 1;
      }
    }   
    
    
  }

  return 1;
}

String DateTimeToString(DateTime dt) {
  char dateStr[23];
  snprintf(dateStr, sizeof(dateStr), "%04u/%02u/%02u-%02u:%02u:%02u", dt.year(), dt.month(), dt.day(), dt.hour(), dt.minute(), dt.second());
  dateStr[sizeof(dateStr) - 1] = 0;

  return String(dateStr);
}


void SetNextPOSTTime(uint32_t* UnixLastPost, uint32_t* UnixNextPost, uint32_t pvoutputReportInterval,
  IotaLogRecord* dayStartLogRecord, IotaLogRecord* logRecord, IotaLogRecord* prevPostLogRecord) {

  *UnixLastPost += pvoutputReportInterval;
  *UnixNextPost += pvoutputReportInterval;
  *prevPostLogRecord = *logRecord;

  // If we move to a new day, then update dayStartLogRecord
  DateTime lastDt(*UnixLastPost + (localTimeDiff * 3600));
  DateTime nextDt(*UnixNextPost + (localTimeDiff * 3600));
  if(lastDt.day() != nextDt.day() || lastDt.month() != nextDt.month() || lastDt.year() != nextDt.year()) {
    trace(T_pvoutput, 39);
    log("pvoutput: Started a new day for log accumulation. Previous day: %s new day: %s", DateTimeToString(lastDt).c_str(), DateTimeToString(nextDt).c_str());
    *dayStartLogRecord = *logRecord;
  }
  else {
    trace(T_pvoutput, 40);
    log("pvoutput: Still in same day for log accumulation. Previous POST: %s next POST: %s", DateTimeToString(lastDt).c_str(), DateTimeToString(nextDt).c_str());
  }
}


#if 0


#include "IotaWatt.h"

static uint32_t pvoutputSendData(bool isResend, uint32_t unixTime, double voltage, double energyConsumed, double powerConsumed,
  double energyGenerated, double powerGenerated);

static String DateTimeToString(DateTime dt);

static void SetNextPOSTTime(uint32_t* UnixLastPost, uint32_t* UnixNextPost, uint32_t pvoutputReportInterval,
  IotaLogRecord* dayStartLogRecord, IotaLogRecord* logRecord, IotaLogRecord* prevPostLogRecord);

uint32_t pvoutputService(struct serviceBlock* _serviceBlock) {
  // trace T_pvoutput
  enum states { initialize, post, resend };
  static states state = initialize;
  static IotaLogRecord* logRecord = new IotaLogRecord;
  static IotaLogRecord* prevPostLogRecord = new IotaLogRecord;
  static IotaLogRecord* dayStartLogRecord = new IotaLogRecord;
  static File pvoutputPostLog;
  static uint32_t UnixLastPost = UNIXtime();
  static uint32_t UnixNextPost = UNIXtime();

  // Stored on object so resend can use them after inital post calculates the values to use
  static uint32_t reqUnixtime = 0;
  static double energyConsumed = 0.0;
  static double powerConsumed = 0.0;
  static double energyGenerated = 0.0;
  static double powerGenerated = 0.0;
  static double voltage = 0.0;

  struct SDbuffer {
    uint32_t data;
    SDbuffer() { data = 0; }
  };
  static SDbuffer* buf = new SDbuffer;

  trace(T_pvoutput, 0);

  // If stop signaled, do so.
  if(pvoutputStop) {
    msgLog(F("pvoutput: stopped."));
    pvoutputStarted = false;
    trace(T_pvoutput, 1);
    pvoutputPostLog.close();
    trace(T_pvoutput, 2);
    SD.remove((char*)pvoutputPostLogFile.c_str());
    trace(T_pvoutput, 3);
    state = initialize;
    return 0;
  }

  if(pvoutputRestarted) {
    state = initialize;
    pvoutputRestarted = false;
  }

  switch(state) {
    case initialize: {
      msgLog(F("pvoutput: Initializing service."));
      if(!RTCrunning) {
        msgLog(F("pvoutput: RTC not yet running, will delay PVOutput service until it is running. Try again in 5 sec"));
        return UNIXtime() + 5;
      }

      // We post the log to pvoutput, so wait until the log service is up and running.
      if(!iotaLog.isOpen()) {
        trace(T_pvoutput, 4);
        msgLog(F(
          "pvoutput: IoTaLog service is not yet running, will delay PVOutput service until it is running. Trying again in 5 sec"));
        return UNIXtime() + 5;
      }

      // If logfile for this service not open then open one
      if(!pvoutputPostLog) {
        trace(T_pvoutput, 5);
        pvoutputPostLog = SD.open(pvoutputPostLogFile, FILE_WRITE);
      }

      if(pvoutputPostLog) {
        // If is a new empty log file, then create a record for the "last key"
        if(pvoutputPostLog.size() == 0) {
          trace(T_pvoutput, 6);
          buf->data = iotaLog.lastKey();
          pvoutputPostLog.write((byte*)buf, 4);
          pvoutputPostLog.flush();
          msgLog(F("pvoutput: pvoutputlog file created."));
        }

        // Read the last key index identifying when we did the last POST
        pvoutputPostLog.seek(pvoutputPostLog.size() - 4);
        pvoutputPostLog.read((byte*)buf, 4);
        UnixLastPost = buf->data;
      }
      else {
        // @todo Copied from pvoutput, if failed to open log file then why do we continue (and not just fail init and try
        // again later?
        trace(T_pvoutput, 7);
        msgLog(F("pvoutput: pvoutputlog file failed to open just using most recent iota log time"));
        UnixLastPost = iotaLog.lastKey();
      }
      UnixNextPost = UnixLastPost + pvoutputReportInterval - (UnixLastPost % pvoutputReportInterval);

      trace(T_pvoutput, 8);
      // For pvoutput we log energy accumulated each day, so we need to
      // read the last record seen the day before and use that as the "reference"
      // When the day ticks over, then we will update the reference
      uint32_t localUnixNextPost = UnixNextPost + (localTimeDiff * 3600);
      DateTime localUnixNextPostDt(localUnixNextPost);
      DateTime previousDay(
        localUnixNextPostDt.year(), localUnixNextPostDt.month(), localUnixNextPostDt.day(), 23, 59, 59);
      previousDay = previousDay - TimeSpan(1, 00, 0, 0);
      uint32_t unixPreviousDay = previousDay.unixtime() - (localTimeDiff * 3600);

      // Now we search backwards in the iotaLog to find the nearest key matching unixPreviousDay
      logRecord->UNIXtime = unixPreviousDay;
      if(logRecord->UNIXtime < iotaLog.firstKey()) {
        logRecord->UNIXtime = iotaLog.firstKey();
      }
      if(logRecord->UNIXtime > iotaLog.lastKey()) {
        logRecord->UNIXtime = iotaLog.lastKey();
      }
      int rkResult = 1;
      do {
        // @todo This would be more efficient with iotaLog supporting readPrev()
        rkResult = iotaLog.readKey(logRecord);
        if(rkResult != 0) {
          trace(T_pvoutput, 9);
          msgLog("pvoutput: WARNING: Failed to read log record " + String(logRecord->UNIXtime)
            + " going backwards until a key matches, next: " + String(logRecord->UNIXtime - 1));
          logRecord->UNIXtime -= 1;
        }
      } while(rkResult != 0 && logRecord->UNIXtime > iotaLog.firstKey());

      // make sure the dayStartLogRecord details are valid
      *dayStartLogRecord = *logRecord;
      for(int i = 0; i < maxInputs; i++) {
        // Check for NaN
        if(dayStartLogRecord->channel[i].accum1 != dayStartLogRecord->channel[i].accum1) {
          trace(T_pvoutput, 10);
          dayStartLogRecord->channel[i].accum1 = 0;
        }
      }
      // Check for NaN
      if(dayStartLogRecord->logHours != dayStartLogRecord->logHours) {
        trace(T_pvoutput, 11);
        dayStartLogRecord->logHours = 0;
      }

      // Obtain the previous post log record
      prevPostLogRecord->UNIXtime = UnixLastPost;
      rkResult = iotaLog.readKey(prevPostLogRecord);
      if(rkResult != 0) {
        trace(T_pvoutput, 12);
        msgLog("pvoutput: WARNING: Failed to read log record " + String(prevPostLogRecord->UNIXtime)
          + " which is supposed to contain the previous post details used to detemine power used since then");
      }

      // Advance state.
      // Set task priority low so that datalog will run before this.
      state = post;
      _serviceBlock->priority = priorityLow;

      msgLog("pvoutput: Started.");
      msgLog("pvoutput:    pvoutputReportInterval: ", pvoutputReportInterval);
      msgLog("pvoutput:    pvoutputApiKey: ", pvoutputApiKey);
      msgLog("pvoutput:    pvoutputSystemId: ", pvoutputSystemId);
      msgLog("pvoutput:    pvoutputMainsChannel: ", pvoutputMainsChannel);
      msgLog("pvoutput:    pvoutputSolarChannel: ", pvoutputSolarChannel);
      msgLog("pvoutput:    pvoutputHTTPTimeout: ", pvoutputHTTPTimeout);

      DateTime dtLast(logRecord->UNIXtime + (localTimeDiff * 3600));
      DateTime dtNow(UNIXtime() + (localTimeDiff * 3600));
      msgLog("pvoutput: Next post for: " + DateTimeToString(localUnixNextPostDt)
        + " comparing to previous day post at: " + DateTimeToString(dtLast) + " now is: " + DateTimeToString(dtNow));
      trace(T_pvoutput, 13);
      return UnixNextPost;
    }

    case post: {
      // If WiFi is not connected,
      // just return without attempting to log and try again in a few seconds.
      if(WiFi.status() != WL_CONNECTED) {
        trace(T_pvoutput, 14);
        return 2;
      }

      // If we are current,
      // Anticipate next posting at next regular interval and break to reschedule.
      if(iotaLog.lastKey() < UnixNextPost) {
        uint32_t now = UNIXtime();
        if(now < UnixNextPost) {
          trace(T_pvoutput, 15);
          return UnixNextPost;
        }
        else {
          trace(T_pvoutput, 16);
          msgLog(
            "pvoutput: We want to post, but iotaLog does not yet have enough data for us to be able to post it. last key : "
            + String(iotaLog.lastKey()) + " We want to post at time : " + String(UnixNextPost)
            + " time has been reached so just waiting on iota log to have the data.Waiting for another second");
          return now + 1;
        }
      }

      // First read the key from UnixLastPost, then read next from the log.
      trace(T_pvoutput, 17);
      logRecord->UNIXtime = UnixLastPost;
      int readResult = iotaLog.readKey(logRecord);
      if (readResult != 0) {
        trace(T_pvoutput, 18);
        msgLog("pvoutput: Failed to read previous log record with time: " + String(logRecord->UNIXtime) + " trying again in a few seconds");
        return UNIXtime() + 1;
      }

      // Not current.  Read sequentially to get the entry >= scheduled post time
      // Skip over any IoTaLog records < UnixNextPost until we find one we can use for diff against for the pvoutput
      // interval Note: Basically asserts that the logs exist, which should have been checked above.
      while(logRecord->UNIXtime < UnixNextPost) {
        if(logRecord->UNIXtime >= iotaLog.lastKey()) {
          msgLog("pvoutput:runaway seq read.", logRecord->UNIXtime);
          ESP.reset();
        }
        readResult = iotaLog.readNext(logRecord);
        if (readResult != 0) {
          trace(T_pvoutput, 19);
          msgLog("pvoutput: Failed to read next log record with serial: " + String(logRecord->serial) + " trying again in a few seconds");
          return UNIXtime() + 1;
        }
      }

      // We now have the first record after the time we want to POST for.
      // If it is a *long* way in the future, we need to skip a number of posts
      if (logRecord->UNIXtime >= (UnixNextPost + pvoutputReportInterval)) {
        UnixLastPost = logRecord->UNIXtime - (logRecord->UNIXtime % pvoutputReportInterval);
        if (UnixLastPost == logRecord->UNIXtime) {
          UnixLastPost -= pvoutputReportInterval;
        }

        // We will re-query the logRecord in the next tick we are ready for
        msgLog("pvoutput: Skipping POST for period from: " + String(UnixNextPost) + " to " + String(UnixLastPost) + " as there is no data recorded for that time");
        UnixNextPost = UnixLastPost + pvoutputReportInterval;
        return UNIXtime() + 1;
      }

      // Adjust the posting time to match the log entry time (at modulo we care about)
      reqUnixtime = logRecord->UNIXtime - (logRecord->UNIXtime % pvoutputReportInterval);
      trace(T_pvoutput, 20);

      // The measurements should be such that:
      // chan 1 : mains +ve indicates net import -ve indicates net export
      // chan 2 : solar -ve indicates generation +ve should never really happen would indicate solar panels using power

      // Find the mean voltage since last post
      int voltageChannel = -1;
      if(pvoutputMainsChannel >= 0) {
        voltageChannel = inputChannel[pvoutputMainsChannel]->_vchannel;
      }
      else if(pvoutputSolarChannel >= 0) {
        voltageChannel = inputChannel[pvoutputSolarChannel]->_vchannel;
      }

      voltage = 0;
      if(voltageChannel >= 0 && logRecord->logHours != prevPostLogRecord->logHours) {
        trace(T_pvoutput, 21);
        voltage = (logRecord->channel[voltageChannel].accum1 - prevPostLogRecord->channel[voltageChannel].accum1);
        voltage /= (logRecord->logHours - prevPostLogRecord->logHours);
      }

      // Energy is calculated since begining of the day
      //
      // Generated energy is always a negative value in our calculations.
      // I.e. I assume most power channels in existing iotawatt indicate
      // power usage of that channel and are positive. In fact many have
      // enabled ability to force them positive.
      //
      // By using a negative value to indicate export/generation we have
      // a more consitent view of things
      //
      // Because a solar channel always generates and never uses power, we
      // will enforce it has a negative value in the case the CT has been
      // installed in reverse. This will not be considered a failure in
      // the configuration we will just cope with it.
      energyGenerated = 0;
      if(pvoutputSolarChannel >= 0) {
        trace(T_pvoutput, 22);
        energyGenerated
          = logRecord->channel[pvoutputSolarChannel].accum1 - dayStartLogRecord->channel[pvoutputSolarChannel].accum1;
        if(energyGenerated > 0) energyGenerated *= -1;
      }

      // Find out how much energy we imported from the main line
      double energyImported = 0.0;
      if(pvoutputMainsChannel >= 0) {
        trace(T_pvoutput, 23);
        energyImported
          = logRecord->channel[pvoutputMainsChannel].accum1 - dayStartLogRecord->channel[pvoutputMainsChannel].accum1;
      }

      // Example:
      // generated = -5kWh
      // imported = 2kWh
      // thus we are consuming 7kWh as we are using all the 5kW and an additional 2kW from mains import
      energyConsumed = energyImported - energyGenerated;

      // the mean power used in W since the last post
      powerGenerated = 0;
      if(pvoutputSolarChannel >= 0 && logRecord->logHours != prevPostLogRecord->logHours) {
        trace(T_pvoutput, 24);
        powerGenerated
          = logRecord->channel[pvoutputSolarChannel].accum1 - prevPostLogRecord->channel[pvoutputSolarChannel].accum1;
        if(powerGenerated > 0) powerGenerated *= -1;
        powerGenerated = powerGenerated / (logRecord->logHours - prevPostLogRecord->logHours);
      }

      // Find out how much energy we imported from the main line
      double powerImported = 0.0;
      if(pvoutputMainsChannel >= 0 && logRecord->logHours != prevPostLogRecord->logHours) {
        trace(T_pvoutput, 25);
        powerImported
          = logRecord->channel[pvoutputMainsChannel].accum1 - prevPostLogRecord->channel[pvoutputMainsChannel].accum1;
        powerImported = powerImported / (logRecord->logHours - prevPostLogRecord->logHours);
      }

      // Example:
      // generated = -5kWh
      // imported = 2kWh
      // thus we are consuming 7kWh as we are using all the 5kW and an additional 2kW from mains import
      powerConsumed = powerImported - powerGenerated;

      // If we are exporting more than we are generating something is wrong
      if(powerImported < powerGenerated) {
        trace(T_pvoutput, 26);
        msgLog(
          "pvoutput: PVOutput configuration is incorrect. Appears we are exporting more power than we are generating. Between " + String(logRecord->UNIXtime) + " and " + String(prevPostLogRecord->UNIXtime) 
          + " we imported: " + String(powerImported) + "Wh and generated: " + String(powerGenerated) + "Wh");
      }

      uint32_t failureWaitDuration = pvoutputSendData(false, reqUnixtime, voltage, energyConsumed, powerConsumed, energyGenerated, powerGenerated);
      if (failureWaitDuration > 0) {
        trace(T_pvoutput, 27);
        // Use resend state, the only real difference is the timeout
        state = resend;
        msgLog("pvoutput: Pushing data to PVOutput failed, trying again in " + String(failureWaitDuration) + " sec");
        return UNIXtime() + failureWaitDuration;
      }

      buf->data = UnixLastPost;
      pvoutputPostLog.write((byte*)buf, 4);
      pvoutputPostLog.flush();
      SetNextPOSTTime(
        &UnixLastPost, &UnixNextPost, pvoutputReportInterval, dayStartLogRecord, logRecord, prevPostLogRecord);

      trace(T_pvoutput, 28);
      state = post;
      return UnixNextPost;
    }

    case resend: {
      msgLog(F("pvoutput: Resending pvoutput data."));
      trace(T_pvoutput, 29);
      uint32_t failureWaitDuration = pvoutputSendData(true, reqUnixtime, voltage, energyConsumed, powerConsumed, energyGenerated, powerGenerated);
      if(failureWaitDuration > 0) {
        trace(T_pvoutput, 30);
        msgLog("pvoutput: Pushing data to PVOutput failed, trying again in " + String(failureWaitDuration) + " sec");
        return UNIXtime() + failureWaitDuration;
      }
      else {
        trace(T_pvoutput, 31);
        buf->data = UnixLastPost;
        pvoutputPostLog.write((byte*)buf, 4);
        pvoutputPostLog.flush();
        SetNextPOSTTime(
          &UnixLastPost, &UnixNextPost, pvoutputReportInterval, dayStartLogRecord, logRecord, prevPostLogRecord);

        state = post;
        return 1;
      }
      break;
    }
  }
  return 1;
}

/************************************************************************************************
 *  pvoutputSend - send data to the pvoutput server.
 ***********************************************************************************************/
uint32_t pvoutputSendData(bool isResend, uint32_t unixTime, double voltage, double energyConsumed, double powerConsumed,
  double energyGenerated, double powerGenerated) {
  // This sends data to PVOutput using API: https://pvoutput.org/help.html#api-addstatus
  trace(T_pvoutput, 32);

  // PVOutput expects reports as positive values, our internal calculations
  // expect negative values for generation so just convert them now
  energyGenerated *= -1;
  powerGenerated *= -1;

  // PVOutput requires data no older than 14 days, will only do 13 days
  uint32_t thirteenDaysAgo = UNIXtime() - (13U * 24U * 60U * 60U);
  if (unixTime < thirteenDaysAgo) {
    msgLog("pvoutput: Post for old data: " + String(unixTime) + " and PVOutput only accepts data upto 14 days old (we are limiting to 13 days): " + String(thirteenDaysAgo));
    return 0;
  }

  // PVOutput requires localized time
  uint32_t localUnixTime = unixTime + (localTimeDiff * 3600);
  DateTime dt(localUnixTime);

  // Now sanity check the data so we dont get PVOutput infinite POST loop
  // due to known problems
  if(energyGenerated < 0.0) {
    trace(T_pvoutput, 33);
    msgLog("pvoutput: energyGenerated: " + String(energyGenerated) + "Wh is invalid. Are the CT coils installed in the correct direction? PVOutput wont accept negative values force changing it to 0.0");
    energyGenerated = 0.0;
  }

  if(powerGenerated < 0.0) {
    trace(T_pvoutput, 34);
    msgLog("pvoutput: powerGenerated: " + String(powerGenerated) + "W is invalid. Are the CT coils installed in the correct direction? PVOutput wont accept negative values force changing it to 0.0");
    powerGenerated = 0.0;
  }

  if(energyConsumed < 0.0) {
    trace(T_pvoutput, 35);
    msgLog("pvoutput: energyConsumed: " + String(energyConsumed) + "Wh is invalid. Are the CT coils installed in the correct direction? PVOutput wont accept negative values force changing it to 0.0");
    energyConsumed = 0.0;
  }

  if(powerConsumed < 0.0) {
    trace(T_pvoutput, 36);
    msgLog("pvoutput: powerConsumed: " + String(powerConsumed) + "W is invalid. Are the CT coils installed in the correct direction? PVOutput wont accept negative values force changing it to 0.0");
    powerConsumed = 0.0;
  }

  char dateStr[10];
  snprintf(dateStr, sizeof(dateStr), "%04u%02u%02u", dt.year(), dt.month(), dt.day());
  dateStr[sizeof(dateStr) - 1] = 0;

  char timeStr[10];
  snprintf(timeStr, sizeof(timeStr), "%02u:%02u", dt.hour(), dt.minute());
  timeStr[sizeof(timeStr) - 1] = 0;

  String path = String("/service/r2/addstatus.jsp") + "?d=" + String(dateStr)
    + "&t=" + String(timeStr) + "&v1=" + String(energyGenerated)
    + "&v2=" + String(powerGenerated) + "&v3=" + String(energyConsumed) + "&v4="
    + String(powerConsumed)
    //+ "&v5=" + String(temperature)
    + "&v6=" + String(voltage) + "&c1=0" // If 1 indicates cumuliative not reset of energy each day
    + "&n=0"                             // If 1 indicates net import/export not gross, currently calc gross in caller
    ;
  msgLog("pvoutput: Posting for time: " + String(unixTime) + " path: " + "pvoutput.org:80" + path
    + " With key: " + String(pvoutputApiKey) + " and systemId: " + String(pvoutputSystemId));
  http.begin("pvoutput.org", 80, path);
  http.addHeader("Host", "pvoutput.org");
  http.addHeader("Content-Type", "application/x-www-form-urlencoded");
  http.addHeader("X-Pvoutput-Apikey", pvoutputApiKey);
  http.addHeader("X-Pvoutput-SystemId", String(pvoutputSystemId));
  http.setTimeout(pvoutputHTTPTimeout);

  unsigned long start = micros();
  int httpCode = http.GET();
  unsigned long end = micros();
  String response = http.getString();
  http.end();
  if(httpCode != HTTP_CODE_OK && httpCode != 204) {
    trace(T_pvoutput, 37);
    String code = String(httpCode);
    if(httpCode < 0) {
      code = http.errorToString(httpCode);
    }

    msgLog("pvoutput: POST FAILED code: " + String(httpCode) + " message: " + code + " response: " + response);
    if (httpCode == 403 && response.indexOf("Exceeded") >= 0 && response.indexOf("requests per hour") >= 0) {
      // Documentation says to wait for an hour before making further requests
      // https://pvoutput.org/help.html#api-errors
      msgLog("pvoutput: Waiting for an hour to try again in accordance if PVOutput API docs");
      return 60 * 60;
    }
    else if (isResend) {
      return 60;
    }
    else {
      return 10;
    }
  }
  else {
    trace(T_pvoutput, 38);
    msgLog("pvoutput: POST success code: " + String(httpCode) + " response: " + response);
  }

  return 0;
}

String DateTimeToString(DateTime dt) {
  char dateStr[23];
  snprintf(dateStr, sizeof(dateStr), "%04u/%02u/%02u-%02u:%02u:%02u", dt.year(), dt.month(), dt.day(), dt.hour(), dt.minute(), dt.second());
  dateStr[sizeof(dateStr) - 1] = 0;

  return String(dateStr);
}

void SetNextPOSTTime(uint32_t* UnixLastPost, uint32_t* UnixNextPost, uint32_t pvoutputReportInterval,
  IotaLogRecord* dayStartLogRecord, IotaLogRecord* logRecord, IotaLogRecord* prevPostLogRecord) {

  *UnixLastPost += pvoutputReportInterval;
  *UnixNextPost += pvoutputReportInterval;
  *prevPostLogRecord = *logRecord;

  // If we move to a new day, then update dayStartLogRecord
  DateTime lastDt(*UnixLastPost + (localTimeDiff * 3600));
  DateTime nextDt(*UnixNextPost + (localTimeDiff * 3600));
  if(lastDt.day() != nextDt.day() || lastDt.month() != nextDt.month() || lastDt.year() != nextDt.year()) {
    trace(T_pvoutput, 39);
    msgLog("pvoutput: Started a new day for log accumulation. Previous day: " + DateTimeToString(lastDt)
      + " new day: " + DateTimeToString(nextDt));
    *dayStartLogRecord = *logRecord;
  }
  else {
    trace(T_pvoutput, 40);
    msgLog("pvoutput: Still in same day for log accumulation. Previous POST: " + DateTimeToString(lastDt)
      + " next POST: " + DateTimeToString(nextDt));
  }
}
#endif
