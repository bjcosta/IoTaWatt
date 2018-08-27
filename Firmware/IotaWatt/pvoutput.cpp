#include "pvoutput.h"
#include "IotaWatt.h"
#include <assert.h>

#define ENABLE_HTTP_DEBUG false

// We use c1=0 as for cumuliative posts there is a limit of 200kWh for the overall accumulation
// This will stop working after about 10 days
//
// We also use n=0 as we can easily calculate the values ourselves and it doesn't work
// for energy anyway only for the power reports.
static const char* PVOUTPUT_POST_DATA_PREFIX = "c1=0&n=0&data=";
static const uint16_t MAX_BULK_SEND = 30;

// Max permitted time in the past for a POST is 14 days
// We will set max to 13 days for now
static const uint32_t MAX_PAST_POST_TIME = 13 * 24 * 60 * 60;

// @todo Ask Bob to make the state enums public
// Copied from asyncHTTPrequest as it is not public for some reason but states used all over the place as int literals
enum readyStates 
{
    readyStateUnsent = 0,           // Client created, open not yet called
    readyStateOpened =  1,          // open() has been called, connected
    readyStateHdrsRecvd = 2,        // send() called, response headers available
    readyStateLoading = 3,          // receiving, partial data available
    readyStateDone = 4              // Request complete, all data available.
}; 


// @todo overloaded dateString() that takes a uint32_t in utilities.cpp same format though and the utilities one could call this
static String dateString(const DateTime& dt)
{
  char dateStr[23] = {0};
  // @todo I prefer this format, but IotaWatt is using some other format I am copying:
  // snprintf(dateStr, sizeof(dateStr), "%04u/%02u/%02u-%02u:%02u:%02u", dt.year(), dt.month(), dt.day(), dt.hour(), dt.minute(), dt.second());

  snprintf(dateStr, sizeof(dateStr), "%u/%u/%u %u:%u:%u", dt.month(), dt.day(), dt.year()%100, dt.hour(), dt.minute(), dt.second());
  dateStr[sizeof(dateStr) - 1] = 0;
  return String(dateStr);
}

//=============================================================================
// PVOutput
//=============================================================================
class PVOutput
{
public:
  bool UpdateConfig(const char* jsonText);
  void GetStatusJson(JsonObject& json) const;
  uint32_t Tick(struct serviceBlock* serviceBlock);

private:
  enum class State 
  {
    STOPPED,
    STOPPING,
    INITIALIZE,
    QUERY_GET_STATUS,
    QUERY_GET_STATUS_WAIT_RESPONSE,
    COLLATE_DATA,
    POST_DATA,
    POST_DATA_WAIT_RESPONSE
  };

  enum class PVOutputError
  {
    NONE,
    UNMAPPED_ERROR,
    DATE_TOO_OLD,
    DATE_IN_FUTURE,
    RATE_LIMIT,
    MOON_POWERED
  };

  struct Config
  {
    // Revision control for dynamic config
    int32_t   revision = -1;

    const char*  apiKey = nullptr;
    int          systemId = 0;
    int          mainsChannel = 0;
    int          solarChannel = 0;
    uint32_t     httpTimeout = 2000;

    // transaction yellow light size
    size_t       reqDataLimit = 4000;

    static const uint32_t MIN_REPORT_INTERVAL = 5*60;

    // Interval (sec) to POST data to pvoutput
    uint32_t     reportInterval = MIN_REPORT_INTERVAL * 1;

    // How many entries to permit in realtime bulk send
    uint16_t     bulkSend = 1;
  };

  struct Entry
  {
    uint32_t unixTime;

    // The above unix time as a local date time
    DateTime dt;

    double voltage;
    double energyConsumed;
    double powerConsumed;
    double energyGenerated;
    double powerGenerated;
  };

  static const char* StateToString(State state);
  void Start();
  void Stop();
  bool IsRunning() const;
  void SetState(State new_state);

  uint32_t TickInitialize(struct serviceBlock* serviceBlock);
  uint32_t TickQueryGetStatus(struct serviceBlock* serviceBlock);
  uint32_t TickQueryGetStatusWaitResponse(struct serviceBlock* serviceBlock);
  uint32_t TickCollateData(struct serviceBlock* serviceBlock);
  uint32_t TickPostData(struct serviceBlock* serviceBlock);
  uint32_t TickPostDataWaitResponse(struct serviceBlock* serviceBlock);

  static PVOutputError InterpretPVOutputError(int responseCode, const String& responseText);
  static const char* ParseFixedInteger(char* tmp, const char* src, size_t size, int* value, int min, int max);
  static const char* ParseExpectedCharacter(const char* src, char expected);
  static const char* ParseStringUpto(const char* src, char delim, const char** fieldString, size_t* fieldStringSize);

  static bool ParseGetStatusResponse(const String& responseText, DateTime* dt);
  uint32_t CalculateDayStart(uint32_t ts);
  static bool ReadSaneLogRecordOrPrev(IotaLogRecord* record, uint32_t when);
  size_t CalculateMissingPeriodsToSkip(IotaLogRecord& prevPostRecord, IotaLogRecord& nextPostRecord);
  void WriteEntryString(const String& entry_str);
  bool CollectNextDataPoint();
  void IncrementTimeInterval(size_t incrementPeriods=1, const char* entryDebug="");
  bool CalculateEntry(Entry* entry, const IotaLogRecord& prevPostRecord, const IotaLogRecord& nextPostRecord, const IotaLogRecord& dayStartRecord);
  static String GenerateEntryString(Entry entry);

  State state = State::STOPPED;
  uint32_t unixDayStart  = 0;
  uint32_t unixPrevPost = 0;
  uint32_t unixNextPost = 0;

  // Current request buffer
  // We were using xbuf here, but the call to send() destroyed the data in the buffer so it couldn't be used for resend
  // I saw no reason to use xbuf here instead of String so have moved to String
  String reqData;

  // Number of measurement intervals in current reqData
  size_t reqEntries = 0;

  // Number of times most recent HTTP request as retried
  int16_t retryCount = 0;

  // Instance of asyncHTTPrequest
  asyncHTTPrequest* request = nullptr;

  // The PVOutput config data
  Config config;

  bool mainsChannelReversed = false;
  bool solarChannelReversed = false;
};
static PVOutput pvoutput;
//=============================================================================



//=============================================================================
void PVOutputUpdateConfig(const char* jsonText)
{
  pvoutput.UpdateConfig(jsonText);
}

//=============================================================================
void PVOutputGetStatusJson(JsonObject& json)
{
  pvoutput.GetStatusJson(json);
}

//=============================================================================
static uint32_t PVOutputTick(struct serviceBlock* serviceBlock)
{
  return pvoutput.Tick(serviceBlock);
}

//=============================================================================
const char* PVOutput::StateToString(State state)
{
  switch(state)
  {
    case State::STOPPED: return "STOPPED";
    case State::STOPPING: return "STOPPING";
    case State::INITIALIZE: return "INITIALIZE";
    case State::QUERY_GET_STATUS: return "QUERY_GET_STATUS";
    case State::QUERY_GET_STATUS_WAIT_RESPONSE: return "QUERY_GET_STATUS_WAIT_RESPONSE";
    case State::COLLATE_DATA: return "COLLATE_DATA";
    case State::POST_DATA: return "POST_DATA";
    case State::POST_DATA_WAIT_RESPONSE: return "POST_DATA_WAIT_RESPONSE";
    default: return "UNKNOWN";
  }
}

//=============================================================================
void PVOutput::SetState(State new_state)
{
  //log("Moving from state: %s to: %s", StateToString(state), StateToString(new_state));
  state = new_state;
}

//=============================================================================
bool PVOutput::UpdateConfig(const char* jsonText)
{
  trace(T_pvoutput,1);
  if (jsonText == nullptr)
  {
    trace(T_pvoutput,2);
    if (pvoutput.IsRunning())
    {
      log("pvoutput: No PVOutput config section. Disabling PVOutput service");
      Stop();
    }
    return false;
  }

  log("Got pvoutput array: %s", jsonText);
  DynamicJsonBuffer Json;
  JsonObject& configJson = Json.parseObject(jsonText);

  // We can choose to do nothing if the javascript didnt change anything relevant in the config
  int revision = configJson["revision"];
  if (revision == config.revision)
  {
    trace(T_pvoutput,3);
    log("pvoutput: PVOutput config revision (%d) is unchanged from running config", revision);
    return true;
  }


  if (!configJson.is<int>("systemId")
    || !configJson.is<int>("mainsChannel")
    || !configJson.is<int>("solarChannel")
    || !configJson.is<int>("httpTimeout")
    || !configJson.is<unsigned int>("reportInterval")
    || !configJson.is<const char*>("apiKey")
    )
  {
    trace(T_pvoutput,4);
    log("pvoutput: Json parse failed. Missing or invalid config items.");
    Stop();
    return false;
  }

  trace(T_pvoutput,5);
  config.revision = revision;
  config.systemId = configJson["systemId"].as<int>();
  config.mainsChannel = configJson["mainsChannel"].as<int>();
  config.solarChannel = configJson["solarChannel"].as<int>();
  config.httpTimeout = configJson["httpTimeout"].as<unsigned int>();
  config.reportInterval = configJson["reportInterval"].as<int>();
  // @todo verify that reportInterval is multiple of MIN_REPORT_INTEVAL

  delete[] config.apiKey;
  config.apiKey = charstar(configJson["apiKey"].as<const char*>());

  // Start or re-start the PVOutput service with the new config
  trace(T_pvoutput,6);
  Start();

  trace(T_pvoutput,7);
  log("Loaded PVOutput config using: revision:%d, systemID:%d, mainChannel:%d, solarChannel:%d, HTTPTimeout:%d, interval:%d, ApiKey:<private>", config.revision, config.systemId, config.mainsChannel, config.solarChannel, config.httpTimeout, config.reportInterval);
  return true;
}

//=============================================================================
void PVOutput::GetStatusJson(JsonObject& json) const
{
  // @todo Writeme
  trace(T_pvoutput,8);
  json.set(F("running"),IsRunning());
  //json.set(F("lastpost"),influxLastPost);
}

//=============================================================================
uint32_t PVOutput::Tick(struct serviceBlock* serviceBlock)
{
  switch(state)
  {
    case State::INITIALIZE: return TickInitialize(serviceBlock);
    case State::QUERY_GET_STATUS: return TickQueryGetStatus(serviceBlock);
    case State::QUERY_GET_STATUS_WAIT_RESPONSE: return TickQueryGetStatusWaitResponse(serviceBlock);
    case State::COLLATE_DATA: return TickCollateData(serviceBlock);
    case State::POST_DATA: return TickPostData(serviceBlock);
    case State::POST_DATA_WAIT_RESPONSE: return TickPostDataWaitResponse(serviceBlock);

    default:
    case State::STOPPED:
      assert(!"Invalid state to be ticked");
      // Fall through to do the same as STOPPING

    case State::STOPPING: 
      trace(T_pvoutput,9);
      SetState(State::STOPPED);
      return 0;
  }
}

//=============================================================================
void PVOutput::Start()
{
  // Make sure to restart the service
  // @todo This is not a nice restart, but aborts any outstanding requests. Do we want a nice async stop?
  Stop();

  log("pvoutput: Starting PVOutput service");

  // If the service has actually stopped (not STOPPING) then we need to re-add it.
  if (state == State::STOPPED)
  {
    trace(T_pvoutput,10);
    log("pvoutput: Service is not running, creating new service to be added to service tick queue");
    NewService(&PVOutputTick);
  }

  trace(T_pvoutput,11);
  SetState(State::INITIALIZE);
}

//=============================================================================
void PVOutput::Stop()
{
  if (state == State::STOPPED || state == State::STOPPING)
  {
    trace(T_pvoutput,12);
    return;
  }

  log("pvoutput: Stopping PVOutput service");
  trace(T_pvoutput,13);

  // The service queue does not permit removal of a service. Instead it requires
  // the service be ticked and that tick return 0 to stop it.
  //
  // So we need to intriduce a stopping state that will achieve this.
  SetState(State::STOPPING);

  // Need to cancel any outstanding requests, reset all objects to initial states
  if (request != nullptr) 
  {
    trace(T_pvoutput,14);
    request->abort();
  }
  delete request;
  request = nullptr;

  // Flush apparently means same as clear() on STL containers not flush() of STL files.
  reqData = "";
 
  unixDayStart  = 0;
  unixPrevPost = 0;
  unixNextPost = 0;
  reqEntries = 0;
  retryCount = 0;
}

//=============================================================================
bool PVOutput::IsRunning() const
{
  return state != State::STOPPED && state != State::STOPPING;
}

//=============================================================================
PVOutput::PVOutputError PVOutput::InterpretPVOutputError(int responseCode, const String& responseText)
{
  // Most errors we will just never recover from and keep those as unmapped errors. 
  // If they are bugs we will need to fix them , if they are config errors then user needs to fix something
  // 
  // Only some errors like old requests will be "skipped" as we know about them and cant do 
  // anything about it
  if (responseCode == 400)
  {
    if (strstr(responseText.c_str(), "Date is older than") != nullptr)
    {
      return PVOutputError::DATE_TOO_OLD;
    }
    else if (strstr(responseText.c_str(), "Date is in the future") != nullptr || strstr(responseText.c_str(), "Invalid future date") != nullptr)
    {
      return PVOutputError::DATE_IN_FUTURE;
    }
    else if (strstr(responseText.c_str(), "Moon powered") != nullptr)
    {
      return PVOutputError::MOON_POWERED;
    }
  }
  else if (responseCode == 403)
  {
    if (strstr(responseText.c_str(), "Exceeded 60 requests per hour") != nullptr)
    {
      return PVOutputError::RATE_LIMIT;
    }
  }

  return PVOutputError::UNMAPPED_ERROR;


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

//=============================================================================
const char* PVOutput::ParseFixedInteger(char* tmp, const char* src, size_t size, int* value, int min, int max)
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

//=============================================================================
const char* PVOutput::ParseExpectedCharacter(const char* src, char expected)
{
  if (src == nullptr)
    return nullptr;

  if (*src != expected)
    return nullptr;

  return src + 1;
}

//=============================================================================
const char* PVOutput::ParseStringUpto(const char* src, char delim, const char** fieldString, size_t* fieldStringSize)
{
  if (src == nullptr)
    return nullptr;

  *fieldString = src;
  *fieldStringSize = 0;
  while (*src != 0 && *src != delim)
  {
    ++src;
    ++(*fieldStringSize);
  }

  if (*src == 0)
    return src;
  else
    return src + 1;
}

//=============================================================================
bool PVOutput::ParseGetStatusResponse(const String& responseText, DateTime* dt)
{
  // Returns something like: 
  // 20180607,03:30,223,125,334,322,0.022,24.5,242.0
  //
  // Following API defined at: https://pvoutput.org/help.html#api-getstatus
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
  // We only care about the date and time for now
  // So we will parse the first two items and ignore the rest
  //
  // YYYYMMDD,HH:MM,
  char tmp[5];
  const char* src = responseText.c_str();

  int year = 0;
  int month = 0;
  int day = 0;
  int hour = 0;
  int minute = 0;
  src = ParseFixedInteger(tmp, src, 4, &year, 0, 9999);
  src = ParseFixedInteger(tmp, src, 2, &month, 1, 12);
  src = ParseFixedInteger(tmp, src, 2, &day, 1, 31);
  src = ParseExpectedCharacter(src, ',');
  src = ParseFixedInteger(tmp, src, 2, &hour, 0, 23);
  src = ParseExpectedCharacter(src, ':');
  src = ParseFixedInteger(tmp, src, 2, &minute, 0, 59);
  src = ParseExpectedCharacter(src, ',');
  if (src == nullptr)
  {
    trace(T_pvoutput,15);
    return false;
  }
  trace(T_pvoutput,16);

  log("Parsed status date/time: %u %u %u %u %u", year, month, day, hour, minute);
  *dt = DateTime(year, month, day, hour, minute, 0);

  // In special case of start of day, we will see if there is any data for energy
  // If not then assume this is the start of the new day
  // If so then assume this is the end of prev day and still need to post start of new day
  // @todo Add back in after testing 
  //if (dt->hour() == 0 && dt->minute() == 0)
  {
    // Now if we can parse a non 0 or non NaN value for either energyConsumption or energyGeneration then
    // it means that we have a end of day record not a start of day record and we want
    // to set the datetime to the sentinel <prev-day>:23:59:59
    bool containsEnergyValues = false;
    const char* fieldString = nullptr;
    size_t fieldStringSize = 0;

    // Read Energy Generation
    src = ParseStringUpto(src, ',', &fieldString, &fieldStringSize);
    if (strncmp(fieldString, "0", fieldStringSize) != 0 || strncmp(fieldString, "NaN", fieldStringSize) != 0)
    {
      containsEnergyValues = true;
    }

    // Read Power Generation
    src = ParseStringUpto(src, ',', &fieldString, &fieldStringSize);

    // Read Energy Consumption
    src = ParseStringUpto(src, ',', &fieldString, &fieldStringSize);
    if (strncmp(fieldString, "0", fieldStringSize) != 0 || strncmp(fieldString, "NaN", fieldStringSize) != 0)
    {
      containsEnergyValues = true;
    }


    // @todo remove this if after testing
    if (dt->hour() == 0 && dt->minute() == 0)
    {

      // For the following posts we get:
      // curl -d "c1=0&n=0&d=20180724&t=23:59&v1=1000&v2=0&v3=1200&v4=100" https://pvoutput.org/service/r2/addstatus.jsp
      // curl "http://pvoutput.org/service/r2/getstatus.jsp"
      // Returns: 20180725,00:00,1000,0,1200,100,NaN,NaN,NaN
      //
      // curl -d "c1=0&n=0&d=20180725&t=00:00&v1=0&v2=0&v3=0&v4=0" https://pvoutput.org/service/r2/addstatus.jsp
      // curl "http://pvoutput.org/service/r2/getstatus.jsp"
      // Returns: 20180725,00:00,0,0,0,0,NaN,NaN,NaN
      if (containsEnergyValues)
      {
        // We need to use the prev day 23:59:59 sentinel as this record read is a end-of-day not a start-of-day record
        // Though PVOutput reports 00:00:00 of next day with non-zero energy values

        // Move back to the sentinel time (is just 1 sec before 00:00:00, i.e. 23:59:59)
        uint32_t tmp = dt->unixtime() - 1;
        *dt = DateTime(tmp);
      }
    }
  }

  return true;
}

//=============================================================================
uint32_t PVOutput::CalculateDayStart(uint32_t ts)
{
  DateTime localDt(ts + (localTimeDiff * 3600));

  trace(T_pvoutput,17);
  DateTime localDayStart(localDt.year(), localDt.month(), localDt.day(), 00, 00, 00);
  uint32_t dayStart = localDayStart.unixtime() - (localTimeDiff * 3600);

  // Make sure it is on a report boundary (Is generally expected anyway)
  dayStart -= dayStart % config.reportInterval;
  return dayStart;
}

//=============================================================================
uint32_t PVOutput::TickInitialize(struct serviceBlock* serviceBlock)
{
  if (!currLog.isOpen())
  {
   trace(T_pvoutput,18);
   log("Waiting for IoTaLog to be opened");
    return UNIXtime() + 5;
  }

  trace(T_pvoutput,19);
  serviceBlock->priority = priorityLow;
  SetState(State::QUERY_GET_STATUS);
  return 1;
}

//=============================================================================
uint32_t PVOutput::TickQueryGetStatus(struct serviceBlock* serviceBlock)
{
  trace(T_pvoutput,20);

  // @todo influx makes this configurable pvoutputBeginPosting
  unixPrevPost = 0;

  // Make sure wifi is connected and there is a resource available.
  if (!WiFi.isConnected() || !HTTPrequestFree)
  {
    trace(T_pvoutput,21);
    return UNIXtime() + 1;
  }
  HTTPrequestFree--;

  // Create a new request
  if (request)
  {
    delete request;
  }
  request = new asyncHTTPrequest;

  // @todo Need to test what is returned on a newly created PVOutput service, we want to make sure we do something sensible. I imagine right now just stay in infinite retry loop

  // API for this is documented at: https://pvoutput.org/help.html#api-getstatus
  request->setTimeout(config.httpTimeout);
  request->setDebug(ENABLE_HTTP_DEBUG);
  // Note: asyncHTTPrequest seems require uppercase HTTP in URL
  request->open("GET", "HTTP://pvoutput.org/service/r2/getstatus.jsp");
  request->setReqHeader("Host", "pvoutput.org"); 
  request->setReqHeader("Content-Type", "application/x-www-form-urlencoded"); 
  request->setReqHeader("X-Pvoutput-Apikey", config.apiKey); 
  request->setReqHeader("X-Pvoutput-SystemId", String(config.systemId).c_str()); 
  trace(T_pvoutput,22);
  reqData = "";
  if(request->debug())
  {
    Serial.println(ESP.getFreeHeap()); 
    DateTime now = DateTime(UNIXtime() + (localTimeDiff * 3600));
    String msg = timeString(now.hour()) + ':' + timeString(now.minute()) + ':' + timeString(now.second());
    Serial.println(msg);
    Serial.println(reqData);
  }

  // Send the request
  log("curl -H \"X-Pvoutput-Apikey: %s\" -H \"X-Pvoutput-SystemId: %u\" \"http://pvoutput.org/service/r2/getstatus.jsp\"", "<private>", config.systemId);
  if (!request->send((uint8_t*)reqData.c_str(), reqData.length()))
  {
    // Try again in a little while
    trace(T_pvoutput,23);
    log("Sending GET request failed");
    delete request;
    request = nullptr;
    return UNIXtime() + 5;
  }

  trace(T_pvoutput,24);
  SetState(State::QUERY_GET_STATUS_WAIT_RESPONSE);
  return 1;
}

//=============================================================================
uint32_t PVOutput::TickQueryGetStatusWaitResponse(struct serviceBlock* serviceBlock)
{
  // If not completed, return to wait.
  trace(T_pvoutput,25);

  if(request->readyState() != 4)
  {
    // @todo Tick right away or wait 1 sec?
  trace(T_pvoutput,26);
    return UNIXtime() + 1;
  }

  trace(T_pvoutput,27);
  HTTPrequestFree++;
  String responseText = request->responseText();
  int responseCode = request->responseHTTPcode();
  delete request;
  request = nullptr;

  if(responseCode != 200)
  {
    trace(T_pvoutput,28);
    log("pvoutput: last entry query failed: %d : %s", responseCode, responseText.c_str());
    // @todo test case where no data at all in pvoutput and handle specially. Dont know what message that has yet.
    switch (InterpretPVOutputError(responseCode, responseText))
    {
    // Wait for a while and try again errors
    case PVOutputError::RATE_LIMIT:
      trace(T_pvoutput,29);
      return UNIXtime() + config.reportInterval;

    // Retry errors (Would reset the PVOutput service but it is already in first state)
    default:
    case PVOutputError::NONE:
    case PVOutputError::UNMAPPED_ERROR:
    case PVOutputError::DATE_TOO_OLD:
    case PVOutputError::DATE_IN_FUTURE:
      trace(T_pvoutput,30);
      SetState(State::QUERY_GET_STATUS);
      return UNIXtime() + 1;
    }
  }

  // Parse the date-time from the response text
  DateTime dt;
  if (!ParseGetStatusResponse(responseText, &dt))
  {
    trace(T_pvoutput,31);
    log("Failed to parse get status response from PVOutput trying request again : %s", responseText.c_str());
    SetState(State::QUERY_GET_STATUS);
    return UNIXtime() + 1;
  }

  // The datetime was given by PVOutput in local time, we need to adjust to UTC
  unixPrevPost = dt.unixtime()  - (localTimeDiff * 3600);

  // Cases we care about:
  // * get normal : prev=get, next=get+interval, day=day of prev(or day of next after adjust) : adjust for span day boundary
  //    Might end up with a 23:59 after adjust for span day boundary(23:59)
  // * get 23:59:59 day end : prev=00-interval(not accurate), next=00:00, day=day of 00:00 (0 energy)
  // * get 00:00:00 day start (same as normal): prev=00, next=00+interval, day=day of 00:00 (basically same as normal but adjust not required)
  if (dt.hour() == 23 && dt.minute() == 59 && dt.second() == 59)
  {
    // Special case, already posted day end, now need to post day start

    // The next post is to be 00:00:00 (which is 1 sec in the future of the read prev post from get status)
    unixPrevPost += 1;
    unixNextPost = unixPrevPost;

    // The prev post needs to be one report interval in the past from 00:00:00 (If using 5 min intervals this is 23:55:00)
    // This actually should be whatever the prev post interval was used to report 23:59:59, but we dont have that information
    // We are assuming the report interval hasn't changed since the post that was reported to PVOutput. This is not
    // necessarily correct but usually correct. Getting this wrong means the energy values are accurate but the instant power
    // usage values for the 00:00:00 post may not be correct but instead based on the current reporting interval which is
    // close enough.
    //
    // Because it is an instantaneous value, it has no bearing on overall power usage for the day.
    unixPrevPost = unixPrevPost - config.reportInterval;
    unixPrevPost -= unixPrevPost % config.reportInterval;
  }
  else
  {
    // Adjust to a report interval boundary (only really matters if changing interval)
    unixPrevPost -= unixPrevPost % config.reportInterval;
    unixNextPost = unixPrevPost + config.reportInterval;

    // If the prev/next crosses a day boundary then set next to 23:59:59
    DateTime prevDt(unixPrevPost + (localTimeDiff * 3600));
    DateTime nextDt(unixNextPost + (localTimeDiff * 3600));
    if (prevDt.year() != nextDt.year() || prevDt.month() != nextDt.month() || prevDt.day() != nextDt.day())
    {
      // Spans a day boundary, adjust to 23:59 as a special case to post the last entry in the prev day
      nextDt = DateTime(prevDt.year(), prevDt.month(), prevDt.day(), 23, 59, 59);
    }

    unixDayStart = CalculateDayStart(unixNextPost);
  }

  // For pvoutput we have to report energy accumulated each day (in addition to accumulated since last tick), 
  // so we need to read the last record seen the day before and use that as the "reference"
  // When the day ticks over, then we will update the reference
  trace(T_pvoutput,35);
  log("unixDayStart: %s, unixPrevPost: %s, unixNextPost: %s, now: %s, lastKey: %s", 
    dateString(unixDayStart).c_str(),
    dateString(unixPrevPost).c_str(),
    dateString(unixNextPost).c_str(),
    dateString(UNIXtime()).c_str(),
    dateString(currLog.lastKey()).c_str()
    );

  assert(reqData.length() == 0);
  assert(reqEntries == 0);
  reqData += PVOUTPUT_POST_DATA_PREFIX;
  SetState(State::COLLATE_DATA);
  return unixNextPost + 1;
}

//=============================================================================
bool PVOutput::ReadSaneLogRecordOrPrev(IotaLogRecord* record, uint32_t when)
{
  // @todo Verify but I think that the logReadKey already gets record or prev
  record->UNIXtime = when;
  bool ret = logReadKey(record) != 0;

  // @todo Old code was looking at currLog.firstKey(), currLog.lastKey() and other things to figure out what was going on
  // There was also code to manually search back in time using:
  //int rkResult = 1;
  //do {
  //rkResult = currLog.readKey(dayStartRecord);
  //if(rkResult != 0) {
  //trace(T_pvoutput, 9);
  //dayStartRecord->UNIXtime -= 1;
  //}
  //} while(rkResult != 0 && dayStartRecord->UNIXtime > currLog.firstKey());

  // Check for NaN
  if(record->serial != record->serial)
  {
    trace(T_pvoutput,36);
    record->serial = 0;
  }

  if(record->UNIXtime != record->UNIXtime)
  {
    trace(T_pvoutput,37);
    record->UNIXtime = 0;
  }

  if(record->logHours != record->logHours)
  {
    trace(T_pvoutput,38);
    record->logHours = 0;
  }

  for(int i = 0; i < maxInputs; i++) 
  {
    if(record->accum1[i] != record->accum1[i]) 
    {
      trace(T_pvoutput,39);
      record->accum1[i] = 0;
    }

    if(record->accum2[i] != record->accum2[i]) 
    {
      trace(T_pvoutput,40);
      record->accum2[i] = 0;
    }
  }
}

//=============================================================================
void PVOutput::IncrementTimeInterval(size_t incrementPeriods, const char* entryDebug)
{
  // Cases we care about:
  // unixNextPost 23:59:59: prev:<keep old: 23:55:00>, next:00:00:00, day: new (day of next)
  // unixNextPost (normal): prev=next, next=next+interval (adjust day boundary), day=day of next (after adjust)
  DateTime localPrevPostDt(unixPrevPost + (localTimeDiff * 3600));
  DateTime localNextPostDt(unixNextPost + (localTimeDiff * 3600));

  if (localNextPostDt.hour() == 23 && localNextPostDt.minute() == 59 && localNextPostDt.second() == 59)
  {
    // Keep same previous (for power values)
    // Get a new day (used for energy calcs) : Will come from unixNextPost

    // One second should move to 00:00:00 of next day
    unixNextPost = unixNextPost + 1;
    localNextPostDt = DateTime(unixNextPost + (localTimeDiff * 3600));

    // Expect the date to change
    assert(
      localPrevPostDt.year() != localNextPostDt.year()
      || localPrevPostDt.month() != localNextPostDt.month()
      || localPrevPostDt.day() != localNextPostDt.day());

    // Expect to be time 00:00:00
    assert(
      localNextPostDt.hour() == 0
      || localNextPostDt.minute() == 0
      || localNextPostDt.second() == 0);

    // Note: If incrementPeriods > 1 then we will still just increment one for now. This is a special case we shouldnt skip
  }
  else
  {
  	// @todo I think this can be simplified a bit
    // Otherwise just do a normal increment but handle crossing the day boundary
    unixNextPost += (incrementPeriods * config.reportInterval);
    //log("unixNextPost: %u, mod: %u local next: %s", (unsigned int)unixNextPost, (unsigned int)(unixNextPost % config.reportInterval), dateString(unixNextPost).c_str());
    //if (!((unixNextPost % config.reportInterval) == 0))
    //{
    //  log("ERROR ASSERT LOST LOGS");
    //}
    assert((unixNextPost % config.reportInterval) == 0);
    localPrevPostDt = localNextPostDt;
    localNextPostDt = DateTime(unixNextPost + (localTimeDiff * 3600));
    if (localPrevPostDt.year() != localNextPostDt.year() 
      || localPrevPostDt.month() != localNextPostDt.month()
      || localPrevPostDt.day() != localNextPostDt.day())
    {
      // Date changed, need to handle this specially by setting next to either 23:59:59 of prev day or 00:00:00 of current day 
      // in order to post the special day end or day start entries.
      //
      // If just incrementing by 1 period the case of 00:00:00 is not relevant as we always want to post day-end, 
      // however because we can skip multiple days when there is no data in the log we may not want an entry at 
      // the end of the previous day as there may be no data in the IoTa log for that day.
      //
      // We will look at prev day and the day before next day and determine if we need to post prev day

      // Notice the localNextPostDt.unixtime() - 1 that will set to 23:59:59 of the day before the day next is in
      localNextPostDt = DateTime(localNextPostDt.year(), localNextPostDt.month(), localNextPostDt.day(), 0, 0, 0);
      unixNextPost = (localNextPostDt.unixtime() - 1)  - (localTimeDiff * 3600);

      // If the day before next is the same day as prev, then we need 23:59:59 to finish off that day
      // otherwise we just skip it and move to 00:00:00 to start the new day
      DateTime dtDayBeforeNext(unixNextPost + (localTimeDiff * 3600));
      if (dtDayBeforeNext.year() != localPrevPostDt.year()
        || dtDayBeforeNext.month() != localPrevPostDt.month()
        || dtDayBeforeNext.day() != localPrevPostDt.day())
      {
        // Move from 23:59:59 to 00:00:00 of the next day
        unixNextPost += 1;
        localNextPostDt = DateTime(unixNextPost + (localTimeDiff * 3600));
        unixPrevPost = unixNextPost - config.reportInterval;
        log("Date changed between prev: %s and next: %s and there is no data in day before next, so moving to day-start-post 00:00:00 of next day", dateString(unixPrevPost).c_str(), dateString(unixNextPost).c_str());
      }
      else
      {
        // stay with 23:59:59 of the day before next to do a day-end post
        log("Date changed between prev: %s and next: %s and there is some data in day before next, so moving to do day-end-post 23:59:59 of day before next", dateString(unixPrevPost).c_str(), dateString(unixNextPost).c_str());
        unixPrevPost = unixNextPost + 1 - config.reportInterval;
      }
    }
    else
    {
      unixPrevPost = unixNextPost - config.reportInterval;
    }

    localPrevPostDt = DateTime(unixPrevPost + (localTimeDiff * 3600));
  }

  const char* message = "";
  if (localPrevPostDt.day() != localNextPostDt.day() || localPrevPostDt.month() != localNextPostDt.month() || localPrevPostDt.year() != localNextPostDt.year())
  {
    trace(T_pvoutput,41);
    message = "Started a new day for log accumulation";
  }
  else
  {
    trace(T_pvoutput,42);
    message = "Still in same day for log accumulation";
  }
  unixDayStart = CalculateDayStart(unixNextPost);

  log("Entry: %s : %s : After incrementing %u periods the new values are: unixDayStart: %s, unixPrevPost: %s, unixNextPost: %s, now: %s, lastKey: %s", 
    entryDebug,
    message,
    (unsigned int)incrementPeriods,
    dateString(unixDayStart).c_str(),
    dateString(unixPrevPost).c_str(),
    dateString(unixNextPost).c_str(),
    dateString(UNIXtime()).c_str(),
    dateString(currLog.lastKey()).c_str()
    );
}

//=============================================================================
bool PVOutput::CalculateEntry(Entry* entry, const IotaLogRecord& prevPostRecord, const IotaLogRecord& nextPostRecord, const IotaLogRecord& dayStartRecord)
{
  // Entry is for unixNextTime and reports data from the last config.reportInterval time
  entry->unixTime = unixNextPost;

  // @todo I think we can remove this from entry again now
  // PVOutput requires localized time, include it in the entry
  uint32_t localUnixTime = entry->unixTime + (localTimeDiff * 3600);
  entry->dt = DateTime(localUnixTime);

  // The measurements should be such that:
  // chan 1 : mains +ve indicates net import -ve indicates net export
  // chan 2 : solar -ve indicates generation +ve should never really happen would indicate solar panels using power

  // Find the mean voltage since last post
  int voltageChannel = -1;
  if(config.mainsChannel >= 0) 
  {
    voltageChannel = inputChannel[config.mainsChannel]->_vchannel;
  }
  else if(config.solarChannel >= 0) 
  {
    voltageChannel = inputChannel[config.solarChannel]->_vchannel;
  }

  double logHours = 0;
  if (nextPostRecord.logHours != prevPostRecord.logHours)
  {
    logHours = nextPostRecord.logHours - prevPostRecord.logHours;
  }

  entry->voltage = 0;
  if (voltageChannel >= 0 && nextPostRecord.logHours != prevPostRecord.logHours)
  {
    trace(T_pvoutput,43);
    entry->voltage = (nextPostRecord.accum1[voltageChannel] - prevPostRecord.accum1[voltageChannel]);
    entry->voltage /= logHours;
  }


  // Energy is calculated since begining of the day
  //
  // Generated energy is always a negative value in our calculations.
  // I.e. I assume most power channels in existing iotawatt indicate
  // power USAGE of that channel and are positive. In fact many have
  // enabled ability to force them positive in the config.
  //
  // By using a negative value to indicate power generated/exported we have
  // a consitent view of things
  //
  // Because a solar channel always generates and never uses power, we
  // will enforce it has a negative value in the case the CT has been
  // installed in reverse. 
  entry->energyGenerated = 0;
  if(config.solarChannel >= 0) 
  {
    trace(T_pvoutput,44);
    entry->energyGenerated = nextPostRecord.accum1[config.solarChannel] - dayStartRecord.accum1[config.solarChannel];
  }

  // Find out how much energy we imported from the main line
  double energyImported = 0.0;
  if(config.mainsChannel >= 0) 
  {
    trace(T_pvoutput,45);
    energyImported = nextPostRecord.accum1[config.mainsChannel] - dayStartRecord.accum1[config.mainsChannel];
  }

  // the mean power used in W since the last post
  entry->powerGenerated = 0;
  if (config.solarChannel >= 0 && nextPostRecord.logHours != prevPostRecord.logHours) 
  {
    trace(T_pvoutput,46);
    entry->powerGenerated = nextPostRecord.accum1[config.solarChannel] - prevPostRecord.accum1[config.solarChannel];
    entry->powerGenerated = entry->powerGenerated / (nextPostRecord.logHours - prevPostRecord.logHours);
  }

  // Find out how much energy we imported from the main line
  double powerImported = 0.0;
  if (config.mainsChannel >= 0 && nextPostRecord.logHours != prevPostRecord.logHours) 
  {
    trace(T_pvoutput,48);
    powerImported = nextPostRecord.accum1[config.mainsChannel] - prevPostRecord.accum1[config.mainsChannel];
    powerImported = powerImported / (nextPostRecord.logHours - prevPostRecord.logHours);
  }

  if (solarChannelReversed)
  {
    entry->energyGenerated *= -1;
    entry->powerGenerated *= -1;
  }

  // How many watts we permit it to report when actual power drain is 0
  // There appears to be about 0.6W of usage when no CT is plugged in
  const double PERMITTED_POWER_ZERO_ERROR = 1.0;

  if(entry->powerGenerated > PERMITTED_POWER_ZERO_ERROR)
  {
    log("pvoutput: At time: %s config appears incorrect or CT on solar is backwards. Power usage of solar channel is expected to be negative but is: %lfW", dateString(entry->unixTime).c_str(), entry->powerGenerated);
    entry->energyGenerated *= -1;
    entry->powerGenerated *= -1;
    solarChannelReversed = !solarChannelReversed;
  }

  if(entry->energyGenerated > logHours * PERMITTED_POWER_ZERO_ERROR)
  {
    log("pvoutput: Warning at time: %s even after reversal solar energy usage (%lf) is not negative. Something is wrong as power usage and energy usage have different signs for solar", dateString(entry->unixTime).c_str(), entry->energyGenerated);
    trace(T_pvoutput,47);
  }

  if (mainsChannelReversed)
  {
    energyImported *= -1;
    powerImported *= -1;
  }

  // If we are exporting more than we are generating something is wrong
  if ((powerImported + PERMITTED_POWER_ZERO_ERROR) < (entry->powerGenerated - PERMITTED_POWER_ZERO_ERROR))
  {
    trace(T_pvoutput,49);
    //log(
    //  "pvoutput: PVOutput configuration is incorrect. Appears we are exporting more power than we are generating. Between " + String(nextPostRecord->UNIXtime) + " and " + String(prevPostRecord->UNIXtime) 
    //  + " we imported: " + String(powerImported) + "Wh and generated: " + String(powerGenerated) + "Wh");
    log("pvoutput: At time: %s config appears incorrect or CT on mains import/export is backwards. Power imported: %lfW is less than solar power used: %lf swapping sign of power imported. We are pushing more power to the grid than we are generating via solar", dateString(entry->unixTime).c_str(), powerImported, entry->powerGenerated);
    energyImported *= -1;
    powerImported *= -1;
    mainsChannelReversed = !mainsChannelReversed;
  }

  if ((energyImported + (logHours * PERMITTED_POWER_ZERO_ERROR) < entry->energyGenerated - (logHours * PERMITTED_POWER_ZERO_ERROR)))
  {
    log("pvoutput: Warning at time: %s even after reversal mains energy usage (%lf) is not less than solar energy generation (%lf)", dateString(entry->unixTime).c_str(), energyImported, entry->energyGenerated);
    trace(T_pvoutput,47);
  }

  // Example:
  // generated = -5kWh
  // imported = 2kWh
  // thus we are consuming 7kWh as we are using all the 5kWh and an additional 2kWh from mains import
  entry->energyConsumed = energyImported - entry->energyGenerated;

  // Example:
  // generated = -5kW
  // imported = 2kW
  // thus we are consuming 7kW as we are using all the 5kW and an additional 2kW from mains import
  entry->powerConsumed = powerImported - entry->powerGenerated;

  return true;
}

//=============================================================================
String PVOutput::GenerateEntryString(Entry entry)
{
  // PVOutput expects reports as positive values, our internal calculations
  // expect negative values for generation so just convert them now
  entry.energyGenerated *= -1;
  entry.powerGenerated *= -1;

  // Now sanity check the data so we dont get PVOutput infinite POST loop
  // due to known problems
  if(entry.energyGenerated < 0.0) 
  {
    trace(T_pvoutput,50);
    //msgLog("pvoutput: energyGenerated: " + String(energyGenerated) + "Wh is invalid. Are the CT coils installed in the correct direction? PVOutput wont accept negative values force changing it to 0.0");
    entry.energyGenerated = 0.0;
  }

  if(entry.powerGenerated < 0.0) 
  {
    trace(T_pvoutput,51);
    //msgLog("pvoutput: powerGenerated: " + String(powerGenerated) + "W is invalid. Are the CT coils installed in the correct direction? PVOutput wont accept negative values force changing it to 0.0");
    entry.powerGenerated = 0.0;
  }

  if(entry.energyConsumed < 0.0) 
  {
    trace(T_pvoutput,52);
    //msgLog("pvoutput: energyConsumed: " + String(energyConsumed) + "Wh is invalid. Are the CT coils installed in the correct direction? PVOutput wont accept negative values force changing it to 0.0");
    entry.energyConsumed = 0.0;
  }

  if(entry.powerConsumed < 0.0) 
  {
    trace(T_pvoutput,53);
    //msgLog("pvoutput: powerConsumed: " + String(powerConsumed) + "W is invalid. Are the CT coils installed in the correct direction? PVOutput wont accept negative values force changing it to 0.0");
    entry.powerConsumed = 0.0;
  }

  char dateStr[10];
  snprintf(dateStr, sizeof(dateStr), "%04u%02u%02u", entry.dt.year(), entry.dt.month(), entry.dt.day());
  dateStr[sizeof(dateStr) - 1] = 0;

  char timeStr[10];
  snprintf(timeStr, sizeof(timeStr), "%02u:%02u", entry.dt.hour(), entry.dt.minute());
  timeStr[sizeof(timeStr) - 1] = 0;

  //Date	Yes	yyyymmdd	date	20100830	r1	
  //Time	Yes	hh:mm	time	14:12	r1	
  //Energy Generation	Yes	number	watt hours	10000	r1	
  //Power Generation	No	number	watts	2000	r1	
  //Energy Consumption	No	number	watt hours	10000	r1	
  //Power Consumption	No	number	watts	2000	r1	
  //Temperature	No	decimal	celsius	23.4	r1	
  //Voltage	No	decimal	volts	240.7	r1	

  return String(dateStr)
    + "," + String(timeStr) 
    + "," + String(entry.energyGenerated) 
    + "," + String(entry.powerGenerated) 
    + "," + String(entry.energyConsumed) 
    + "," + String(entry.powerConsumed) 
    + "," // temperature
    + "," + String(entry.voltage);
}

//=============================================================================
static bool logReadNextKey(IotaLogRecord* callerRecord)
{
  // We want to read the first record with time >= callerRecord->UNIXtime + 1
  uint32_t key = callerRecord->UNIXtime + 1;

  // If key is found in curr log, or in future, or greater than the hist log last key
  // then we will use a log record from the curr log
  if (key >= currLog.firstKey() || key > histLog.lastKey())
  {
    // Read the key to obtain the serial
    // @todo Assuming this returns record for key or earlier. If not then need to handle the key < currLog.firstKey() and key > histLog.lastKey() case. I.e. return the first item in the curr log
    if (currLog.readKey(callerRecord) != 0)
    {
      // Failed to read because there is nothing in the log at all
      if (histLog.fileSize() == 0)
      {
        trace(T_pvoutput,54);
        return false;
      }

      // @todo is this unreachable?
      trace(T_pvoutput,55);
      assert(!"Unreachable");
      return false;
    }

    // If what we ended up reading was less than what we want then read the next key
    if (callerRecord->UNIXtime < key)
    {
      trace(T_pvoutput,56);
      if (currLog.readNext(callerRecord) != 0)
      {
        trace(T_pvoutput,57);
        return false;
      }
    }

    // @todo Assert that the prev record is smaller to make sure we did the correct thing
    if (callerRecord->serial > 0)
    {
      trace(T_pvoutput,58);
      IotaLogRecord tmpRecord;
      assert(currLog.readSerial(&tmpRecord, callerRecord->serial - 1) == 0);
      assert(tmpRecord.UNIXtime < key);
    }
      trace(T_pvoutput,59);
    assert(callerRecord->UNIXtime >= key);
    return true;
  }
  else
  {
    // Will either get data from histLog or there is no data

    // Read the key to obtain the serial
    // @todo Assuming this returns record for key or earlier. If not then need to handle the key < currLog.firstKey() and key > histLog.lastKey() case. I.e. return the first item in the curr log
    if (histLog.readKey(callerRecord) != 0)
    {
      // In this case I expect the histLog to be empty
      trace(T_pvoutput,60);
      assert(histLog.fileSize() == 0);
      return false;
    }

    // If what we ended up reading was less than what we want then read the next key
    if (callerRecord->UNIXtime < key)
    {
      trace(T_pvoutput,61);
      if (histLog.readNext(callerRecord) != 0)
      {
        trace(T_pvoutput,62);
        assert(!"I think this should have used currLog and not reached here.");
        return false;
      }
    }

    // @todo Assert that the prev record is smaller to make sure we did the correct thing
    if (callerRecord->serial > 0)
    {
      trace(T_pvoutput,63);
      IotaLogRecord tmpRecord;
      assert(histLog.readSerial(&tmpRecord, callerRecord->serial - 1) == 0);
      assert(tmpRecord.UNIXtime < key);
    }
    trace(T_pvoutput,64);
    assert(callerRecord->UNIXtime >= key);
    return true;
  }
}

//=============================================================================
// @todo Dont think these prev/next records need to be ref, maybe const ref or maybe even not at all do the log in the caller
size_t PVOutput::CalculateMissingPeriodsToSkip(IotaLogRecord& prevPostRecord, IotaLogRecord& nextPostRecord)
{
  trace(T_pvoutput,65);
  log("No difference in recorded time between records serial:%d %s(for expected: %s) - serial:%d %s(for expected: %s) (IoTa wasnt running during that period). Wont post anything as we have no data",
    prevPostRecord.serial,
    dateString(prevPostRecord.UNIXtime).c_str(),
    dateString(unixPrevPost).c_str(),
    nextPostRecord.serial,
    dateString(nextPostRecord.UNIXtime).c_str(),
    dateString(unixNextPost).c_str()
  );

  // To make it clearer will rename to "currentPostTime"
  uint32_t currentPostTime = unixNextPost;

  // To make it clearer will rename nextPostRecord. Note: These records are big so we dont
  // just create a new temporary but reuse the existing temporaries memory
  IotaLogRecord& nextAvailableLogRecord(nextPostRecord);

  // Rather than looping through the state machine many times when there is a big hole in the log
  // We will instead find the next record available in the log and all the periods 
  // that are covered by that hole will be skipped in one go.
  //
  // This is common when the IoTa has been switched off for a period of time and saves 
  // a lot of time for it to catchup.
  nextAvailableLogRecord.UNIXtime = currentPostTime;
  if (!logReadNextKey(&nextAvailableLogRecord))
  {
    trace(T_pvoutput,66);
    log("Failed to read next record from the log. Do a normal increment as fallback.");
    return 1;
  }

  // If the nextAvailableLogRecord <= currentPostTime+interval then we do a normal increment of: 1
  // If the nextAvailableLogRecord <= currentPostTime+2xinterval then we do a increment of: 2
  // If the nextAvailableLogRecord <= currentPostTime+3xinterval then we do a increment of: 3
  // ...
  size_t wholeReportPeriodsToSkip = (nextAvailableLogRecord.UNIXtime - currentPostTime) / config.reportInterval;

  // Note: If it is on the boundary we want <= not < so we need to subtract one from the wholeReportPeriodsToSkip
  if (((nextAvailableLogRecord.UNIXtime - currentPostTime) % config.reportInterval) == 0)
  {
    trace(T_pvoutput,67);
    --wholeReportPeriodsToSkip;
  }

  if (wholeReportPeriodsToSkip > 0)
  {
    trace(T_pvoutput,68);
    log("Read next log from file: serial:%d %s so skipping: %u reports from: %s to %s", 
      nextAvailableLogRecord.serial,
      dateString(nextAvailableLogRecord.UNIXtime).c_str(),
      (unsigned int)wholeReportPeriodsToSkip,
      dateString(unixNextPost).c_str(),
      dateString(unixNextPost + (wholeReportPeriodsToSkip * config.reportInterval)).c_str()
      );
  }
  else
  {
    trace(T_pvoutput,69);
    // Always skip at least 1 record
    log("No remaining hole in the log read next record serial:%d %s. Using standard time increment.", nextAvailableLogRecord.serial, dateString(nextAvailableLogRecord.UNIXtime).c_str());
  }

  return wholeReportPeriodsToSkip + 1;
}

//=============================================================================
void PVOutput::WriteEntryString(const String& entry_str)
{
  if (reqData.length() > strlen(PVOUTPUT_POST_DATA_PREFIX))
  {
    // If already one item in the reqData, then separate this one with a ;
    reqData += ";";
  }
  reqData += entry_str;
  ++reqEntries;
  //log("Add entry: %s", entry_str.c_str());
}

//=============================================================================
bool PVOutput::CollectNextDataPoint()
{
  // Make sure it is not older than max addstatus API will permit us to post
  uint32_t now = UNIXtime();
  if (unixNextPost + MAX_PAST_POST_TIME < now)
  {
    trace(T_pvoutput,33);
    uint32_t oldestAcceptable = now - MAX_PAST_POST_TIME;
    uint32_t diff = oldestAcceptable - unixNextPost;
    size_t periodsToSkip = (diff / config.reportInterval) + 1;
    log("unixNextPost: %s is too old and PVOutput API will not accept this data, we are going to skip: %u periods to set it to a time that will be accepted by PVOutput", dateString(unixNextPost).c_str(), (unsigned int)periodsToSkip);
    IncrementTimeInterval(periodsToSkip, "<no entry> Unposted data too far in past, PVOutput API wont accept it so skipping");
    return true;  
  }

  IotaLogRecord prevPostRecord;
  if (!ReadSaneLogRecordOrPrev(&prevPostRecord, unixPrevPost))
  {
    trace(T_pvoutput,70);
    log("Failed to read prev post log record");
    // Dont move forward on failure so we get a bug report
    return false;
  }

  // Special case for day end entry, we will read at time 00:00:00 and post it for 23:59:59
  DateTime localNextPostDt(unixNextPost + (localTimeDiff * 3600));
  uint32_t additional_time = 0;
  if (localNextPostDt.hour() == 23 && localNextPostDt.minute() == 59 && localNextPostDt.second() == 59)
  {
    additional_time = 1;
  }
  IotaLogRecord nextPostRecord;
  if (!ReadSaneLogRecordOrPrev(&nextPostRecord, unixNextPost + additional_time))
  {
    trace(T_pvoutput,71);
    log("Failed to read next post log record");
    // Dont move forward on failure so we get a bug report
    return false;
  }

  // Compute the time difference between log entries.
  // If zero, then IoTa wasn't running during that period we will skip posting for it and 
  // potentially more periods depending on how long the hole in the IoTa log is
  //
  // Note: When we have a gap, we will post the day start entry so we must not skip the 00:00:00 record
  double elapsedHours = nextPostRecord.logHours - prevPostRecord.logHours;
  if(elapsedHours == 0 && !(localNextPostDt.hour() == 0 && localNextPostDt.minute() == 0 && localNextPostDt.second() == 0))
  {
    trace(T_pvoutput,72);
    size_t periodsToSkip = CalculateMissingPeriodsToSkip(prevPostRecord, nextPostRecord);
    IncrementTimeInterval(periodsToSkip, "<no entry> Skipping empty IoTa log entries");
    return true;  
  }
  trace(T_pvoutput,73);

  // Otherwise we have some data we need to POST, get the remaining data and
  // prepare the request
  IotaLogRecord dayStartRecord;
  if (!ReadSaneLogRecordOrPrev(&dayStartRecord, unixDayStart))
  {
    trace(T_pvoutput,74);
    log("Failed to read day start log record");
    // Dont move forward on failure so we get a bug report
    return false;
  }

  Entry entry;
  if (!CalculateEntry(&entry, prevPostRecord, nextPostRecord, dayStartRecord))
  {
    trace(T_pvoutput,75);
    // Dont move forward on failure so we get a bug report
    return false;
  }

  // Generate the entry string and write it to the reqData
  String entry_str = GenerateEntryString(entry);
  WriteEntryString(entry_str);

  // Do a standard time increment
  IncrementTimeInterval(1, entry_str.c_str());
  return true;
}

//=============================================================================
uint32_t PVOutput::TickCollateData(struct serviceBlock* serviceBlock)
{
  // @todo Not sure why this is required. Feel it should be removed, as if req has outstanding data it should have been handled and cleared already
  assert(request == nullptr);
  if (request != nullptr && request->readyState() < 4 && reqData.length() >= config.reqDataLimit)
  {
    log("An outstanding request is incomplete try again soon");
    return 1; 
  }  

  // @todo Other services handle STOP bool here so the stop happens in the state machine tick defined location. May still require that if we see issues with syncrhonous stop we implemented

  // If buffer isn't full, add another measurement.
  if (reqData.length() < config.reqDataLimit && unixNextPost <= currLog.lastKey())
  {
    CollectNextDataPoint();
  }

  // If we have any unposted entries and the next post requires a wait time in the future
  // then we will post these right away.
  // I.e. Always post what we right away if we will need to wait for more data.
  //
  // @todo Calling UnixTime is VERY slow. Instead use time from service?
  bool realtimePost = false;
  if (reqEntries >= config.bulkSend && unixNextPost >= UNIXtime())
  {
    realtimePost = true;
  }

  // Is the data ready to be posted to PVOutput
  bool isRequestAvailable = ((request == nullptr || request->readyState() == 4) && HTTPrequestFree);
  bool isRequestBufferFull = reqEntries >= MAX_BULK_SEND || reqData.length() >= config.reqDataLimit;
  if (isRequestAvailable && (realtimePost || isRequestBufferFull))
  {
    trace(T_pvoutput,76);
    //log("Got enough entries, posting to server now");
    SetState(State::POST_DATA);
    return 1;
  }

  //log("After collection, waiting until %s before next tick, now is: %s", dateString(unixNextPost).c_str(), dateString(UNIXtime()).c_str());
  return unixNextPost;
}

//=============================================================================
uint32_t PVOutput::TickPostData(struct serviceBlock* serviceBlock)
{
  //log("Sending post");
  trace(T_pvoutput,77);

  if (! WiFi.isConnected())
  {
    trace(T_pvoutput,78);
    //log("Waiting for wifi to connect again");
    return UNIXtime() + 1;
  }

  // Make sure there's enough memory
  if (ESP.getFreeHeap() < 15000)
  {
    trace(T_pvoutput,79);
    log("Insufficient memory waiting for it to free up");
    return UNIXtime() + 1;
  }

  if (!HTTPrequestFree)
  {
    log("Insufficient http requests available waiting for it to free up");
    return 1;
  }
  HTTPrequestFree--;

  // @todo we can assert it is null?
  if (request == nullptr)
  {
    request = new asyncHTTPrequest;
  }

  // API Documented at: https://pvoutput.org/help.html#api-addbatchstatus
  request->setTimeout(config.httpTimeout);
  request->setDebug(ENABLE_HTTP_DEBUG);
  // NOTE: asyncHTTPrequest seems to require upper case HTTP
  request->open("POST", "HTTP://pvoutput.org/service/r2/addbatchstatus.jsp");
  request->setReqHeader("Host", "pvoutput.org"); 
  request->setReqHeader("Content-Type", "application/x-www-form-urlencoded"); 
  request->setReqHeader("X-Pvoutput-Apikey", config.apiKey); 
  String sid = String(config.systemId);
  request->setReqHeader("X-Pvoutput-SystemId", sid.c_str());
  // reqData already has all the data we want to POST in it
  // This data should also already be prefixed with: 
  // c1=0&n=0&data=
  trace(T_pvoutput,80);
  if(request->debug())
  {
    Serial.println(ESP.getFreeHeap()); 
    DateTime now = DateTime(UNIXtime() + (localTimeDiff * 3600));
    String msg = timeString(now.hour()) + ':' + timeString(now.minute()) + ':' + timeString(now.second());
    Serial.println(msg);
    Serial.println(reqData);
  }

  trace(T_pvoutput,81);
  // @todo check bool error
  log("curl -d \"%s\" -H \"X-Pvoutput-Apikey: %s\" -H \"X-Pvoutput-SystemId: %u\" \"http://pvoutput.org/service/r2/addbatchstatus.jsp\"", reqData.c_str(), "<private>", config.systemId);
  if (!request->send((uint8_t*)reqData.c_str(), reqData.length()))
  {
    // Try again in a little while
    trace(T_pvoutput,82);
    log("Sending POST request failed");
    delete request;
    request = nullptr;
    return UNIXtime() + 5;
  }
  SetState(State::POST_DATA_WAIT_RESPONSE);
  return 1;
}

//=============================================================================
uint32_t PVOutput::TickPostDataWaitResponse(struct serviceBlock* serviceBlock)
{
  trace(T_pvoutput,83);
  assert(request != nullptr);

  // If not yet ready, then wait
  if (request->readyState() != readyStateDone)
  {
    //log("POST response ticked but response not yet ready, waiting for 1 sec");
    return UNIXtime() + 1;
  }

  HTTPrequestFree++;
  trace(T_pvoutput,84);

  // @todo Bad API in asyncHTTPrequest calling responseText() erases buffer so only able to request it once
  int responseCode = request->responseHTTPcode();
  String responseText = request->responseText();
  delete request;
  request = nullptr; 

  if(responseCode != 200)
  {
    trace(T_pvoutput,85);
    log("pvoutput: Post Failed: %d : %s", responseCode, responseText.c_str());
    switch (InterpretPVOutputError(responseCode, responseText))
    {
      // This one will fall through and be treated like a success
      // I.e. We are skipping this data
      case PVOutputError::DATE_TOO_OLD:
        trace(T_pvoutput,86);
        log("Skipping data that is known to be too old and will never be accepted by pvoutput. Fall through treat like success case");
        // Break to treat as success case resulting in data being skipped
        break;

      // @todo Add back in after testing. For now if we see this may be a bug
      //case PVOutputError::MOON_POWERED:
      //  trace(T_pvoutput,86);
      //  log("Skipping data that PVOutput thinks is invalid. Fall through treat like success case");
      //  // Break to treat as success case resulting in data being skipped
      //  break;

      // In these cases we will retry sending after a small wait
      case PVOutputError::DATE_IN_FUTURE:
      case PVOutputError::RATE_LIMIT:
        // Retry upto 2 hours worth and then fall through to reset
        // The extra time is because the DATE_IN_FUTURE is a common error when IoTaWatt is 
        // using the incorrect local time due to daylight savings (currently no support to handle that)
        //
        // There are issues with the midnight time boundary and PVOutput I have seen 
        // it fail with DATE_IN_FUTURE upto 1 hour past expected time.
        trace(T_pvoutput,87);
        ++retryCount;
        if (retryCount < ((2 * 60 * 60) / config.reportInterval) + 2)
        {
          trace(T_pvoutput,88);
          SetState(State::POST_DATA);
          log("pvoutput: Retrying post again in %u seconds", config.reportInterval);
          return UNIXtime() + config.reportInterval;
        }
        // Fall through to reset state machine

      case PVOutputError::NONE:
      case PVOutputError::UNMAPPED_ERROR:
      default:
        trace(T_pvoutput,89);
        log("Generic PVOutput error resetting state machine");
        Start();
        return 1;
    }
  }
  //log("pvoutput: Batch POST was successful");

  // POST was successful, go back into loop reading new post data
  trace(T_pvoutput,90);
  retryCount = 0;
  reqData = "";
  reqEntries = 0;
  reqData = PVOUTPUT_POST_DATA_PREFIX;
  SetState(State::COLLATE_DATA);
  return 1;
}

//=============================================================================
