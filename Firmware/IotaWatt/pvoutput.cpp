#include "IotaWatt.h"

#define assert(Condition)                                                  \
  \
if(!(Condition)) \
{                                                        \
    msgLog("pvoutput: ASSERTION: " #Condition " " __FILE__ ":", __LINE__); \
    ESP.reset();                                                           \
  \
}

struct PVOutputRecord {
  uint32_t reqUnixtime = 0;
  double energyConsumed = 0.0;
  double powerConsumed = 0.0;
  double energyGenerated = 0.0;
  double powerGenerated = 0.0;
  double voltage = 0.0;
};

static String DateTimeToString(DateTime dt);

static void startNextPostInterval(uint32_t* nextPostTime, uint32_t pvoutputReportInterval,
  IotaLogRecord* dayStartLogRecord, IotaLogRecord* logRecord, IotaLogRecord* prevPostLogRecord);

static void readKeyAtOrBefore(IotaLog& iotaLog, IotaLogRecord* record, uint32_t when);

static uint32_t getPreviousDay(uint32_t unixTime);

static uint32_t UTCToLocalTime(uint32_t utc);

static uint32_t LocalToUTCTime(uint32_t local);

static PVOutputRecord CreatePVOutputRecord(
  uint32_t postTime, IotaLogRecord* logRecord, IotaLogRecord* prevPostLogRecord, IotaLogRecord* dayStartLogRecord);

static boolean pvoutputSendData(const PVOutputRecord& pvoutputRecord);

uint32_t pvoutputService(struct serviceBlock* _serviceBlock) {
  // trace T_pvoutput
  enum states { initialize, post, resend };
  static states state = initialize;
  static IotaLogRecord* logRecord = new IotaLogRecord;
  static IotaLogRecord* prevPostLogRecord = new IotaLogRecord;
  static IotaLogRecord* dayStartLogRecord = new IotaLogRecord;
  static File pvoutputPostLog;
  static uint32_t nextPostTime = UNIXtime();
  static PVOutputRecord pvoutputRecord;

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

  if(pvoutputInitialize) {
    state = initialize;
    pvoutputInitialize = false;
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

      uint32_t prevPostTime = 0;
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
        prevPostTime = buf->data;
      }
      else {
        // @todo Copied from influx, if failed to open log file then why do we continue (and not just fail init and try
        // again later?
        trace(T_pvoutput, 7);
        msgLog(F("pvoutput: pvoutputlog file failed to open just using most recent iota log time"));
        prevPostTime = iotaLog.lastKey();
      }

      // Obtain the record used for for prevPostTime
      readKeyAtOrBefore(iotaLog, prevPostLogRecord, prevPostTime);
      msgLog("pvoutput: Using previous iota log record with time: " + String(prevPostLogRecord->UNIXtime)
        + " as previous record for expected prev time: " + String(prevPostTime));

      // Now we will figure out when our next post should be
      // It will be the next quantized interval following prevPostTime
      //
      // Note that prevPostTime may not be on a quantized boundary say if the
      // user changes the interval in the config file. In this case, we will
      // be starting a new interval. The first record posted will be a slightly different
      // time frame than pvoutputReportInterval as it will cover the time from
      // prevPostTime to the new quantized interval sequence so we dont lose any data
      nextPostTime = prevPostTime + pvoutputReportInterval - (prevPostTime % pvoutputReportInterval);

      // For pvoutput we log energy accumulated each day, so we also need to
      // read the last record seen the day before and use that as the reference
      // for accumulated energy this day.
      uint32_t unixPreviousDay = getPreviousDay(nextPostTime);
      readKeyAtOrBefore(iotaLog, dayStartLogRecord, unixPreviousDay);
      msgLog("pvoutput: Using end of previous day iota log record with time: " + String(dayStartLogRecord->UNIXtime)
        + " as previous day record for expected prev day time: " + String(unixPreviousDay));

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

      // Add a log for debugging timing issues
      uint32_t now = UNIXtime();
      DateTime dtNext(UTCToLocalTime(nextPostTime));
      DateTime dtLast(UTCToLocalTime(prevPostLogRecord->UNIXtime));
      DateTime dtLastDay(UTCToLocalTime(dayStartLogRecord->UNIXtime));
      DateTime dtNow(UTCToLocalTime(now));
      msgLog("pvoutput: Next post will occur at time: " + DateTimeToString(dtNext) + "(" + String(nextPostTime) + ")"
        + " comparing to previous day post at: " + DateTimeToString(dtLastDay) + "("
        + String(dayStartLogRecord->UNIXtime) + ") and previous interval post at: " + DateTimeToString(dtLast) + "("
        + String(prevPostLogRecord->UNIXtime) + ") now is: " + DateTimeToString(dtNow) + "(" + String(now) + ")");
      trace(T_pvoutput, 14);
      return nextPostTime;
    }

    case post: {
      // If WiFi is not connected,
      // just return without attempting to log and try again in a few seconds.
      if(WiFi.status() != WL_CONNECTED) {
        trace(T_pvoutput, 15);
        return UNIXtime() + 2;
      }

      // If IoTa log has not got data for the time we want to post for yet, then wait for it
      if(iotaLog.lastKey() < nextPostTime) {
        uint32_t now = UNIXtime();
        if(now < nextPostTime) {
          trace(T_pvoutput, 16);
          return nextPostTime;
        }
        else {
          trace(T_pvoutput, 17);
          msgLog(
            "pvoutput: We want to post, but iotaLog does not yet have enough data for us to be able to post it. last key : "
            + String(iotaLog.lastKey()) + " We want to post at time : " + String(nextPostTime)
            + " time has been reached so just waiting on iota log to have the data.Waiting for another second");
          return now + 1;
        }
      }

      // Read sequentially to find the log record for the time we want to post for now
      //
      // Skip over any IoTaLog records until we find one we want to report to pvoutput
      trace(T_pvoutput, 18);
      *logRecord = *prevPostLogRecord;
      uint32_t prevKey = logRecord->UNIXtime;
      while(logRecord->UNIXtime < nextPostTime) {

        if(logRecord->UNIXtime >= iotaLog.lastKey()) {
          msgLog("pvoutput:runaway seq read.", logRecord->UNIXtime);
          ESP.reset();
        }

        prevKey = logRecord->UNIXtime;
        int rkResult = iotaLog.readNext(logRecord);
        if(rkResult != 0) {
          trace(T_pvoutput, 17);
          msgLog("pvoutput: Failed reading from log. Trying again in a second");
          return UNIXtime() + 1;
        }
      }

      // If we went past the record, then go back one
      // I.e. We report from prevPostLogRecord to logRecord where it is the first record where logRecord->UNIXtime <=
      // nextPostTime
      if(logRecord->UNIXtime > nextPostTime) {
        logRecord->UNIXtime = prevKey;
        int rkResult = iotaLog.readKey(logRecord);
        if(rkResult != 0) {
          trace(T_pvoutput, 17);
          msgLog("pvoutput: Failed reading from log. Trying again in a second");
          return UNIXtime() + 1;
        }
      }

      pvoutputRecord = CreatePVOutputRecord(nextPostTime, logRecord, prevPostLogRecord, dayStartLogRecord);
      if(!pvoutputSendData(pvoutputRecord)) {
        trace(T_pvoutput, 26);
        // Use resend state, the only real difference is the timeout
        state = resend;
        msgLog("pvoutput: Pushing data to PVOutput failed, trying again in 10 sec");
        return UNIXtime() + 10;
      }

      buf->data = nextPostTime;
      pvoutputPostLog.write((byte*)buf, 4);
      pvoutputPostLog.flush();
      startNextPostInterval(&nextPostTime, pvoutputReportInterval, dayStartLogRecord, logRecord, prevPostLogRecord);

      trace(T_pvoutput, 27);
      state = post;
      return nextPostTime;
    }

    case resend: {
      msgLog(F("pvoutput: Resending pvoutput data."));
      trace(T_pvoutput, 28);
      if(!pvoutputSendData(pvoutputRecord)) {
        trace(T_pvoutput, 29);
        msgLog("pvoutput: Pushing data to PVOutput failed, trying again in 60 sec");
        return UNIXtime() + 60;
      }
      else {
        trace(T_pvoutput, 30);
        buf->data = nextPostTime;
        pvoutputPostLog.write((byte*)buf, 4);
        pvoutputPostLog.flush();
        startNextPostInterval(&nextPostTime, pvoutputReportInterval, dayStartLogRecord, logRecord, prevPostLogRecord);

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
boolean pvoutputSendData(const PVOutputRecord& rec) {
  // This sends data to PVOutput using API: https://pvoutput.org/help.html#api-addstatus
  trace(T_pvoutput, 31);

  // PVOutput requires localized time
  uint32_t localUnixTime = UTCToLocalTime(rec.reqUnixtime);
  DateTime dt(localUnixTime);

  String path = String("/service/r2/addstatus.jsp") + "?d=" + String(dt.year()) + String(dt.month()) + String(dt.day())
    + "&t=" + String(dt.hour()) + ":" + String(dt.minute()) + "&v1=" + String(rec.energyGenerated)
    + "&v2=" + String(rec.powerGenerated) + "&v3=" + String(rec.energyConsumed) + "&v4="
    + String(rec.powerConsumed)
    //+ "&v5=" + String(rec.temperature)
    + "&v6=" + String(rec.voltage) + "&c1=0" // If 1 indicates cumuliative not reset of energy each day
    + "&n=0" // If 1 indicates net import/export not gross, currently calc gross in caller
    ;
  msgLog("pvoutput: Posting for time: " + String(rec.reqUnixtime) + " path: " + "pvoutput.org:80" + path
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
    trace(T_pvoutput, 36);
    String code = String(httpCode);
    if(httpCode < 0) {
      code = http.errorToString(httpCode);
    }

    msgLog("pvoutput: POST FAILED code: " + String(httpCode) + " message: " + code + " response: " + response);
    return false;
  }
  else {
    trace(T_pvoutput, 37);
    msgLog("pvoutput: POST success code: " + String(httpCode) + " response: " + response);
  }

  return true;
}

String DateTimeToString(DateTime dt) {
  return String(dt.year()) + "/" + String(dt.month()) + "/" + String(dt.day()) + " " + String(dt.hour()) + ":"
    + String(dt.minute()) + ":" + String(dt.second());
}

void startNextPostInterval(uint32_t* nextPostTime, uint32_t pvoutputReportInterval, IotaLogRecord* dayStartLogRecord,
  IotaLogRecord* logRecord, IotaLogRecord* prevPostLogRecord) {

  // Update the previous record to point to the one just posted (irrespective of if there is a time jump for next
  // because of a gap in the iota log)
  *prevPostLogRecord = *logRecord;

  // Find the next time to post
  *nextPostTime += pvoutputReportInterval;

  // If there are no logs for the next interval but there are logs for much further in the future (I.e. A time jump in
  // iota log) then we need to skip so we dont keep posting for missing data periods. Check the log to see where the
  // next record is.
  //
  // If it is after our nextPostTime, then we need to adjust next post time to the first quantized period before the
  // next record and thus potentially skip a few post intervals
  int rkResult = iotaLog.readNext(logRecord);
  if(rkResult == 0) {
    if(logRecord->UNIXtime > *nextPostTime) {
      uint32_t newPostTime = logRecord->UNIXtime - (logRecord->UNIXtime % pvoutputReportInterval);
      ;
      msgLog("pvoutput: Big jump in log record keys. Expecting next as: " + String(*nextPostTime)
        + " but the next one in the iota log is: " + String(logRecord->UNIXtime)
        + " setting next to: " + String(newPostTime));
      *nextPostTime = newPostTime;
    }
  }

  // If we move to a new day, then update dayStartLogRecord
  DateTime lastDt(UTCToLocalTime(prevPostLogRecord->UNIXtime));
  DateTime nextDt(UTCToLocalTime(*nextPostTime));
  if(lastDt.day() != nextDt.day() || lastDt.month() != nextDt.month() || lastDt.year() != nextDt.year()) {
    trace(T_pvoutput, 38);
    msgLog("pvoutput: Started a new day for log accumulation. Previous day: " + DateTimeToString(lastDt)
      + " new day: " + DateTimeToString(nextDt));
    *dayStartLogRecord = *prevPostLogRecord;
  }
  else {
    msgLog("pvoutput: Same day, keeping prev day record, updating next post to: " + String(*nextPostTime)
      + " and prev is: " + String(prevPostLogRecord->UNIXtime));
  }
}

void readKeyAtOrBefore(IotaLog& iotaLog, IotaLogRecord* record, uint32_t when) {
  record->UNIXtime = when;

  if(record->UNIXtime < iotaLog.firstKey()) {
    record->UNIXtime = iotaLog.firstKey();
  }

  if(record->UNIXtime > iotaLog.lastKey()) {
    record->UNIXtime = iotaLog.lastKey();
  }

  bool firstError = true;
  int rkResult = 1;
  do {
    // @todo This would be more efficient with iotaLog supporting readPrev()
    rkResult = iotaLog.readKey(record);
    if(rkResult != 0) {
      if(firstError) {
        firstError = false;
        trace(T_pvoutput, 9);
        msgLog("pvoutput: WARNING: Failed to read log record " + String(record->UNIXtime)
          + " going backwards until a key matches, next: " + String(record->UNIXtime - 1));
      }
      record->UNIXtime -= 1;
    }
  } while(rkResult != 0 && record->UNIXtime > iotaLog.firstKey());

  // I have no idea why, but in IoTa existing code there is a tendency to always check for NaN
  // and replace it with 0. It would be better to fix the cause of the NaN but
  // for now lets follow the existing pattern as there is probably a good reason for it.
  for(int i = 0; i < maxInputs; i++) {
    // Check for NaN
    if(record->channel[i].accum1 != record->channel[i].accum1) {
      trace(T_pvoutput, 10);
      record->channel[i].accum1 = 0;
    }
  }
  // Check for NaN
  if(record->logHours != record->logHours) {
    trace(T_pvoutput, 11);
    record->logHours = 0;
  }
}

uint32_t getPreviousDay(uint32_t unixTime) {
  uint32_t localUnixTime = UTCToLocalTime(unixTime);
  DateTime localDt(localUnixTime);
  DateTime previousDay(localDt.year(), localDt.month(), localDt.day(), 23, 59, 59);
  previousDay = previousDay - TimeSpan(1, 00, 0, 0);
  uint32_t unixPreviousDay = LocalToUTCTime(previousDay.unixtime());
  return unixPreviousDay;
}

uint32_t UTCToLocalTime(uint32_t utc) { return utc + (localTimeDiff * 3600); }

uint32_t LocalToUTCTime(uint32_t local) { return local - (localTimeDiff * 3600); }

PVOutputRecord CreatePVOutputRecord(
  uint32_t postTime, IotaLogRecord* logRecord, IotaLogRecord* prevPostLogRecord, IotaLogRecord* dayStartLogRecord) {
  PVOutputRecord ret;

  // Adjust the posting time to match the log entry time (at modulo we care about)
  // reqUnixtime = logRecord->UNIXtime - (logRecord->UNIXtime % pvoutputReportInterval);
  ret.reqUnixtime = postTime - (postTime % pvoutputReportInterval);
  msgLog("pvoutput: postTime: " + String(postTime) + ", req: " + String(ret.reqUnixtime));
  assert(ret.reqUnixtime == postTime);
  trace(T_pvoutput, 19);

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

  ret.voltage = 0;
  if(voltageChannel >= 0 && logRecord->logHours != prevPostLogRecord->logHours) {
    trace(T_pvoutput, 20);
    ret.voltage = (logRecord->channel[voltageChannel].accum1 - prevPostLogRecord->channel[voltageChannel].accum1);
    ret.voltage /= (logRecord->logHours - prevPostLogRecord->logHours);
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
  ret.energyGenerated = 0;
  if(pvoutputSolarChannel >= 0) {
    trace(T_pvoutput, 21);
    ret.energyGenerated
      = logRecord->channel[pvoutputSolarChannel].accum1 - dayStartLogRecord->channel[pvoutputSolarChannel].accum1;
    if(ret.energyGenerated > 0) ret.energyGenerated *= -1;
  }

  // Find out how much energy we imported from the main line
  double energyImported = 0.0;
  if(pvoutputMainsChannel >= 0) {
    trace(T_pvoutput, 22);
    energyImported
      = logRecord->channel[pvoutputMainsChannel].accum1 - dayStartLogRecord->channel[pvoutputMainsChannel].accum1;
  }

  // Example:
  // generated = -5kWh
  // imported = 2kWh
  // thus we are consuming 7kWh as we are using all the 5kW and an additional 2kW from mains import
  ret.energyConsumed = energyImported - ret.energyGenerated;

  // the mean power used in W since the last post
  ret.powerGenerated = 0;
  if(pvoutputSolarChannel >= 0 && logRecord->logHours != prevPostLogRecord->logHours) {
    trace(T_pvoutput, 23);
    ret.powerGenerated
      = logRecord->channel[pvoutputSolarChannel].accum1 - prevPostLogRecord->channel[pvoutputSolarChannel].accum1;
    if(ret.powerGenerated > 0) ret.powerGenerated *= -1;
    ret.powerGenerated = ret.powerGenerated / (logRecord->logHours - prevPostLogRecord->logHours);
  }

  // Find out how much energy we imported from the main line
  double powerImported = 0.0;
  if(pvoutputMainsChannel >= 0 && logRecord->logHours != prevPostLogRecord->logHours) {
    trace(T_pvoutput, 24);
    powerImported
      = logRecord->channel[pvoutputMainsChannel].accum1 - prevPostLogRecord->channel[pvoutputMainsChannel].accum1;
    powerImported = powerImported / (logRecord->logHours - prevPostLogRecord->logHours);
  }

  // Example:
  // generated = -5kWh
  // imported = 2kWh
  // thus we are consuming 7kWh as we are using all the 5kW and an additional 2kW from mains import
  ret.powerConsumed = powerImported - ret.powerGenerated;

  // If we are exporting more than we are generating something is wrong
  if(powerImported < ret.powerGenerated) {
    trace(T_pvoutput, 25);
    msgLog("pvoutput: PVOutput configuration is incorrect. Appears we are exporting more power than we are generating.",
      "Imported: " + String(powerImported) + ", Generated: " + String(ret.powerGenerated));
  }

  // PVOutput expects reports as positive values, our internal calculations
  // expect negative values for generation so just convert them now
  ret.energyGenerated *= -1;
  ret.powerGenerated *= -1;

  // Now sanity check the data so we dont get PVOutput infinite POST loop
  // due to known problems
  if(ret.energyGenerated < 0.0) {
    trace(T_pvoutput, 32);
    msgLog("pvoutput: energyGenerated is invalid PVOutput wont accept negative values", String(ret.energyGenerated));
    ret.energyGenerated = 0.0;
  }

  if(ret.powerGenerated < 0.0) {
    trace(T_pvoutput, 33);
    msgLog("pvoutput: powerGenerated is invalid PVOutput wont accept negative values", String(ret.powerGenerated));
    ret.powerGenerated = 0.0;
  }

  if(ret.energyConsumed < 0.0) {
    trace(T_pvoutput, 34);
    msgLog("pvoutput: energyConsumed is invalid PVOutput wont accept negative values", String(ret.energyConsumed));
    ret.energyConsumed = 0.0;
  }

  if(ret.powerConsumed < 0.0) {
    trace(T_pvoutput, 35);
    msgLog("pvoutput: powerConsumed is invalid PVOutput wont accept negative values", String(ret.powerConsumed));
    ret.powerConsumed = 0.0;
  }

  return ret;
}
