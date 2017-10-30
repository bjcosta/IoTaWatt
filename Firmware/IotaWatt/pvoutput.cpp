#include "IotaWatt.h"

boolean pvoutputSendData(uint32_t unixTime, double voltage, double energyConsumed, double powerConsumed,
  double energyGenerated, double powerGenerated);

String DateTimeToString(DateTime dt) {
  return String(dt.year()) + "/" + String(dt.month()) + "/" + String(dt.day()) + ":" + String(dt.hour()) + ":"
    + String(dt.minute()) + ":" + String(dt.second());
}

void SetNextPOSTTime(uint32_t* UnixLastPost, uint32_t* UnixNextPost, uint32_t pvoutputReportInterval,
  IotaLogRecord* dayStartLogRecord, IotaLogRecord* logRecord, IotaLogRecord* prevPostLogRecord) {
  *UnixLastPost += pvoutputReportInterval;
  *UnixNextPost += pvoutputReportInterval;
  *prevPostLogRecord = *logRecord;

  // If we move to a new day, then update dayStartLogRecord
  DateTime lastDt(*UnixLastPost + (localTimeDiff * 3600));
  DateTime nextDt(*UnixNextPost + (localTimeDiff * 3600));
  if(lastDt.day() != nextDt.day()) {
    msgLog("pvoutput:Started a new day. Previous day: " + DateTimeToString(lastDt)
      + " new day: " + DateTimeToString(nextDt));
    *dayStartLogRecord = *logRecord;
  }
  else {
    msgLog("pvoutput:Still in same day. Previous POST: " + DateTimeToString(lastDt)
      + " next POST: " + DateTimeToString(nextDt));
  }
}

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
    trace(T_pvoutput, 4);
    pvoutputPostLog.close();
    trace(T_pvoutput, 5);
    SD.remove((char*)pvoutputPostLogFile.c_str());
    trace(T_pvoutput, 6);
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
        msgLog(F("pvoutput: IoTaLog service is not yet running, will delay PVOutput service until it is running. Try "
                 "again in 5 sec"));
        return UNIXtime() + 5;
      }

      // If logfile for this service not open then open one
      if(!pvoutputPostLog) { pvoutputPostLog = SD.open(pvoutputPostLogFile, FILE_WRITE); }

      if(pvoutputPostLog) {
        // If is a new empty log file, then create a record for the "last key"
        if(pvoutputPostLog.size() == 0) {
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
        // @todo Copied from influx, if failed to open log file then why do we continue and not just fail init and try
        // again later?
        msgLog(F("pvoutput: pvoutputlog file failed to open just using most recent iota log time"));
        UnixLastPost = iotaLog.lastKey();
      }
      UnixNextPost = UnixLastPost + pvoutputReportInterval - (UnixLastPost % pvoutputReportInterval);

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
      if(logRecord->UNIXtime < iotaLog.firstKey()) { logRecord->UNIXtime = iotaLog.firstKey(); }
      if(logRecord->UNIXtime > iotaLog.lastKey()) { logRecord->UNIXtime = iotaLog.lastKey(); }
      int rkResult = 1;
      do {
        rkResult = iotaLog.readKey(logRecord);
        if(rkResult != 0) {
          msgLog("pvoutput: WARNING: Failed to read log record " + String(logRecord->UNIXtime)
            + " going backwards until find a key that matches, next: " + String(logRecord->UNIXtime - 1));
          logRecord->UNIXtime -= 1;
        }
      } while(rkResult != 0 && logRecord->UNIXtime > iotaLog.firstKey());

      // make sure the dayStartLogRecord details are valid
      *dayStartLogRecord = *logRecord;
      for(int i = 0; i < maxInputs; i++) {
        // Check for NaN
        if(dayStartLogRecord->channel[i].accum1 != dayStartLogRecord->channel[i].accum1)
          dayStartLogRecord->channel[i].accum1 = 0;
      }
      // Check for NaN
      if(dayStartLogRecord->logHours != dayStartLogRecord->logHours) dayStartLogRecord->logHours = 0;

      // Obtain the previous post log record
      prevPostLogRecord->UNIXtime = UnixLastPost;
      rkResult = iotaLog.readKey(prevPostLogRecord);
      if(rkResult != 0) {
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

      return UnixNextPost;
    }

    case post: {
      // If WiFi is not connected,
      // just return without attempting to log and try again in a few seconds.
      if(WiFi.status() != WL_CONNECTED) { return 2; }

      // If we are current,
      // Anticipate next posting at next regular interval and break to reschedule.
      if(iotaLog.lastKey() < UnixNextPost) {
        uint32_t now = UNIXtime();
        if(now < UnixNextPost) { return UnixNextPost; }
        else {
          msgLog("pvoutput: We want to post, but iotaLog does not yet have enough data for us to be able to post it. "
                 "last key : "
            + String(iotaLog.lastKey()) + " We want to post at time : " + String(UnixNextPost)
            + " time has been reached so just waiting on iota log to have the data.Waiting for another second");
          return now + 1;
        }
      }

      // Not current.  Read sequentially to get the entry >= scheduled post time

      // Skip over any IoTaLog records < UnixNextPost until we find one we can use for diff against for the pvoutput
      // interval Note: Basically asserts that the logs exist, which should have been checked above.
      trace(T_pvoutput, 1);
      logRecord->UNIXtime = UnixLastPost;
      while(logRecord->UNIXtime < UnixNextPost) {
        if(logRecord->UNIXtime >= iotaLog.lastKey()) {
          msgLog("pvoutput:runaway seq read.", logRecord->UNIXtime);
          ESP.reset();
        }
        iotaLog.readNext(logRecord);
      }

      // Adjust the posting time to match the log entry time (at modulo we care about)
      reqUnixtime = logRecord->UNIXtime - (logRecord->UNIXtime % pvoutputReportInterval);
      trace(T_pvoutput, 2);

      // The measurements should be such that:
      // chan 1 : mains +ve indicates net import -ve indicates net export
      // chan 2 : solar -ve indicates generation +ve should never really happen would indicate solar panels using power

      // Find the mean voltage since last post
      int voltageChannel = -1;
      if(pvoutputMainsChannel >= 0) { voltageChannel = inputChannel[pvoutputMainsChannel]->_vchannel; }
      else if(pvoutputSolarChannel >= 0) {
        voltageChannel = inputChannel[pvoutputSolarChannel]->_vchannel;
      }

      voltage = 0;
      if(voltageChannel >= 0 && logRecord->logHours != prevPostLogRecord->logHours) {
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
        energyGenerated
          = logRecord->channel[pvoutputSolarChannel].accum1 - dayStartLogRecord->channel[pvoutputSolarChannel].accum1;
        if(energyGenerated > 0) energyGenerated *= -1;
      }

      // Find out how much energy we imported from the main line
      double energyImported = 0.0;
      if(pvoutputMainsChannel >= 0) {
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
        powerGenerated
          = logRecord->channel[pvoutputSolarChannel].accum1 - prevPostLogRecord->channel[pvoutputSolarChannel].accum1;
        if(powerGenerated > 0) powerGenerated *= -1;
        powerGenerated = powerGenerated / (logRecord->logHours - prevPostLogRecord->logHours);
      }

      // Find out how much energy we imported from the main line
      double powerImported = 0.0;
      if(pvoutputMainsChannel >= 0 && logRecord->logHours != prevPostLogRecord->logHours) {
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
        msgLog(
          "pvoutput: PVOutput configuration is incorrect. Appears we are exporting more power than we are generating.",
          "Imported: " + String(powerImported) + ", Generated: " + String(powerGenerated));
      }

      if(!pvoutputSendData(reqUnixtime, voltage, energyConsumed, powerConsumed, energyGenerated, powerGenerated)) {
        // Use resend state, the only real difference is the timeout
        state = resend;
        msgLog("pvoutput: Pushing data to PVOutput failed, trying again in 10 sec");
        return UNIXtime() + 10;
      }

      buf->data = UnixLastPost;
      pvoutputPostLog.write((byte*)buf, 4);
      pvoutputPostLog.flush();
      SetNextPOSTTime(
        &UnixLastPost, &UnixNextPost, pvoutputReportInterval, dayStartLogRecord, logRecord, prevPostLogRecord);

      trace(T_pvoutput, 3);
      state = post;
      return UnixNextPost;
    }

    case resend: {
      msgLog(F("pvoutput: Resending pvoutput data."));
      if(!pvoutputSendData(reqUnixtime, voltage, energyConsumed, powerConsumed, energyGenerated, powerGenerated)) {
        msgLog("pvoutput: Pushing data to PVOutput failed, trying again in 60 sec");
        return UNIXtime() + 60;
      }
      else {
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
boolean pvoutputSendData(uint32_t unixTime, double voltage, double energyConsumed, double powerConsumed,
  double energyGenerated, double powerGenerated) {
  // This sends data to PVOutput using API: https://pvoutput.org/help.html#api-addstatus
  trace(T_pvoutput, 7);

  // PVOutput expects reports as positive values, our internal calculations
  // expect negative values for generation so just convert them now
  energyGenerated *= -1;
  powerGenerated *= -1;

  // PVOutput requires localized time
  uint32_t localUnixTime = unixTime + (localTimeDiff * 3600);
  DateTime dt(localUnixTime);

  // Now sanity check the data so we dont get PVOutput infinite POST loop
  // due to known problems
  if(energyGenerated < 0.0) {
    msgLog("pvoutput: energyGenerated is invalid PVOutput wont accept negative values", String(energyGenerated));
    energyGenerated = 0.0;
  }

  if(powerGenerated < 0.0) {
    msgLog("pvoutput: powerGenerated is invalid PVOutput wont accept negative values", String(powerGenerated));
    powerGenerated = 0.0;
  }

  if(energyConsumed < 0.0) {
    msgLog("pvoutput: energyConsumed is invalid PVOutput wont accept negative values", String(energyConsumed));
    energyConsumed = 0.0;
  }

  if(powerConsumed < 0.0) {
    msgLog("pvoutput: powerConsumed is invalid PVOutput wont accept negative values", String(powerConsumed));
    powerConsumed = 0.0;
  }

  String path = String("/service/r2/addstatus.jsp") + "?d=" + String(dt.year()) + String(dt.month()) + String(dt.day())
    + "&t=" + String(dt.hour()) + ":" + String(dt.minute()) + "&v1=" + String(energyGenerated)
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
    String code = String(httpCode);
    if(httpCode < 0) { code = http.errorToString(httpCode); }

    msgLog("pvoutput: POST FAILED code: " + String(httpCode) + " message: " + code + " response: " + response);
    return false;
  }
  else {
    msgLog("pvoutput: POST success code: " + String(httpCode) + " response: " + response);
  }

  return true;
}
