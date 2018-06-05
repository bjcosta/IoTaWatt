#include "IotaWatt.h"

/***************************************************************************************************
 *  GetFeedData SERVICE.
 *  
 *  The web server does a pretty good job of handling file downloads and uploads asynchronously, 
 *  but assumes that any callbacks to new handlers get the job completely done befor returning. 
 *  The GET /feed/data/ request takes a long time and generates a lot of data so it needs to 
 *  run as a SERVICE so that sampling can continue while it works on providing the data.  
 *  To accomplish that without modifying ESP8266WebServer, we schedule this SERVICE and
 *  block subsequent calls to server.handleClient() until the request is satisfied, at which time
 *  this SERVICE returns with code 0 to cause it's serviceBlock to be deleted.  When a new /feed/data
 *  request comes in, the web server handler will reshedule this SERVICE with NewService.
 * 
 **************************************************************************************************/

uint32_t getFeedData(struct serviceBlock* _serviceBlock){
  // trace T_GFD
  enum   states {Initialize, Setup, process};
 
  static states state = Initialize;
  static IotaLogRecord* logRecord;
  static IotaLogRecord* lastRecord;
  static char*    bufr;
  static double   elapsedHours;
  static uint32_t bufrSize = 0;
  static uint32_t bufrPos = 0;
  static uint32_t startUnixTime;
  static uint32_t endUnixTime;
  static uint32_t intervalSeconds;
  static bool     modeRequest;
  static uint32_t UnixTime;
  static String   replyData = "";
  
  struct req {
    req* next;
    int channel;
    char queryType;
    Script* output;
    req(){next=nullptr; channel=0; queryType=' '; output=nullptr;};
    ~req(){delete next;};
  } static reqRoot;
     
  switch (state) {
    case Initialize: {
      state = Setup;
      return 1;
    }
  
    case Setup: {
      trace(T_GFD,0);

        // Validate the request parameters
      
      startUnixTime = server.arg("start").substring(0,10).toInt();
      endUnixTime = server.arg("end").substring(0,10).toInt();
      if(server.hasArg("interval")){
        intervalSeconds = server.arg("interval").toInt();
        modeRequest = false;;
      }
      else if(server.hasArg("mode")){
        modeRequest = true;
        if(server.arg("mode")== "daily") intervalSeconds = 86400;
        else if(server.arg("mode") == "weekly") intervalSeconds = 86400 * 7;
        else if(server.arg("mode") == "monthly") intervalSeconds = 86400 * 30;
        else if(server.arg("mode") == "yearly") intervalSeconds = 86400 * 365;
      }
      if((startUnixTime % 5) ||
         (endUnixTime % 5) ||
         (intervalSeconds % 5) ||
         (intervalSeconds <= 0) ||
         (endUnixTime < startUnixTime) ||
         ((endUnixTime - startUnixTime) / intervalSeconds > 2000)) {
        server.send(400, "text/plain", "Invalid request");
        state = Setup;
        serverAvailable = true;
        return 0;    
      }
      
          // Parse the ID parm into a list.
      
      String idParm = server.arg("id");
      reqRoot.next = nullptr;
      req* reqPtr = &reqRoot;
      int i = 0;
      if(idParm.startsWith("[")){
        idParm[idParm.length()-1] = ',';
        i = 1;
      } else {
        idParm += ",";
      }
      while(i < idParm.length()){
        reqPtr->next = new req;
        reqPtr = reqPtr->next;
        String id = idParm.substring(i,idParm.indexOf(',',i));
        String name = id.substring(2);
        i = idParm.indexOf(',',i) + 1;
        if(id.charAt(0) == 'I'){
          for(int j=0; j<maxInputs; j++){
            if(inputChannel[j]->isActive() &&
               name.equals(inputChannel[j]->_name)){
               reqPtr->channel = inputChannel[j]->_channel;
               reqPtr->output = nullptr;
               reqPtr->queryType = id.charAt(1);
               break;
            }
          }
        }
        else if(id.charAt(0) == 'O'){
          Script* script = outputs->first();
          while(script){
            if(name.equals(script->name())){
              reqPtr->channel = -1;
              reqPtr->output = script;
              reqPtr->queryType = id.charAt(1);
              break;
            } 
            script = script->next(); 
          }  
        }
      }
          
      logRecord = new IotaLogRecord;
      lastRecord = new IotaLogRecord;
     
      if(startUnixTime >= histLog.firstKey()){   
        lastRecord->UNIXtime = startUnixTime - intervalSeconds;
      } else {
        lastRecord->UNIXtime = histLog.firstKey();
      }
      logReadKey(lastRecord);
      
          // Using String for a large buffer abuses the heap
          // and takes up a lot of time. We will build 
          // relatively short response elements with String
          // and copy them to this larger buffer.

      bufrSize = ESP.getFreeHeap() / 2;
      if(bufrSize > 4096) bufrSize = 4096;
      bufr = new char [bufrSize];

          // Setup buffer to do it "chunky-style"
      
      bufr[3] = '\r';
      bufr[4] = '\n'; 
      bufrPos = 5;
      server.setContentLength(CONTENT_LENGTH_UNKNOWN);
      server.send(200,"application/json","");
      replyData = "[";
      UnixTime = startUnixTime;
      state = process;
      _serviceBlock->priority = priorityLow;
      return 1;
    }
  
    case process: {
      trace(T_GFD,1);
      uint32_t exitTime = nextCrossMs + 5000 / frequency;              // Take an additional half cycle
      SPI.beginTransaction(SPISettings(SPI_FULL_SPEED, MSBFIRST, SPI_MODE0));

          // Loop to generate entries
      
      while(UnixTime <= endUnixTime) {
        int rtc;
        logRecord->UNIXtime = UnixTime;
        logReadKey(logRecord);
        trace(T_GFD,2);
        replyData += '[';  //  + String(UnixTime) + "000,";
        elapsedHours = logRecord->logHours - lastRecord->logHours;
        req* reqPtr = &reqRoot;
        while((reqPtr = reqPtr->next) != nullptr){
          int channel = reqPtr->channel;
          if(rtc || logRecord->logHours == lastRecord->logHours){
            replyData +=  "null";
          }
  
            // input channel

          else if(channel >= 0){
            trace(T_GFD,3);       
            if(reqPtr->queryType == 'V') {
              replyData += String((logRecord->accum1[channel] - lastRecord->accum1[channel]) / elapsedHours,1);
            } 
            else if(reqPtr->queryType == 'P') {
              replyData += String((logRecord->accum1[channel] - lastRecord->accum1[channel]) / elapsedHours,1);
            }
            else if(reqPtr->queryType == 'E') {
                replyData += String((logRecord->accum1[channel] / 1000.0),3);              
            } 
            else {
              replyData += "null";
            } 
          }
  
           // output channel
          
          else {
            trace(T_GFD,4);
            if(reqPtr->output == nullptr){
              replyData += "null";
            }
            else if(reqPtr->queryType == 'V'){
              replyData += String(reqPtr->output->run(lastRecord, logRecord, elapsedHours), 1);
            }
            else if(reqPtr->queryType == 'P'){
              replyData += String(reqPtr->output->run(lastRecord, logRecord, elapsedHours), 1);
            }
            else if(reqPtr->queryType == 'E'){
                replyData += String(reqPtr->output->run((IotaLogRecord*) nullptr, logRecord, 1000.0), 3);
            }
            else if(reqPtr->queryType == 'O'){
              replyData += String(reqPtr->output->run(lastRecord, logRecord, elapsedHours), reqPtr->output->precision());
            }
            else {
              replyData += "null";
            }
          }
          if(replyData.endsWith("NaN")){
            replyData.remove(replyData.length()-3);
            replyData += "null";
          }
          replyData += ',';
        } 
           
        replyData.setCharAt(replyData.length()-1,']');
        IotaLogRecord* swapRecord = lastRecord;
        lastRecord = logRecord;
        logRecord = swapRecord;
        UnixTime += intervalSeconds;

            // When buffer is full, send a chunk.
        
        trace(T_GFD,5);
        if((bufrSize - bufrPos - 5) < replyData.length()){
          trace(T_GFD,6);
          sendChunk(bufr, bufrPos);
          bufrPos = 5;
        }

            // Copy this element into the buffer
        
        for(int i = 0; i < replyData.length(); i++) {
          bufr[bufrPos++] = replyData[i];  
        }
        replyData = ',';
      }
      trace(T_GFD,7);

          // All entries generated, terminate Json and send.
      
      replyData.setCharAt(replyData.length()-1,']');
      for(int i = 0; i < replyData.length(); i++) {
        bufr[bufrPos++] = replyData[i];  
      }
      sendChunk(bufr, bufrPos); 

          // Send terminating zero chunk, clean up and exit.
      
      sendChunk(bufr, 5); 
      trace(T_GFD,7);
      replyData = "";
      delete reqRoot.next;
      delete[] bufr;
      delete logRecord;
      delete lastRecord;
      state = Setup;
      serverAvailable = true;
      HTTPrequestFree++;
      return 0;                                       // Done for now, return without scheduling.
    }
  }
}

void sendChunk(char* bufr, uint32_t bufrPos){
  trace(T_GFD,9);
  const char* hexDigit = "0123456789ABCDEF";
  int _len = bufrPos - 5;
  bufr[0] = hexDigit[_len/256];
  bufr[1] = hexDigit[(_len/16) % 16];
  bufr[2] = hexDigit[_len % 16]; 
  bufr[bufrPos] = '\r';
  bufr[bufrPos+1] = '\n';
  bufr[bufrPos+2] = 0;
  //server.sendContent(bufr);                         pre 02_02_22 send command
  //   New ESP8266/Arduino core does chunked under the hood.
  //   Would need to strip out chunk header and footer and then sendContent would
  //   convert bufr to a String and bracket with chunk header and footer writes.
  //   Since already have the chunked headers and footers in the bufr, just write it
  //   to the WiFiClient and avoid conversion to String and related heap requirements.
  server.client().write(bufr, bufrPos+2);
} 