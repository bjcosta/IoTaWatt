#pragma once

#include "IotaWatt.h"
#include "xbuf.h"

uint32_t pvoutputService(struct serviceBlock* _serviceBlock);
bool pvoutputConfig(const char*);

extern bool         pvoutputStarted;                    // set true when Service started
extern bool         pvoutputStop;                       // set true to stop the Service
extern bool         pvoutputRestart;                    // Initialize or reinitialize
extern const char*  pvoutputApiKey;
extern int          pvoutputSystemId;
extern int          pvoutputMainsChannel;
extern int          pvoutputSolarChannel;
extern unsigned int pvoutputHTTPTimeout;
extern uint32_t     pvoutputReportInterval; // Interval (sec) to invoke pvoutput

//extern ScriptSet*   pvoutputOutputs; // @todo not used yet
