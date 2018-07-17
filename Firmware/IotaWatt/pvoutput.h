#pragma once
#include "IotaWatt.h"

// Minimal interface required of an output service
void PVOutputUpdateConfig(const char* jsonText);
void PVOutputGetStatusJson(JsonObject& pvoutput);

