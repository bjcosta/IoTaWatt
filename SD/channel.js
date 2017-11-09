// Globals used between refreshes to remember state
var demo_mode = false;
var vchannel = null;
var ichannel = null;
var previousPoint = null;
var refreshIntervalId = null;

function getConfigUrl() {
	if (demo_mode) {
		return "config.txt";
	}
	return "config.txt";
}

function getChannelURL(channel_override=null) {
	if (channel_override === null) {
		channel_override = getCurrentChannel();
	}

	if (demo_mode) {
		return 'command_channel_' + channel_override + '.json';
	}
	return "command?channel=" + channel_override;
}

function getSampleURL(channel_override=null) {
	if (channel_override === null) {
		channel_override = getCurrentChannel();
	}

	if (demo_mode) {
		return "samples.csv";
	}
	return "command?sample=" + channel_override;
}

function readFile(path, responseHandler){
  var xmlHttp = new XMLHttpRequest();
  xmlHttp.onreadystatechange = function() {
    if (this.readyState == 4 && this.status == 200) {
      if(this.getResponseHeader("X-configSHA256") !== null){
        configSHA256 = this.getResponseHeader("X-configSHA256");
      }
      responseHandler(this.responseText);
    }
  };
  xmlHttp.open("GET", path, true);
  xmlHttp.send();
}

function checkConfig(config){
	if(config.device.channels === undefined){
		config.device.channels = 15;
	}

	if(config.inputs === undefined){
		config.inputs = [{channel:0, type:"VT", model:"generic", cal:10, phase:0}];
	}
	for(var i=0; i<config.inputs.length; i++){
		if(config.inputs[i] === undefined || (config.inputs[i] !== null && config.inputs[i].channel > i)){
			config.inputs.splice(i,0,null);
		}
	}
	for(var i=config.inputs.length; i<config.device.channels; i++){
		config.inputs.push(null);
	}
	config.inputs.splice(config.device.channels,config.inputs.length-config.device.channels);

	if(config.device.burden === undefined){
		config.device.burden = [0];
	}
	for(var i=config.device.burden.length; i<config.device.channels; i++){
		config.device.burden.push(24);
	}

	if(config.format == 1){
		for(i in config.outputs) {
			config.outputs[i].script = old2newScript(config.outputs[i].script);
		}
		config.format == 2;
	}
}

function old2newScript(oldScript){
	var newScript = "";
	for(i in oldScript){
		if(oldScript[i].oper == "const"){
			newScript += "#" + oldScript[i].value;
		}
		else if(oldScript[i].oper == "input"){
			newScript += "@" + oldScript[i].value;
		}
		else if(oldScript[i].oper == "binop"){
			newScript += oldScript[i].value;
		}
		else if(oldScript[i].oper == "push"){
			newScript += "(";
		}
		else if(oldScript[i].oper == "pop"){
			newScript += ")";
		}
		else if(oldScript[i].oper == "abs"){
			newScript += "|";
		}
	}
	return newScript;
}

function getCurrentChannel() {
	var chan = $('#channel').find(":selected").val();
	if (chan === null || typeof chan == 'undefined') {
		chan = '1';
	}
	return chan;
}

function queryConfig(callback) {
	readFile(getConfigUrl(), function(response) {
		var config = JSON.parse(response);
		checkConfig(config);
		callback(config);
	});
}

function processConfig(config) {
	var currentChannel = getCurrentChannel();
	$('#channel').remove();
	var options = '';
	for(i in config.inputs) {
		// Only care about CT channels
		if (config.inputs[i] !== null && config.inputs[i].type == 'CT'){
			var o = '<option value="' + config.inputs[i].channel + '"';
			if (config.inputs[i].channel == currentChannel) {
				o += ' selected="selected"';
			}
			o += '>Channel ' + config.inputs[i].channel + ' : ' + config.inputs[i].name + '</option>';
			
			options += o;
		}
	}
	
	$('<select id="channel" onchange="refreshChannel()">' + options + '</select>').appendTo('#channelSelection');
}

function queryChannel(channel, callback) {
	$.get(getChannelURL(channel), function(response) {
		// Accept either a object or a string in case remote server doesn't have correct MIME type
		var ichan = (typeof response == 'string' || response instanceof String) ? eval("(" + response + ')') : response;
		
		$.get(getChannelURL(ichan.vchannel), function(response) {
			var vchan = (typeof response == 'string' || response instanceof String) ? eval("(" + response + ')') : response;
		
			callback(vchan, ichan);
		});
	});
}

function querySamples(callback) {
	$.get(getSampleURL(), function(response) {
		var sampleCsvText = response;
		callback(sampleCsvText);
	});
}

function processSamples(sampleCsvText, vchan, ichan) {
	// Note: Slice out the first line it is a count of the number of samples, not actual data
	var csvArray = $.csv.toArrays(sampleCsvText.split("\n").slice(1).join("\n"));
	var vratio = vchan.calibration * vchan.aRefValue / vchan.adcrange * vchan.vadj3;
	var iratio = ichan.calibration * ichan.aRefValue / ichan.adcrange;

	// Do the same as the C code and figure out how to adjust the phase of the I channel to match the V channel
	var phaseCorrection = (vchan.phase - ichan.phase) * csvArray.length / 360.0;
	var stepCorrection = Math.round(phaseCorrection);
	
	// For now we are not doing fractional phase like C code does using linear interpolation
	//var stepCorrection = Math.floor(phaseCorrection);
	//var stepFraction = phaseCorrection - stepCorrection;
	//if (stepFraction < 0) {
	//	stepCorrection--;
	//	stepFraction += 1.0;
	//}
	
	var impl_voltage = []
	var impl_voltage_fft = []
	var impl_current = []
	var impl_current_fft = []
	var impl_power = []
	var impl_power_fft = []
	for (var i = 0; i < csvArray.length; i++) {
		var v = csvArray[i][0];
		var cIndex = (i + stepCorrection) % csvArray.length;
		if (cIndex < 0) {
			cIndex += csvArray.length;
		}
		var c = csvArray[cIndex][1];
		
		v = vratio * v;
		c = iratio * c;
		var p = v * c;
		
		impl_voltage.push([i, v]);
		impl_current.push([i, c]);
		impl_power.push([i, p]);

		impl_voltage_fft.push(v);
		impl_current_fft.push(c);
		impl_power_fft.push(p);
	}
	
	// FFTLEN must usually be a multiple of 2, but according to docs jsFFT wants multiple of 4
	var FFTLEN = 4;
	for (FFTLEN = 4; FFTLEN < csvArray.length; FFTLEN *= 2) {
	}

	// Zero pad to power of two
	for (var i = csvArray.length; i < FFTLEN; ++i) {
		impl_voltage_fft.push(0);
		impl_current_fft.push(0);
	}
	
	var cplxdata = jsFFT.makeComplex(impl_voltage_fft, FFTLEN);
	var fftdata = jsFFT.FFT(cplxdata, FFTLEN, 1);
	var magdata = jsFFT.Magnitude(fftdata, FFTLEN);
	impl_voltage_fft = [];
	for (var i = 0; i < FFTLEN / 2; i++) {
		impl_voltage_fft.push([i, magdata[i]]);
	}

	cplxdata = jsFFT.makeComplex(impl_current_fft, FFTLEN);
	fftdata = jsFFT.FFT(cplxdata, FFTLEN, 1);
	magdata = jsFFT.Magnitude(fftdata, FFTLEN);
	impl_current_fft = [];
	for (var i = 0; i < FFTLEN / 2; i++) {
		impl_current_fft.push([i, magdata[i]]);
	}
	
	cplxdata = jsFFT.makeComplex(impl_power_fft, FFTLEN);
	fftdata = jsFFT.FFT(cplxdata, FFTLEN, 1);
	magdata = jsFFT.Magnitude(fftdata, FFTLEN);
	impl_power_fft = [];
	for (var i = 0; i < FFTLEN / 2; i++) {
		impl_power_fft.push([i, magdata[i]]);
	}

	return {
		voltage: impl_voltage,
		voltage_fft: impl_voltage_fft,
		current: impl_current,
		current_fft: impl_current_fft,
		power: impl_power,
		power_fft: impl_power_fft
	};
}

function showTooltip(x, y, contents) {
	$('<div id="tooltip">' + contents + '</div>').css( {
		position: 'absolute',
		display: 'none',
		top: y + 5,
		left: x + 5,
		border: '1px solid #fdd',
		padding: '2px',
		'background-color': '#fee',
		opacity: 0.80
	}).appendTo("body").fadeIn(200);
}

function plotHover (event, pos, item) {
	if (item) {
		var z = item.dataIndex;
		if (previousPoint != item.datapoint) {
			previousPoint = item.datapoint;

			$("#tooltip").remove();
			var item_time = item.datapoint[0];
			var item_value = item.datapoint[1];
			showTooltip(item.pageX, item.pageY, "<span style='font-size:11px'>"+item.series.label+"</span><br>"+item_value, "#fff");
		}
	} else $("#tooltip").remove();
}

function drawGraphs(graphData) {
	$.plot("#voltage_placeholder", [{
			data: graphData.voltage,
			label: 'Voltage'
		}], {
			grid: {hoverable: true}
		});
	$('#voltage_placeholder').bind("plothover", plotHover);

	$.plot("#current_placeholder", [{
			data: graphData.current,
			label: 'Current'
		}], {
			grid: {hoverable: true}
		});
	$('#current_placeholder').bind("plothover", plotHover);

	$.plot("#power_placeholder", [{
			data: graphData.power,
			label: 'Power'
		}], {
			grid: {hoverable: true}
		});
	$('#power_placeholder').bind("plothover", plotHover);

	$.plot("#voltage_fft_placeholder", [{
			data: graphData.voltage_fft,
			label: 'Voltage FFT',
			bars: {show: true}
		}], {
			grid: {hoverable: true}
		});
	$('#voltage_fft_placeholder').bind("plothover", plotHover);

	$.plot("#current_fft_placeholder", [{
			data: graphData.current_fft,
			label: 'Current FFT',
			bars: {show: true}
		}], {
			grid: {hoverable: true}
		});
	$('#current_fft_placeholder').bind("plothover", plotHover);

	$.plot("#power_fft_placeholder", [{
			data: graphData.power_fft,
			label: 'Power FFT',
			bars: {show: true}
		}], {
			grid: {hoverable: true}
		});
	$('#power_fft_placeholder').bind("plothover", plotHover);
}

function refreshSamples(callback=null) {
	querySamples(function (sampleCsvText) {

	// Use the vchannel, ichannel stored from previous call
		var graphData = processSamples(sampleCsvText, vchannel, ichannel);
		drawGraphs(graphData);
		if (callback != null)
			callback();
	});
}

function refreshChannel(callback=null) {
	queryChannel(getCurrentChannel(), function(vchan, ichan) {
		
		// Assign the globals used in refreshSamples
		vchannel = vchan;
		ichannel = ichan;
		
		refreshSamples(callback);
	});
}

function refreshConfig(callback=null) {
	queryConfig(function (config){
		// No need to store config as a global, the relevant details are stored in the HTML channel selector
		processConfig(config);
		refreshChannel(callback);
	});
}

function autoRefreshSamples(cb) {
	if (cb.checked) {
		refreshIntervalId = setInterval(refreshSamples, 2000);
	}
	else {
		if (refreshIntervalId != null) {
			clearInterval(refreshIntervalId);
			refreshIntervalId = null;
		}
	}
}

function initializePage() {
	var graphData = {
		voltage: [],
		voltage_fft: [],
		current: [],
		current_fft: [],
		power: [],
		power_fft: []
	};

	// Draw initial graphs with 0's assigned already in graphData
	drawGraphs(graphData);
	refreshConfig();
}

