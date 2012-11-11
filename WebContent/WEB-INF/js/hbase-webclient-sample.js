$(function() {
	$( "#operation").accordion();
	$( "#summary").accordion();
	$( "#result").accordion();
});

var handleSuccess = function(data, dataType) {
	$("#result_internal").empty().append("SUCCESS : " + data);
};

var handleError = function(XMLHttpRequest, textStatus, errorThrown){
	$("#result_internal").empty().append("ERROR : " + errorThrown);
};

var handleComplete = function(XMLHttpRequest, textStatus) {
	$("#status").val(XMLHttpRequest.status);
	$("#message").val(XMLHttpRequest.statusText);
};


function hbase() {
	var url = "http://localhost:8080/hbase-webclient-sample/HBaseClient";

	var param = "";
	var delim = "?";
	$(".param").each(function () {
		if ($(this).attr("value") != "") {
			param = param + delim + $(this).attr("id") + "=" + $(this).attr("value");
			delim = "&";
		}
	});
	$("#query").val("/HBaseClient" + param);

	// Ajax通信
	jQuery.support.cors = true;
	$.ajax({
		type: "GET",
		dataType: "json",
		url: url + param,
		cache: false,
		crossDomain: true,
		success: handleSuccess,
		error: handleError,
		complete: handleComplete
	});
	$("#status").val("RUNNING");
}
