var chart;

/**
 * Request data from the server, add it to the graph and set a timeout
 * to request again
 */
function requestData() {
    $.ajax({
        url: '/live-data',
        success: function(point) {
            var series = chart2.series[0],
                shift = series.data.length > 20; // shift if the series is
                                                 // longer than 20

            // add the point
            chart2.series[0].addPoint(point, true, shift);

            // call it again after one second
            setTimeout(requestData, 1000);
        },
        cache: false
    });
}


function requestDataIteration(i) {
    $.ajax({
        url: '/live-iteration',
        data:{"iteration":i},
        success: function(point) {
            var series = chart1.series[0],
                shift = series.data.length > 20; // shift if the series is
                                                 // longer than 20

            // add the point
            chart1.series[0].addPoint(point, true, shift);

            // call it again after one second
            setTimeout(function(){
                requestDataIteration(i+1)
            }, 1000);
        },
        cache: false
    });
}

$(document).ready(function() {
    chart1 = new Highcharts.Chart({
        chart: {
            renderTo: 'data-container',
            defaultSeriesType: 'spline',
            events: {
                load: requestDataIteration(1)//requestData
            }
        },
        title: {
            text: 'Twitter Frequency data'
        },
        xAxis: {
            type: 'number',
            tickPixelInterval: 150,
            maxZoom: 20 * 1000
        },
        yAxis: {
            minPadding: 0.2,
            maxPadding: 0.2,
            title: {
                text: 'Value',
                margin: 80
            }
        },
        series: [{
            name: 'Tweets Frequency',
            data: []
        }]
    });
    chart2 = new Highcharts.Chart({
        chart: {
            renderTo: 'data-container2',
            defaultSeriesType: 'areaspline',
            events: {
                load: requestData()//requestData
            }
        },
        title: {
            text: 'Twitter Frequency data'
        },
        xAxis: {
            type: 'number',
            tickPixelInterval: 150,
            maxZoom: 20 * 1000
        },
        yAxis: {
            minPadding: 0.2,
            maxPadding: 0.2,
            title: {
                text: 'Value',
                margin: 80
            }
        },
        series: [{
            name: 'Tweets Frequency',
            data: []
        }]
    });
});