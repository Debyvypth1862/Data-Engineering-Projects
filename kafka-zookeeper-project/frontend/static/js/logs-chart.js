(function ($) {
  "use strict";

  var ctx = document.getElementById("logsChart");
  var myChart;
  var logCounts = { INFO: 0, WARN: 0, ERROR: 0 }; // Track log counts

  if (ctx) {
    ctx.height = 150;
    myChart = new Chart(ctx, {
      type: "doughnut",
      data: {
        datasets: [{
          data: [0, 0, 0], // Start with 0 logs
          backgroundColor: [
            "rgba(0, 123, 255, 0.9)",  // Blue for INFO
            "rgba(255, 193, 7, 0.9)",  // Yellow for WARN
            "rgba(220, 53, 69, 0.9)"   // Red for ERROR
          ],
          hoverBackgroundColor: [
            "rgba(0, 123, 255, 0.7)",
            "rgba(255, 193, 7, 0.7)",
            "rgba(220, 53, 69, 0.7)"
          ]
        }],
        labels: ["INFO", "WARN", "ERROR"] // Labels match log levels
      },
      options: {
        legend: {
          position: "top",
          labels: { fontFamily: "Poppins" }
        },
        responsive: true
      }
    });
  }

  // Establish WebSocket connection
  var socket = io("http://localhost:5002", { transports: ["websocket"] });

  socket.on("connect", function () {
    console.log("✅ WebSocket Connected!");
  });

  socket.on("connect_error", function (error) {
    console.error("🚨 WebSocket Connection Error:", error);
  });

  socket.on("disconnect", function (reason) {
    console.log("❌ WebSocket Disconnected! Reason:", reason);
  });

  // Handle incoming log event
  socket.on("log_event", function (data) {
    console.log("📥 Received WebSocket log:", data);

    // Update log count based on log level
    if (logCounts.hasOwnProperty(data.level)) {
      logCounts[data.level] += 1;
    }

    // Update the chart data
    myChart.data.datasets[0].data = [
      logCounts["INFO"],
      logCounts["WARN"],
      logCounts["ERROR"]
    ];

    myChart.update();
  });

})(jQuery);
