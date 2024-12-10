$(function () {
  // api_url = "http://127.0.0.1:5000/";
  api_url = "https://bizrecapi.onrender.com";
  predict_api = api_url + "/predict";
  location_api = api_url + "/location";
  cuizine_api = api_url + "/cuizine";
  settings_url = api_url + "/settings";

  $("#result").hide();

  try {
    //call location api
    $.ajax({
      url: settings_url,
      type: "GET",
      success: function (response) {
        // Process the response data
        console.log(response);
        loc_data = response.location;
        cui_data = response.cuizine;
        var location = document.getElementById("location");
        var cuzine = document.getElementById("cuzine");

        //remove all options
        var opt = document.createElement("option");
        opt.innerHTML = "Select Location";
        opt.value = "";
        location.innerHTML = "";
        location.appendChild(opt);
        for (var i = 0; i < loc_data.length; i++) {
          var opt = document.createElement("option");
          opt.value = loc_data[i];
          opt.innerHTML = loc_data[i];
          location.appendChild(opt);
        }

        var opt = document.createElement("option");
        opt.innerHTML = "Select Cuizine";
        cuzine.innerHTML = "";
        opt.value = "";
        cuzine.appendChild(opt);
        for (var i = 0; i < cui_data.length; i++) {
          var opt = document.createElement("option");
          opt.value = cui_data[i];
          opt.innerHTML = cui_data[i];
          cuzine.appendChild(opt);
        }
      },
      error: function (error) {
        console.error("Error fetching data:", error);
        Swal.fire({
          text: "Error fetching data",
          icon: "error",
        });
      },
    });

    // $.ajax({
    //   url: cuizine_api,
    //   type: "GET",
    //   success: function (response) {
    //     // Process the response data
    //     console.log(response);
    //     data = response;
    //     for (var i = 0; i < data.length; i++) {
    //       var opt = document.createElement("option");
    //       opt.value = data[i];
    //       opt.innerHTML = data[i];
    //       location.appendChild(opt);
    //     }
    //   },
    //   error: function (error) {
    //     console.error("Error fetching data:", error);
    //     $("#result").html("Error fetching data");
    //   },
    // });
  } catch (e) {
    console.log(e);
    Swal.fire({
      text: "Error loading the system",
      icon: "error",
    });
  }

  submit.addEventListener("click", function () {
    //get value of dropdowm control
    var rating = document.getElementById("rating");
    var cuzine = document.getElementById("cuzine");
    var city = document.getElementById("location");
    var wifi = document.getElementById("wifi").checked;
    var parking = document.getElementById("parking").checked;

    console.log(wifi);
    console.log(parking);

    var rest_rating = rating.value;
    var rest_cuzine = cuzine.value;
    var location = city.value;
    var need_wifi = wifi == true ? "Yes" : "No";
    var need_parking = parking == true ? "Yes" : "No";

    var formErr = false;

    //check if value is empty
    if (location == "" || rest_cuzine == "" || rest_rating == "") {
      formErr = true;
    }

    if (formErr) {
      Swal.fire({
        text: "Please select value for required field(s)",
        icon: "info",
      });
      return false;
    }

    $(".home").hide();
    $("#result").show();

    const data = {
      user_location: location,
      min_rating: rest_rating,
      need_parking: need_parking,
      need_wifi: need_wifi,
      cuisine_type: rest_cuzine,
    };
    console.log(data);

    $.ajax({
      url: predict_api,
      type: "POST",
      contentType: "application/json", // Adjust as needed
      data: JSON.stringify({ data }),
      success: function (response) {
        // Process the response data
        console.log(response);
      },
      error: function (error) {
        console.error("Error fetching data:", error);
        Swal.fire({
          text: "Error fetching data",
          icon: "error",
        });
      },
    });
  });

  reset.addEventListener("click", function () {
    //get value of dropdowm control
    $("#result").hide();
    $(".home").show();
  });
});
