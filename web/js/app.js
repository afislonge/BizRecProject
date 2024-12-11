$(function () {
  api_url = "http://127.0.0.1:5000/";
  //api_url = "https://bizrecapi.onrender.com";
  predict_api = api_url + "/recommend";
  location_api = api_url + "/location";
  cuizine_api = api_url + "/cuizine";
  settings_url = api_url + "/settings";

  $(document).ajaxSend(function () {
    $("#overlay").fadeIn(300);
  });

  $(".select2").select2();

  $("#rating").ionRangeSlider({
    postfix: " Star",
    min: 1,
    max: 5,
    from: 1,
    onChange: function (obj) {
      $("#txtrating").val(obj.from);
    },
    onStart: function (obj) {
      $("#txtrating").val(obj.from);
    },
  });

  $("#result").hide();
  $(".home").hide();

  fetchData();

  function fetchData() {
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

          setTimeout(function () {
            $("#overlay").fadeOut(300);
          }, 500);
        },
        error: function (error) {
          console.error("Error fetching data:", error);
          Swal.fire({
            text: "Error fetching data",
            icon: "error",
          });

          setTimeout(function () {
            $("#overlay").fadeOut(300);
          }, 500);
        },
      });
    } catch (e) {
      console.log(e);
      Swal.fire({
        text: "Error loading the system",
        icon: "error",
      });

      setTimeout(function () {
        $("#overlay").fadeOut(300);
      }, 500);
    }
  }

  submit.addEventListener("click", function () {
    //get value of dropdowm control
    var rating = document.getElementById("txtrating");
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
    $("#result").html("");

    $.ajax({
      url: predict_api,
      type: "POST",
      contentType: "application/json", // Adjust as needed
      data: JSON.stringify(data),
      success: function (response) {
        // Process the response data
        console.log(response.data);
        var data = response.data;
        var html = "";
        for (var i = 0; i < data.length; i++) {
          topiclist = "";
          topiclist = "";
          for (var j = 0; j < data[i].topic_list.length; j++) {
            per = data[i].topic_percentage_list[j];
            count = data[i].topic_count_list[j];
            topic = data[i].topic_list[j];
            topiclist +=
              "<li>" +
              topic +
              " : " +
              count +
              " (" +
              per.toFixed(2) +
              "%)</li>" +
              "</ul>";
          }

          html +=
            '<div class="listing-item"><div class="details"><div class="name-rating">' +
            '<span class="name">' +
            data[i].bus_name +
            "</span>" +
            '<span class="rating">' +
            data[i].rating +
            '<span class="stars">★★★★★</span> (' +
            data[i].total_reviews +
            " Reviews)</span>" +
            ' </div><div class="info">Restaurant</div>' +
            '<div class="amenities">Business Address: ' +
            data[i].address +
            "</div> </div>" +
            '<div class="location">' +
            topiclist +
            "</div>" +
            "</div>";
        }
        $("#result").html(html);

        setTimeout(function () {
          $("#overlay").fadeOut(300);
        }, 500);
      },
      error: function (error) {
        console.error("Error fetching data:", error);
        Swal.fire({
          text: "Error fetching data",
          icon: "error",
        });

        setTimeout(function () {
          $("#overlay").fadeOut(300);
        }, 500);
      },
    });
  });

  reset.addEventListener("click", function () {
    //get value of dropdowm control
    $("#result").hide();
    $(".home").hide();
    // $(".home").show();
  });

  // Function to save data to local storage
  function saveDataToLocalStorage(data) {
    localStorage.setItem("appData", JSON.stringify(data));
  }

  // Function to retrieve data from local storage
  function getDataFromLocalStorage() {
    const storedData = localStorage.getItem("appData");
    return storedData ? JSON.parse(storedData) : null;
  }
});
