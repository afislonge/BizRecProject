$(function () {
  api_url = "localhost/api/";

  $("#result").hide();

  submit.addEventListener("click", function () {
    //get value of dropdowm control
    var rating = document.getElementById("rating");
    var cuzine = document.getElementById("cuzine");
    var city = document.getElementById("location");

    var rest_rating = rating.value;
    var rest_cuzine = cuzine.value;
    var location = city.value;

    var rating = rating.value;
    var cuzine = cuzine.value;
    var location = city.value;

    var formErr = false;

    //check if value is empty
    if (location == "" || cuzine == "" || rating == "") {
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
      rating: rating.value,
      cuzine: cuzine.value,
      location: location.value,
    };
    console.log(data);

    fetch(api_url, {
      method: "POST",
      body: JSON.stringify(data),
      headers: {
        "Content-Type": "application/json",
      },
    })
      .then((response) => response.json())
      .then((data) => {
        console.log("Success:", data);
      })
      .catch((error) => {
        console.error("Error:", error);
      });
  });

  reset.addEventListener("click", function () {
    //get value of dropdowm control
    $("#result").hide();
    $(".home").show();
  });
});
